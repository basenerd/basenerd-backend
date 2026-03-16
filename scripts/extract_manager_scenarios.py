#!/usr/bin/env python3
"""
Extract manager decision scenarios from completed MLB games.

Walks through play-by-play data to find actual decisions (pitching changes,
pinch hits, stolen bases, bunts, IBBs) and missed opportunities where the
manager engine would have recommended a move.

Usage:
    python scripts/extract_manager_scenarios.py --date yesterday
    python scripts/extract_manager_scenarios.py --date 2025-06-15
    python scripts/extract_manager_scenarios.py --start 2025-06-01 --end 2025-06-15
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Load .env if present
env_path = ROOT / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

import urllib.request
import psycopg
from services.mlb_api import get_game_feed


def _fetch_schedule(date_str):
    """Fetch completed games for a date directly from MLB API (avoids Windows strftime issues)."""
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date_str}&hydrate=team,linescore"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read())
    games = []
    for d in data.get("dates") or []:
        for g in d.get("games") or []:
            status = (g.get("status") or {}).get("abstractGameState", "")
            if status.lower() == "final":
                away = (g.get("teams", {}).get("away", {}).get("team") or {})
                home = (g.get("teams", {}).get("home", {}).get("team") or {})
                games.append({
                    "gamePk": g.get("gamePk"),
                    "away_abbrev": away.get("abbreviation", "?"),
                    "home_abbrev": home.get("abbreviation", "?"),
                })
    return games

# RE24 tables (same as manager_engine.py)
RE24 = {
    0: {0: 0.461, 1: 0.831, 2: 1.068, 3: 1.373, 4: 1.270, 5: 1.632, 6: 1.825, 7: 2.151},
    1: {0: 0.245, 1: 0.489, 2: 0.644, 3: 0.867, 4: 0.899, 5: 1.088, 6: 1.275, 7: 1.474},
    2: {0: 0.095, 1: 0.214, 2: 0.305, 3: 0.429, 4: 0.353, 5: 0.468, 6: 0.538, 7: 0.693},
}
P_SCORE_1 = {
    0: {0: 0.255, 1: 0.415, 2: 0.608, 3: 0.637, 4: 0.831, 5: 0.855, 6: 0.874, 7: 0.869},
    1: {0: 0.153, 1: 0.265, 2: 0.390, 3: 0.413, 4: 0.650, 5: 0.660, 6: 0.676, 7: 0.699},
    2: {0: 0.065, 1: 0.130, 2: 0.222, 3: 0.226, 4: 0.263, 5: 0.278, 6: 0.270, 7: 0.326},
}
SB_BREAKEVEN = {(1, 0): 0.769, (1, 1): 0.699, (1, 2): 0.679, (2, 0): 0.947, (2, 1): 0.746, (2, 2): 0.798}

# Win probability estimation using log5 / run-differential model
# Based on Tango's model: WP = 1 / (1 + 10^(-k * run_diff / sqrt(innings_remaining)))
import math

def _win_prob(score_diff, inning, half, outs):
    """Estimate win probability for the batting team given current game state.
    score_diff: batting team runs - pitching team runs (positive = batting team leads)
    Returns probability that the batting team wins (0-1).
    """
    # Innings remaining (each half = 0.5 innings)
    half_innings_left = max(1, (9 - inning) * 2 + (0 if half == "bottom" else 1) - (outs / 3))
    innings_left = half_innings_left / 2.0
    # Empirical constant calibrated to MLB data
    k = 0.34
    wp = 1.0 / (1.0 + 10.0 ** (-k * score_diff / max(0.5, math.sqrt(innings_left))))
    return round(min(0.95, max(0.05, wp)), 3)


def _win_prob_delta(ctx, re_delta):
    """Given a run expectancy change, estimate win prob shift for the batting team's manager.
    re_delta: expected runs gained by making the move (positive = good for batting team).
    """
    base_wp = _win_prob(ctx["score_diff"], ctx["inning"], ctx["half"], ctx["outs"])
    new_wp = _win_prob(ctx["score_diff"] + re_delta, ctx["inning"], ctx["half"], ctx["outs"])
    return base_wp, new_wp


def _db_url():
    url = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_URL_PG") or ""
    if not url:
        raise RuntimeError("Missing DATABASE_URL env var")
    return url


def _compute_leverage(inning, outs, score_diff, base_state):
    risp = base_state & 0b110
    if inning >= 7 and abs(score_diff) <= 1:
        return "high"
    if inning >= 7 and abs(score_diff) <= 2 and risp and outs < 2:
        return "high"
    if inning >= 6 and abs(score_diff) <= 3:
        return "medium"
    return "low"


def _base_state_str(bs):
    parts = []
    if bs & 1: parts.append("1st")
    if bs & 2: parts.append("2nd")
    if bs & 4: parts.append("3rd")
    return ", ".join(parts) if parts else "empty"


def _outs_str(n):
    return f"{n} out{'s' if n != 1 else ''}"


def _half_display(half):
    return "Top" if half == "top" else "Bottom"


def _engine_rec_pitching_change(ctx):
    """Heuristic engine recommendation for pitching change."""
    reasons = []
    confidence = 0.3
    if ctx["pitcher_pitch_count"] >= 100:
        reasons.append(f"{ctx['pitcher_pitch_count']} pitches")
        confidence += 0.25
    elif ctx["pitcher_pitch_count"] >= 90:
        reasons.append(f"{ctx['pitcher_pitch_count']} pitches")
        confidence += 0.15
    if ctx["pitcher_tto"] >= 3:
        reasons.append(_tto_label(ctx["pitcher_tto"]))
        confidence += 0.2
    leverage = _compute_leverage(ctx["inning"], ctx["outs"], ctx["score_diff"], ctx["base_state"])
    if leverage == "high":
        confidence += 0.15
        reasons.append("high leverage")
    if not reasons:
        reasons.append("late inning situation")
    recommend = "yes" if confidence >= 0.5 else "no"
    # Win prob: fresh arm expected to reduce opponent RE by ~0.15 runs
    re_delta = 0.15 if ctx.get("pitching_side") == ctx.get("batting_side") else -0.15
    # This is from the pitching team's perspective — making a change helps the pitching team
    base_wp = _win_prob(-ctx["score_diff"], ctx["inning"], ctx["half"], ctx["outs"])
    wp_yes = _win_prob(-ctx["score_diff"] + 0.15, ctx["inning"], ctx["half"], ctx["outs"])
    wp_no = base_wp
    return {"recommend": recommend, "confidence": round(min(0.95, confidence), 2), "detail": "; ".join(reasons),
            "win_prob_yes": round(wp_yes, 3), "win_prob_no": round(wp_no, 3)}


def _engine_rec_pinch_hit(ctx):
    """Heuristic engine recommendation for pinch hit."""
    confidence = 0.3
    reasons = []
    if ctx.get("batter_spot") and ctx["batter_spot"] >= 8:
        confidence += 0.15
        reasons.append(f"batting {ctx['batter_spot']}th")
    leverage = _compute_leverage(ctx["inning"], ctx["outs"], ctx["score_diff"], ctx["base_state"])
    if leverage == "high":
        confidence += 0.2
        reasons.append("high leverage")
    if ctx["base_state"] & 0b110:
        confidence += 0.1
        reasons.append("RISP")
    if not reasons:
        reasons.append("late inning opportunity")
    recommend = "yes" if confidence >= 0.5 else "no"
    # Pinch hitter upgrade ~ +0.1 expected runs
    base_wp = _win_prob(ctx["score_diff"], ctx["inning"], ctx["half"], ctx["outs"])
    wp_yes = _win_prob(ctx["score_diff"] + 0.1, ctx["inning"], ctx["half"], ctx["outs"])
    wp_no = base_wp
    return {"recommend": recommend, "confidence": round(min(0.9, confidence), 2), "detail": "; ".join(reasons),
            "win_prob_yes": round(wp_yes, 3), "win_prob_no": round(wp_no, 3)}


def _engine_rec_stolen_base(ctx):
    """Heuristic engine recommendation for stolen base."""
    # SB success gains ~0.4 runs, caught stealing loses ~0.6 runs. Net EV at 70% success = +0.1
    bs = ctx["base_state"]
    outs = ctx["outs"]
    # Compute RE delta for success vs failure
    re_now = RE24.get(outs, {}).get(bs, 0.3)
    # If runner on 1st -> 2nd
    if bs & 1:
        sb_bs = (bs & ~1) | 2
        cs_bs = bs & ~1
    elif bs & 2:
        sb_bs = (bs & ~2) | 4
        cs_bs = bs & ~2
    else:
        sb_bs = bs
        cs_bs = bs
    re_success = RE24.get(outs, {}).get(sb_bs, 0.3)
    cs_outs = min(2, outs + 1)
    re_fail = RE24.get(cs_outs, {}).get(cs_bs, 0.1)
    # Assume 70% success rate
    ev = 0.7 * (re_success - re_now) + 0.3 * (re_fail - re_now)
    base_wp = _win_prob(ctx["score_diff"], ctx["inning"], ctx["half"], ctx["outs"])
    wp_yes = _win_prob(ctx["score_diff"] + ev, ctx["inning"], ctx["half"], ctx["outs"])
    wp_no = base_wp
    return {"recommend": "yes", "confidence": 0.55, "detail": "Runner has base-stealing ability in a close game",
            "win_prob_yes": round(wp_yes, 3), "win_prob_no": round(wp_no, 3)}


def _engine_rec_bunt(ctx):
    """Heuristic engine recommendation for bunt."""
    bs = ctx["base_state"]
    outs = ctx["outs"]
    base_wp = _win_prob(ctx["score_diff"], ctx["inning"], ctx["half"], ctx["outs"])
    if outs != 0:
        return {"recommend": "no", "confidence": 0.7, "detail": "Sacrifice only makes sense with 0 outs",
                "win_prob_yes": round(base_wp - 0.02, 3), "win_prob_no": round(base_wp, 3)}
    # Compare P(score 1+) before and after bunt
    current_p1 = P_SCORE_1.get(0, {}).get(bs, 0.25)
    bunt_bs = 0
    if bs & 1: bunt_bs |= 2
    if bs & 2: bunt_bs |= 4
    if bs & 4: bunt_bs |= 4
    bunt_p1 = P_SCORE_1.get(1, {}).get(bunt_bs, 0.25)
    # RE delta from bunting
    re_now = RE24.get(0, {}).get(bs, 0.46)
    re_bunt = RE24.get(1, {}).get(bunt_bs, 0.25)
    re_delta = re_bunt - re_now
    wp_yes = _win_prob(ctx["score_diff"] + re_delta, ctx["inning"], ctx["half"], ctx["outs"])
    wp_no = base_wp
    if bunt_p1 > current_p1:
        return {"recommend": "yes", "confidence": round(0.5 + (bunt_p1 - current_p1) * 2, 2),
                "detail": f"Bunting improves P(score 1+) from {current_p1:.0%} to {bunt_p1:.0%}",
                "win_prob_yes": round(wp_yes, 3), "win_prob_no": round(wp_no, 3)}
    return {"recommend": "no", "confidence": 0.6,
            "detail": f"Bunting lowers P(score 1+) from {current_p1:.0%} to {bunt_p1:.0%}",
            "win_prob_yes": round(wp_yes, 3), "win_prob_no": round(wp_no, 3)}


def _engine_rec_ibb(ctx):
    """Heuristic engine recommendation for IBB."""
    leverage = _compute_leverage(ctx["inning"], ctx["outs"], ctx["score_diff"], ctx["base_state"])
    bs = ctx["base_state"]
    outs = ctx["outs"]
    # IBB adds a runner: e.g. bases 010 -> 011, etc.
    ibb_bs = bs | 1  # put runner on 1st
    re_now = RE24.get(outs, {}).get(bs, 0.3)
    re_ibb = RE24.get(outs, {}).get(ibb_bs, 0.5)
    # IBB is from the pitching team's perspective (defensive move), so negative RE delta is good
    re_delta = -(re_ibb - re_now)  # negative because more RE on base = bad for pitching team
    base_wp = _win_prob(-ctx["score_diff"], ctx["inning"], ctx["half"], ctx["outs"])
    wp_yes = _win_prob(-ctx["score_diff"] + re_delta, ctx["inning"], ctx["half"], ctx["outs"])
    wp_no = base_wp
    if leverage == "high" and ctx["outs"] < 2:
        return {"recommend": "yes", "confidence": 0.6, "detail": "High leverage, sets up force/DP",
                "win_prob_yes": round(wp_yes, 3), "win_prob_no": round(wp_no, 3)}
    return {"recommend": "no", "confidence": 0.55, "detail": "Putting another runner on base is risky",
            "win_prob_yes": round(wp_yes, 3), "win_prob_no": round(wp_no, 3)}


ENGINE_RECS = {
    "pitching_change": _engine_rec_pitching_change,
    "pinch_hit": _engine_rec_pinch_hit,
    "stolen_base": _engine_rec_stolen_base,
    "bunt": _engine_rec_bunt,
    "ibb": _engine_rec_ibb,
}

def _tto_label(tto):
    """Human-readable TTO label."""
    if tto == 1:
        return "first time through the order"
    elif tto == 2:
        return "second time through the order"
    elif tto == 3:
        return "third time through the order"
    else:
        return "{}th time through the order".format(tto)


DECISION_QUESTIONS = {
    "pitching_change": {
        "question": "Do you make a pitching change?",
        "yes_label": "Pull the pitcher",
        "no_label": "Leave him in",
    },
    "pinch_hit": {
        "question": "Do you pinch-hit here?",
        "yes_label": "Send in a pinch hitter",
        "no_label": "Let him hit",
    },
    "stolen_base": {
        "question": "Do you send the runner?",
        "yes_label": "Send the runner",
        "no_label": "Hold the runner",
    },
    "bunt": {
        "question": "Do you have him bunt?",
        "yes_label": "Sacrifice bunt",
        "no_label": "Swing away",
    },
    "ibb": {
        "question": "Do you intentionally walk this batter?",
        "yes_label": "Walk him",
        "no_label": "Pitch to him",
    },
}


def _get_player_hand(game_players, player_id, role="bat"):
    """Get player handedness from gameData.players. role='bat' or 'pitch'."""
    key = f"ID{player_id}"
    gp = game_players.get(key) or {}
    if role == "bat":
        return ((gp.get("batSide") or {}).get("code") or "?").upper()
    return ((gp.get("pitchHand") or {}).get("code") or "?").upper()


def _get_boxscore_player(boxscore, player_id):
    """Find a player's boxscore entry across both teams."""
    for side in ("away", "home"):
        team_box = (boxscore.get("teams") or {}).get(side) or {}
        players = team_box.get("players") or {}
        key = f"ID{player_id}"
        if key in players:
            return players[key], side
    return None, None


def _extract_batter_stats(boxscore, player_id):
    """Extract game + season batting stats for a player from boxscore."""
    entry, _ = _get_boxscore_player(boxscore, player_id)
    if not entry:
        return {}
    stats = (entry.get("stats") or {}).get("batting") or {}
    season = (entry.get("seasonStats") or {}).get("batting") or {}
    return {
        "game": {
            "ab": stats.get("atBats", 0),
            "h": stats.get("hits", 0),
            "hr": stats.get("homeRuns", 0),
            "rbi": stats.get("rbi", 0),
            "bb": stats.get("baseOnBalls", 0),
            "so": stats.get("strikeOuts", 0),
        },
        "season": {
            "avg": season.get("avg", ".000"),
            "obp": season.get("obp", ".000"),
            "slg": season.get("slg", ".000"),
            "ops": season.get("ops", ".000"),
            "ab": season.get("atBats", 0),
            "hr": season.get("homeRuns", 0),
        },
    }


def _extract_pitcher_stats(boxscore, player_id):
    """Extract game + season pitching stats for a player from boxscore."""
    entry, _ = _get_boxscore_player(boxscore, player_id)
    if not entry:
        return {}
    stats = (entry.get("stats") or {}).get("pitching") or {}
    season = (entry.get("seasonStats") or {}).get("pitching") or {}
    return {
        "game": {
            "ip": stats.get("inningsPitched", "0.0"),
            "h": stats.get("hits", 0),
            "r": stats.get("runs", 0),
            "er": stats.get("earnedRuns", 0),
            "bb": stats.get("baseOnBalls", 0),
            "so": stats.get("strikeOuts", 0),
            "pitches": stats.get("pitchesThrown") or stats.get("numberOfPitches", 0),
        },
        "season": {
            "era": season.get("era", "0.00"),
            "whip": season.get("whip", "0.00"),
            "so": season.get("strikeOuts", 0),
            "ip": season.get("inningsPitched", "0.0"),
        },
    }


def _fetch_splits(player_id, season, role="batting"):
    """Fetch vs-L/vs-R splits from MLB stats API. Returns {vs_L: {...}, vs_R: {...}}."""
    try:
        group = "hitting" if role == "batting" else "pitching"
        url = (
            f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
            f"?stats=statSplits&season={season}&group={group}"
            f"&sitCodes=vl,vr"
        )
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read())
        splits = {}
        for stat_group in data.get("stats") or []:
            for split in stat_group.get("splits") or []:
                sit = (split.get("split") or {}).get("code") or ""
                s = split.get("stat") or {}
                if role == "batting":
                    splits[sit] = {
                        "avg": s.get("avg", ".000"),
                        "obp": s.get("obp", ".000"),
                        "ops": s.get("ops", ".000"),
                        "ab": s.get("atBats", 0),
                    }
                else:
                    splits[sit] = {
                        "era": s.get("era", "0.00"),
                        "avg": s.get("avg", ".000"),
                        "ops": s.get("ops", ".000"),
                        "ab": s.get("atBats", 0),
                    }
        return {"vs_L": splits.get("vl", {}), "vs_R": splits.get("vr", {})}
    except Exception:
        return {}


def extract_scenarios_from_game(feed, game_pk):
    """Extract decision-point scenarios from a completed game feed."""
    if not feed:
        return []

    game_data = feed.get("gameData") or {}
    live_data = feed.get("liveData") or {}
    all_plays = (live_data.get("plays") or {}).get("allPlays") or []
    boxscore = live_data.get("boxscore") or {}

    teams = game_data.get("teams") or {}
    away_abbr = ((teams.get("away") or {}).get("abbreviation") or "").upper()
    home_abbr = ((teams.get("home") or {}).get("abbreviation") or "").upper()
    game_date_str = (game_data.get("datetime") or {}).get("officialDate") or ""
    season = int((game_data.get("game") or {}).get("season", 2025) or 2025)
    game_players = game_data.get("players") or {}

    scenarios = []

    # Cache for splits (to avoid re-fetching the same player)
    _splits_cache = {}

    def _cached_splits(pid, role="batting"):
        key = (pid, role)
        if key not in _splits_cache:
            _splits_cache[key] = _fetch_splits(pid, season, role)
        return _splits_cache[key]

    # Track running game state
    pitcher_pitch_counts = {}  # pitcher_id -> count
    pitcher_batters_faced = {}  # pitcher_id -> set of batter_ids
    pitcher_names_map = {}  # pitcher_id -> name
    current_pitcher = {"top": None, "bottom": None}

    # Get starting pitchers from boxscore
    for side_key in ("away", "home"):
        team_box = (boxscore.get("teams") or {}).get(side_key) or {}
        pitchers = team_box.get("pitchers") or []
        if pitchers:
            pid = int(pitchers[0])
            half = "top" if side_key == "home" else "bottom"
            current_pitcher[half] = pid
            pitcher_pitch_counts[pid] = 0
            pitcher_batters_faced[pid] = set()
            # Get starter name
            key = f"ID{pid}"
            gp = game_players.get(key) or {}
            pitcher_names_map[pid] = gp.get("fullName") or gp.get("lastName") or str(pid)

    # Get batting orders
    batting_orders = {}
    for side_key in ("away", "home"):
        team_box = (boxscore.get("teams") or {}).get(side_key) or {}
        batting_orders[side_key] = [int(x) for x in (team_box.get("battingOrder") or [])]

    for play_idx, play in enumerate(all_plays):
        about = play.get("about") or {}
        result = play.get("result") or {}
        matchup = play.get("matchup") or {}
        play_events = play.get("playEvents") or []

        inning = about.get("inning") or 1
        half_raw = (about.get("halfInning") or "top").lower()
        half = "top" if "top" in half_raw else "bottom"
        is_complete = about.get("isComplete", False)

        batter_id = (matchup.get("batter") or {}).get("id")
        pitcher_id = (matchup.get("pitcher") or {}).get("id")
        batter_name = (matchup.get("batter") or {}).get("fullName") or ""
        pitcher_name = (matchup.get("pitcher") or {}).get("fullName") or ""
        stand = ((matchup.get("batSide") or {}).get("code") or "R").upper()
        p_throws = ((matchup.get("pitchHand") or {}).get("code") or "R").upper()

        # Determine scores BEFORE this play
        away_score = about.get("awayScore") or 0
        home_score = about.get("homeScore") or 0
        # The about scores might be post-play; for pre-play we use the previous play's result
        if play_idx > 0:
            prev_result = (all_plays[play_idx - 1].get("result") or {})
            away_score = prev_result.get("awayScore") or away_score
            home_score = prev_result.get("homeScore") or home_score

        if half == "top":
            score_diff = away_score - home_score
            batting_side = "away"
            pitching_side = "home"
        else:
            score_diff = home_score - away_score
            batting_side = "home"
            pitching_side = "away"

        # Track pitch counts — use the pitcher who was ALREADY in the game
        # (current_pitcher[half]) not the matchup pitcher which may be the new reliever
        outgoing_pitcher_id = current_pitcher.get(half)
        outgoing_pitcher_name = pitcher_names_map.get(outgoing_pitcher_id, "") if outgoing_pitcher_id else ""
        outgoing_pitch_count = pitcher_pitch_counts.get(outgoing_pitcher_id, 0) if outgoing_pitcher_id else 0
        outgoing_tto = max(1, len(pitcher_batters_faced.get(outgoing_pitcher_id, set())) // 9 + 1) if outgoing_pitcher_id else 1

        if pitcher_id:
            if pitcher_id not in pitcher_pitch_counts:
                pitcher_pitch_counts[pitcher_id] = 0
                pitcher_batters_faced[pitcher_id] = set()
            pitcher_names_map[pitcher_id] = pitcher_name
            current_pitcher[half] = pitcher_id

        # Count pitches in this at-bat
        ab_pitches = sum(1 for ev in play_events if ev.get("isPitch"))
        if pitcher_id:
            pitcher_pitch_counts[pitcher_id] = pitcher_pitch_counts.get(pitcher_id, 0) + ab_pitches
            if batter_id:
                pitcher_batters_faced.setdefault(pitcher_id, set()).add(batter_id)

        # Current pitcher state (pre-AB)
        pitch_count = pitcher_pitch_counts.get(pitcher_id, 0) - ab_pitches
        tto = max(1, len(pitcher_batters_faced.get(pitcher_id, set())) // 9 + 1)

        # Runners (from play runners data or count)
        runners = play.get("runners") or []
        count = play.get("count") or {}

        # Pre-play outs: use previous play's post-play outs, reset on half-inning change
        if play_idx == 0:
            outs = 0
        else:
            prev_about = (all_plays[play_idx - 1].get("about") or {})
            prev_half = "top" if "top" in (prev_about.get("halfInning") or "top").lower() else "bottom"
            prev_inning = prev_about.get("inning") or 1
            if prev_half != half or prev_inning != inning:
                outs = 0
            else:
                prev_count = all_plays[play_idx - 1].get("count") or {}
                outs = prev_count.get("outs") or 0

        # Safety: skip if outs >= 3 (shouldn't happen with pre-play tracking)
        if outs >= 3:
            outs = 0

        # Get base runners from the start of this play
        base_state = 0
        runner_names = {}
        for r in runners:
            start = (r.get("movement") or {}).get("start")
            if start == "1B":
                base_state |= 1
                runner_names["1B"] = ((r.get("details") or {}).get("runner") or {}).get("fullName", "")
            elif start == "2B":
                base_state |= 2
                runner_names["2B"] = ((r.get("details") or {}).get("runner") or {}).get("fullName", "")
            elif start == "3B":
                base_state |= 4
                runner_names["3B"] = ((r.get("details") or {}).get("runner") or {}).get("fullName", "")

        # Determine batter lineup spot
        order = batting_orders.get(batting_side, [])
        batter_spot = None
        for i, bid in enumerate(order):
            if bid == batter_id:
                batter_spot = i + 1
                break

        play_type = (result.get("type") or "").strip()
        event_type = (result.get("eventType") or "").strip()
        event = (result.get("event") or "").strip()
        description = (result.get("description") or "").strip()

        # Build context for storage
        ctx = {
            "inning": inning,
            "half": half,
            "outs": outs,
            "score_diff": score_diff,
            "away_score": away_score,
            "home_score": home_score,
            "batting_side": batting_side,
            "pitching_side": pitching_side,
            "batter_id": batter_id,
            "pitcher_id": pitcher_id,
            "batter_name": batter_name,
            "pitcher_name": pitcher_name,
            "stand": stand,
            "p_throws": p_throws,
            "batter_hand": _get_player_hand(game_players, batter_id, "bat") if batter_id else stand,
            "pitcher_throws": _get_player_hand(game_players, pitcher_id, "pitch") if pitcher_id else p_throws,
            "base_state": base_state,
            "batter_spot": batter_spot,
            "pitcher_pitch_count": pitch_count,
            "pitcher_tto": tto,
            "runner_names": runner_names,
            "venue": home_abbr,
            "season": season,
            "batter_stats": _extract_batter_stats(boxscore, batter_id) if batter_id else {},
            "pitcher_stats": _extract_pitcher_stats(boxscore, pitcher_id) if pitcher_id else {},
        }

        score_desc = "tie game" if score_diff == 0 else (
            "{}-run lead".format(abs(score_diff)) if score_diff > 0
            else "{}-run deficit".format(abs(score_diff))
        )
        base_desc = "bases empty" if base_state == 0 else "runner(s) on {}".format(_base_state_str(base_state))
        situation_desc = "{} {}, {}. {}, {}".format(
            _half_display(half), inning, score_desc, _outs_str(outs), base_desc
        )

        # ==========================================
        # DETECT POSITIVE EVENTS (manager DID it)
        # ==========================================

        # 1) Pitching substitution (check playEvents for action events)
        for ev in play_events:
            if not ev.get("isPitch") and ev.get("type") == "action":
                ev_desc = (ev.get("details") or {}).get("description") or ""
                ev_event = (ev.get("details") or {}).get("eventType") or (ev.get("details") or {}).get("event") or ""
                if "pitching_substitution" in ev_event.lower() or "pitching change" in ev_desc.lower():
                    # Only in close, interesting situations
                    if inning >= 5 and abs(score_diff) <= 3:
                        # Use outgoing pitcher data (not the new reliever)
                        pc_pitcher_id = outgoing_pitcher_id or pitcher_id
                        pc_pitcher_name = outgoing_pitcher_name or pitcher_name
                        pc_pitch_count = outgoing_pitch_count
                        pc_tto = outgoing_tto

                        # Extract incoming reliever name from description
                        # Format: "Pitching Change: Camilo Doval replaces Logan Webb."
                        reliever_name = ""
                        if ":" in ev_desc:
                            after_colon = ev_desc.split(":", 1)[1].strip()
                            if " replaces " in after_colon:
                                reliever_name = after_colon.split(" replaces ")[0].strip()
                            elif after_colon:
                                reliever_name = after_colon.rstrip(".")

                        pc_ctx = dict(ctx)
                        pc_ctx["pitcher_id"] = pc_pitcher_id
                        pc_ctx["pitcher_name"] = pc_pitcher_name
                        pc_ctx["pitcher_pitch_count"] = pc_pitch_count
                        pc_ctx["pitcher_tto"] = pc_tto
                        pc_ctx["p_throws"] = _get_player_hand(game_players, pc_pitcher_id, "pitch")

                        # Outgoing pitcher game + season stats
                        pc_ctx["pitcher_stats"] = _extract_pitcher_stats(boxscore, pc_pitcher_id)
                        pc_ctx["pitcher_splits"] = _cached_splits(pc_pitcher_id, "pitching")

                        # Reliever stats (if we can find their ID)
                        reliever_id = None
                        if reliever_name:
                            # Try to find reliever ID from matchup pitcher or game_players
                            for gp_key, gp_val in game_players.items():
                                if (gp_val.get("fullName") or "") == reliever_name:
                                    reliever_id = gp_val.get("id")
                                    break
                        if reliever_id:
                            pc_ctx["reliever_id"] = reliever_id
                            pc_ctx["reliever_name"] = reliever_name
                            pc_ctx["reliever_throws"] = _get_player_hand(game_players, reliever_id, "pitch")
                            pc_ctx["reliever_stats"] = _extract_pitcher_stats(boxscore, reliever_id)
                            pc_ctx["reliever_splits"] = _cached_splits(reliever_id, "pitching")

                        # Batter stats for context
                        pc_ctx["batter_hand"] = stand
                        pc_ctx["batter_stats"] = _extract_batter_stats(boxscore, batter_id)

                        rec = _engine_rec_pitching_change(pc_ctx)
                        tto_label = _tto_label(pc_tto)
                        yes_label = "Bring in {}".format(reliever_name) if reliever_name else "Pull the pitcher"
                        options = {
                            "question": "Do you make a pitching change?",
                            "yes_label": yes_label,
                            "no_label": "Leave {} in".format(pc_pitcher_name.split()[-1] if pc_pitcher_name else "him"),
                            "situation": "{situation}. {name} has thrown {pc} pitches, {tto}.".format(
                                situation=situation_desc, name=pc_pitcher_name, pc=pc_pitch_count, tto=tto_label),
                        }
                        scenarios.append({
                            "game_pk": game_pk, "game_date": game_date_str,
                            "away_team_abbr": away_abbr, "home_team_abbr": home_abbr,
                            "inning": inning, "half": half, "outs": outs,
                            "away_score": away_score, "home_score": home_score,
                            "base_state": base_state,
                            "batter_id": batter_id, "batter_name": batter_name,
                            "pitcher_id": pc_pitcher_id, "pitcher_name": pc_pitcher_name,
                            "pitcher_pitch_count": pc_pitch_count, "pitcher_tto": pc_tto,
                            "decision_type": "pitching_change",
                            "actual_decision": "yes",
                            "actual_detail": ev_desc,
                            "engine_recommendation": json.dumps(rec),
                            "context_json": json.dumps(pc_ctx),
                            "options_json": json.dumps(options),
                            "play_index": play_idx,
                        })

        # 2) Pinch hit (offensive substitution)
        for ev in play_events:
            if not ev.get("isPitch") and ev.get("type") == "action":
                ev_event = (ev.get("details") or {}).get("eventType") or ""
                ev_desc = (ev.get("details") or {}).get("description") or ""
                if "offensive_substitution" in ev_event.lower() and "pinch" in ev_desc.lower():
                    if inning >= 5 and abs(score_diff) <= 3:
                        # Parse pinch hitter and replaced batter from description
                        # Format: "Offensive Sub: Pinch-hitter Matt Chapman replaces LaMonte Wade Jr."
                        ph_name = ""
                        replaced_name = batter_name
                        if "replaces" in ev_desc.lower():
                            parts = ev_desc.split("replaces", 1)
                            after = parts[-1].strip().rstrip(".")
                            replaced_name = after or batter_name
                            # Pinch hitter name comes after "Pinch-hitter " or "Pinch hitter "
                            before = parts[0]
                            for marker in ("Pinch-hitter ", "Pinch hitter ", "pinch-hitter ", "pinch hitter "):
                                if marker in before:
                                    ph_name = before.split(marker, 1)[1].strip()
                                    break

                        # Find player IDs from game_players by name
                        ph_id = None
                        replaced_id = None
                        if ph_name:
                            for gp_key, gp_val in game_players.items():
                                if (gp_val.get("fullName") or "") == ph_name:
                                    ph_id = gp_val.get("id")
                                    break
                        if replaced_name:
                            for gp_key, gp_val in game_players.items():
                                if (gp_val.get("fullName") or "") == replaced_name:
                                    replaced_id = gp_val.get("id")
                                    break
                        # Fallback: if matchup batter matches PH, the replaced is someone else
                        if not replaced_id and ph_id and batter_id == ph_id:
                            replaced_id = None  # Can't determine
                        elif not replaced_id:
                            replaced_id = batter_id

                        ph_ctx = dict(ctx)
                        ph_ctx["p_throws"] = _get_player_hand(game_players, pitcher_id, "pitch") if pitcher_id else p_throws

                        # Original batter stats + splits
                        ph_ctx["original_batter_name"] = replaced_name
                        ph_ctx["original_batter_id"] = replaced_id
                        ph_ctx["original_batter_hand"] = _get_player_hand(game_players, replaced_id, "bat") if replaced_id else "?"
                        ph_ctx["original_batter_stats"] = _extract_batter_stats(boxscore, replaced_id) if replaced_id else {}
                        ph_ctx["original_batter_splits"] = _cached_splits(replaced_id, "batting") if replaced_id else {}

                        # Pinch hitter stats + splits
                        if ph_id:
                            ph_ctx["pinch_hitter_name"] = ph_name
                            ph_ctx["pinch_hitter_id"] = ph_id
                            ph_ctx["pinch_hitter_hand"] = _get_player_hand(game_players, ph_id, "bat")
                            ph_ctx["pinch_hitter_stats"] = _extract_batter_stats(boxscore, ph_id)
                            ph_ctx["pinch_hitter_splits"] = _cached_splits(ph_id, "batting")
                            ph_ctx["batter_hand"] = ph_ctx["pinch_hitter_hand"]

                        # Pitcher stats + splits for context
                        ph_ctx["pitcher_stats"] = _extract_pitcher_stats(boxscore, pitcher_id)
                        ph_ctx["pitcher_splits"] = _cached_splits(pitcher_id, "pitching") if pitcher_id else {}

                        rec = _engine_rec_pinch_hit(ph_ctx)
                        yes_label = "Send in {}".format(ph_name) if ph_name else "Send in a pinch hitter"
                        # Use last name but skip suffixes like "Jr.", "II", "III"
                        _suffixes = {"jr.", "jr", "ii", "iii", "iv", "sr.", "sr"}
                        _rparts = replaced_name.split() if replaced_name else []
                        _last = _rparts[-1] if _rparts else "him"
                        if _last.lower().rstrip(".") in _suffixes and len(_rparts) >= 2:
                            _last = _rparts[-2]
                        no_label = "Let {} hit".format(_last)
                        options = {
                            "question": "Do you pinch-hit here?",
                            "yes_label": yes_label,
                            "no_label": no_label,
                            "situation": f"{situation_desc}. Batting spot {batter_spot or '?'} in the order.",
                        }
                        scenarios.append({
                            "game_pk": game_pk, "game_date": game_date_str,
                            "away_team_abbr": away_abbr, "home_team_abbr": home_abbr,
                            "inning": inning, "half": half, "outs": outs,
                            "away_score": away_score, "home_score": home_score,
                            "base_state": base_state,
                            "batter_id": batter_id, "batter_name": batter_name,
                            "pitcher_id": pitcher_id, "pitcher_name": pitcher_name,
                            "pitcher_pitch_count": pitch_count, "pitcher_tto": tto,
                            "decision_type": "pinch_hit",
                            "actual_decision": "yes",
                            "actual_detail": ev_desc,
                            "engine_recommendation": json.dumps(rec),
                            "context_json": json.dumps(ph_ctx),
                            "options_json": json.dumps(options),
                            "play_index": play_idx,
                        })

        if not is_complete:
            continue

        # 3) Stolen base / caught stealing
        if event_type in ("stolen_base_2b", "stolen_base_3b", "stolen_base_home",
                          "caught_stealing_2b", "caught_stealing_3b", "caught_stealing_home"):
            if abs(score_diff) <= 3:
                was_successful = "stolen_base" in event_type
                rec = _engine_rec_stolen_base(ctx)
                options = {**DECISION_QUESTIONS["stolen_base"],
                           "situation": f"{situation_desc}.",
                           "outcome": "Safe!" if was_successful else "Caught stealing!"}
                scenarios.append({
                    "game_pk": game_pk, "game_date": game_date_str,
                    "away_team_abbr": away_abbr, "home_team_abbr": home_abbr,
                    "inning": inning, "half": half, "outs": outs,
                    "away_score": away_score, "home_score": home_score,
                    "base_state": base_state,
                    "batter_id": batter_id, "batter_name": batter_name,
                    "pitcher_id": pitcher_id, "pitcher_name": pitcher_name,
                    "pitcher_pitch_count": pitch_count, "pitcher_tto": tto,
                    "decision_type": "stolen_base",
                    "actual_decision": "yes",
                    "actual_detail": description,
                    "engine_recommendation": json.dumps(rec),
                    "context_json": json.dumps(ctx),
                    "options_json": json.dumps(options),
                    "play_index": play_idx,
                })

        # 4) Sacrifice bunt
        if event_type in ("sac_bunt", "sac_bunt_double_play"):
            if abs(score_diff) <= 3:
                rec = _engine_rec_bunt(ctx)
                options = {**DECISION_QUESTIONS["bunt"],
                           "situation": f"{situation_desc}. {batter_name} at the plate."}
                scenarios.append({
                    "game_pk": game_pk, "game_date": game_date_str,
                    "away_team_abbr": away_abbr, "home_team_abbr": home_abbr,
                    "inning": inning, "half": half, "outs": outs,
                    "away_score": away_score, "home_score": home_score,
                    "base_state": base_state,
                    "batter_id": batter_id, "batter_name": batter_name,
                    "pitcher_id": pitcher_id, "pitcher_name": pitcher_name,
                    "pitcher_pitch_count": pitch_count, "pitcher_tto": tto,
                    "decision_type": "bunt",
                    "actual_decision": "yes",
                    "actual_detail": description,
                    "engine_recommendation": json.dumps(rec),
                    "context_json": json.dumps(ctx),
                    "options_json": json.dumps(options),
                    "play_index": play_idx,
                })

        # 5) Intentional walk
        if event_type == "intent_walk":
            if abs(score_diff) <= 3:
                rec = _engine_rec_ibb(ctx)
                options = {**DECISION_QUESTIONS["ibb"],
                           "situation": f"{situation_desc}. {batter_name} at the plate."}
                scenarios.append({
                    "game_pk": game_pk, "game_date": game_date_str,
                    "away_team_abbr": away_abbr, "home_team_abbr": home_abbr,
                    "inning": inning, "half": half, "outs": outs,
                    "away_score": away_score, "home_score": home_score,
                    "base_state": base_state,
                    "batter_id": batter_id, "batter_name": batter_name,
                    "pitcher_id": pitcher_id, "pitcher_name": pitcher_name,
                    "pitcher_pitch_count": pitch_count, "pitcher_tto": tto,
                    "decision_type": "ibb",
                    "actual_decision": "yes",
                    "actual_detail": description,
                    "engine_recommendation": json.dumps(rec),
                    "context_json": json.dumps(ctx),
                    "options_json": json.dumps(options),
                    "play_index": play_idx,
                })

        # ==========================================
        # DETECT NON-EVENTS (manager DIDN'T do it)
        # ==========================================

        # Only for completed at-bats in interesting game states
        if play_type != "atBat" or abs(score_diff) > 3:
            continue

        # Non-event: pitcher left in despite fatigue
        if inning >= 6 and (pitch_count >= 85 or tto >= 3) and abs(score_diff) <= 3:
            leverage = _compute_leverage(inning, outs, score_diff, base_state)
            if leverage in ("high", "medium"):
                rec = _engine_rec_pitching_change(ctx)
                if rec["recommend"] == "yes":
                    tto_label = _tto_label(tto)
                    options = {**DECISION_QUESTIONS["pitching_change"],
                               "situation": f"{situation_desc}. {pitcher_name} has {pitch_count} pitches, {tto_label}."}
                    scenarios.append({
                        "game_pk": game_pk, "game_date": game_date_str,
                        "away_team_abbr": away_abbr, "home_team_abbr": home_abbr,
                        "inning": inning, "half": half, "outs": outs,
                        "away_score": away_score, "home_score": home_score,
                        "base_state": base_state,
                        "batter_id": batter_id, "batter_name": batter_name,
                        "pitcher_id": pitcher_id, "pitcher_name": pitcher_name,
                        "pitcher_pitch_count": pitch_count, "pitcher_tto": tto,
                        "decision_type": "pitching_change",
                        "actual_decision": "no",
                        "actual_detail": f"Manager left {pitcher_name} in the game",
                        "engine_recommendation": json.dumps(rec),
                        "context_json": json.dumps(ctx),
                        "options_json": json.dumps(options),
                        "play_index": play_idx,
                    })

        # Non-event: no pinch hit for bottom-of-order batter in late close game
        if inning >= 7 and batter_spot and batter_spot >= 8 and abs(score_diff) <= 2:
            if base_state & 0b110:  # RISP
                rec = _engine_rec_pinch_hit(ctx)
                if rec["recommend"] == "yes":
                    options = {**DECISION_QUESTIONS["pinch_hit"],
                               "situation": f"{situation_desc}. {batter_name} batting {batter_spot}th in the order."}
                    scenarios.append({
                        "game_pk": game_pk, "game_date": game_date_str,
                        "away_team_abbr": away_abbr, "home_team_abbr": home_abbr,
                        "inning": inning, "half": half, "outs": outs,
                        "away_score": away_score, "home_score": home_score,
                        "base_state": base_state,
                        "batter_id": batter_id, "batter_name": batter_name,
                        "pitcher_id": pitcher_id, "pitcher_name": pitcher_name,
                        "pitcher_pitch_count": pitch_count, "pitcher_tto": tto,
                        "decision_type": "pinch_hit",
                        "actual_decision": "no",
                        "actual_detail": f"Manager let {batter_name} hit",
                        "engine_recommendation": json.dumps(rec),
                        "context_json": json.dumps(ctx),
                        "options_json": json.dumps(options),
                        "play_index": play_idx,
                    })

    return scenarios


def save_scenarios(scenarios):
    """Insert scenarios into the database, skipping duplicates."""
    if not scenarios:
        return 0

    sql = """
        INSERT INTO manager_game_scenarios
            (game_pk, game_date, away_team_abbr, home_team_abbr,
             inning, half, outs, away_score, home_score, base_state,
             batter_id, batter_name, pitcher_id, pitcher_name,
             pitcher_pitch_count, pitcher_tto,
             decision_type, actual_decision, actual_detail,
             engine_recommendation, context_json, options_json, play_index)
        VALUES
            (%(game_pk)s, %(game_date)s, %(away_team_abbr)s, %(home_team_abbr)s,
             %(inning)s, %(half)s, %(outs)s, %(away_score)s, %(home_score)s, %(base_state)s,
             %(batter_id)s, %(batter_name)s, %(pitcher_id)s, %(pitcher_name)s,
             %(pitcher_pitch_count)s, %(pitcher_tto)s,
             %(decision_type)s, %(actual_decision)s, %(actual_detail)s,
             %(engine_recommendation)s, %(context_json)s, %(options_json)s, %(play_index)s)
        ON CONFLICT (game_pk, play_index, decision_type) DO NOTHING
    """
    inserted = 0
    with psycopg.connect(_db_url(), connect_timeout=15) as conn:
        with conn.cursor() as cur:
            for s in scenarios:
                try:
                    cur.execute(sql, s)
                    if cur.rowcount > 0:
                        inserted += 1
                except Exception as e:
                    print(f"  [WARN] Insert failed: {e}")
                    conn.rollback()
                    continue
        conn.commit()
    return inserted


def process_date(date_str):
    """Process all completed games for a date."""
    print(f"\n=== Processing {date_str} ===")
    final_games = _fetch_schedule(date_str)
    if not final_games:
        print(f"  No completed games found for {date_str}")
        return 0

    total_scenarios = 0
    print(f"  Found {len(final_games)} completed games")

    for game in final_games:
        game_pk = game.get("gamePk") or game.get("game_pk")
        if not game_pk:
            continue

        away = game.get("away_abbrev", "?")
        home = game.get("home_abbrev", "?")
        print(f"  Game {game_pk}: {away} @ {home}")

        try:
            feed = get_game_feed(game_pk)
            if not feed or feed.get("scheduleOnly"):
                print(f"    Skipped (no feed)")
                continue

            scenarios = extract_scenarios_from_game(feed, game_pk)
            if scenarios:
                inserted = save_scenarios(scenarios)
                print(f"    Extracted {len(scenarios)} scenarios, inserted {inserted} new")
                total_scenarios += inserted
            else:
                print(f"    No scenarios found")
        except Exception as e:
            print(f"    Error: {e}")
            continue

        time.sleep(0.5)  # Be nice to the API

    return total_scenarios


def main():
    parser = argparse.ArgumentParser(description="Extract manager decision scenarios from completed MLB games")
    parser.add_argument("--date", help="Date to process (YYYY-MM-DD or 'yesterday')")
    parser.add_argument("--start", help="Start date for range processing (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date for range processing (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.date:
        if args.date.lower() == "yesterday":
            dt = datetime.utcnow() - timedelta(days=1)
        elif args.date.lower() == "today":
            dt = datetime.utcnow()
        else:
            dt = datetime.strptime(args.date, "%Y-%m-%d")
        total = process_date(dt.strftime("%Y-%m-%d"))
        print(f"\nDone. {total} new scenarios inserted.")

    elif args.start and args.end:
        start = datetime.strptime(args.start, "%Y-%m-%d")
        end = datetime.strptime(args.end, "%Y-%m-%d")
        total = 0
        dt = start
        while dt <= end:
            total += process_date(dt.strftime("%Y-%m-%d"))
            dt += timedelta(days=1)
        print(f"\nDone. {total} total new scenarios inserted across date range.")

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
