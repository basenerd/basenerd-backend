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
        reasons.append(f"{ctx['pitcher_tto']}x through the order")
        confidence += 0.2
    leverage = _compute_leverage(ctx["inning"], ctx["outs"], ctx["score_diff"], ctx["base_state"])
    if leverage == "high":
        confidence += 0.15
        reasons.append("high leverage")
    if not reasons:
        reasons.append("late inning situation")
    recommend = "yes" if confidence >= 0.5 else "no"
    return {"recommend": recommend, "confidence": round(min(0.95, confidence), 2), "detail": "; ".join(reasons)}


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
    return {"recommend": recommend, "confidence": round(min(0.9, confidence), 2), "detail": "; ".join(reasons)}


def _engine_rec_stolen_base(ctx):
    """Heuristic engine recommendation for stolen base."""
    # Default: generally lean no unless clearly favorable
    return {"recommend": "yes", "confidence": 0.55, "detail": "Runner has base-stealing ability in a close game"}


def _engine_rec_bunt(ctx):
    """Heuristic engine recommendation for bunt."""
    bs = ctx["base_state"]
    outs = ctx["outs"]
    if outs != 0:
        return {"recommend": "no", "confidence": 0.7, "detail": "Sacrifice only makes sense with 0 outs"}
    # Compare P(score 1+) before and after bunt
    current_p1 = P_SCORE_1.get(0, {}).get(bs, 0.25)
    bunt_bs = 0
    if bs & 1: bunt_bs |= 2
    if bs & 2: bunt_bs |= 4
    if bs & 4: bunt_bs |= 4
    bunt_p1 = P_SCORE_1.get(1, {}).get(bunt_bs, 0.25)
    if bunt_p1 > current_p1:
        return {"recommend": "yes", "confidence": round(0.5 + (bunt_p1 - current_p1) * 2, 2),
                "detail": f"Bunting improves P(score 1+) from {current_p1:.0%} to {bunt_p1:.0%}"}
    return {"recommend": "no", "confidence": 0.6,
            "detail": f"Bunting lowers P(score 1+) from {current_p1:.0%} to {bunt_p1:.0%}"}


def _engine_rec_ibb(ctx):
    """Heuristic engine recommendation for IBB."""
    leverage = _compute_leverage(ctx["inning"], ctx["outs"], ctx["score_diff"], ctx["base_state"])
    if leverage == "high" and ctx["outs"] < 2:
        return {"recommend": "yes", "confidence": 0.6, "detail": "High leverage, sets up force/DP"}
    return {"recommend": "no", "confidence": 0.55, "detail": "Putting another runner on base is risky"}


ENGINE_RECS = {
    "pitching_change": _engine_rec_pitching_change,
    "pinch_hit": _engine_rec_pinch_hit,
    "stolen_base": _engine_rec_stolen_base,
    "bunt": _engine_rec_bunt,
    "ibb": _engine_rec_ibb,
}

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

    # Track running game state
    pitcher_pitch_counts = {}  # pitcher_id -> count
    pitcher_batters_faced = {}  # pitcher_id -> set of batter_ids
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

        # Track pitch counts
        if pitcher_id:
            if pitcher_id not in pitcher_pitch_counts:
                pitcher_pitch_counts[pitcher_id] = 0
                pitcher_batters_faced[pitcher_id] = set()
            current_pitcher[half] = pitcher_id

        # Count pitches in this at-bat
        ab_pitches = sum(1 for ev in play_events if ev.get("isPitch"))
        if pitcher_id:
            pitcher_pitch_counts[pitcher_id] = pitcher_pitch_counts.get(pitcher_id, 0) + ab_pitches
            if batter_id:
                pitcher_batters_faced.setdefault(pitcher_id, set()).add(batter_id)

        # Calculate state
        pitch_count = pitcher_pitch_counts.get(pitcher_id, 0) - ab_pitches  # pre-AB count
        tto = max(1, len(pitcher_batters_faced.get(pitcher_id, set())) // 9 + 1)

        # Runners (from play runners data or count)
        runners = play.get("runners") or []
        # Pre-play base state from count info
        count = play.get("count") or {}
        outs = count.get("outs") or about.get("outs") or 0

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
            "base_state": base_state,
            "batter_spot": batter_spot,
            "pitcher_pitch_count": pitch_count,
            "pitcher_tto": tto,
            "runner_names": runner_names,
            "venue": home_abbr,
            "season": season,
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
                    # Only in interesting situations
                    if inning >= 5 and abs(score_diff) <= 5:
                        rec = _engine_rec_pitching_change(ctx)
                        options = {**DECISION_QUESTIONS["pitching_change"],
                                   "situation": f"{situation_desc}. {pitcher_name} has {pitch_count} pitches, {tto}x through the order."}
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
                            "actual_decision": "yes",
                            "actual_detail": ev_desc,
                            "engine_recommendation": json.dumps(rec),
                            "context_json": json.dumps(ctx),
                            "options_json": json.dumps(options),
                            "play_index": play_idx,
                        })

        # 2) Pinch hit (offensive substitution)
        for ev in play_events:
            if not ev.get("isPitch") and ev.get("type") == "action":
                ev_event = (ev.get("details") or {}).get("eventType") or ""
                ev_desc = (ev.get("details") or {}).get("description") or ""
                if "offensive_substitution" in ev_event.lower() and "pinch" in ev_desc.lower():
                    if inning >= 5 and abs(score_diff) <= 5:
                        rec = _engine_rec_pinch_hit(ctx)
                        options = {**DECISION_QUESTIONS["pinch_hit"],
                                   "situation": f"{situation_desc}. Batting spot {batter_spot or '?'} in the order."}
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
                            "context_json": json.dumps(ctx),
                            "options_json": json.dumps(options),
                            "play_index": play_idx,
                        })

        if not is_complete:
            continue

        # 3) Stolen base / caught stealing
        if event_type in ("stolen_base_2b", "stolen_base_3b", "stolen_base_home",
                          "caught_stealing_2b", "caught_stealing_3b", "caught_stealing_home"):
            if abs(score_diff) <= 5:
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
            if abs(score_diff) <= 4:
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
            if abs(score_diff) <= 4:
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
        if play_type != "atBat" or abs(score_diff) > 4:
            continue

        # Non-event: pitcher left in despite fatigue
        if inning >= 6 and (pitch_count >= 85 or tto >= 3) and abs(score_diff) <= 3:
            leverage = _compute_leverage(inning, outs, score_diff, base_state)
            if leverage in ("high", "medium"):
                rec = _engine_rec_pitching_change(ctx)
                if rec["recommend"] == "yes":
                    options = {**DECISION_QUESTIONS["pitching_change"],
                               "situation": f"{situation_desc}. {pitcher_name} has {pitch_count} pitches, {tto}x through the order."}
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
