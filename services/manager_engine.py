# services/manager_engine.py
"""
Basenerd Manager Decision Engine.

Evaluates the live game state and returns contextual managerial
recommendations (pitching changes, pinch hits, stolen bases, bunts, IBBs)
gated by inning, score, leverage and game situation so they only appear
when a real manager would plausibly consider them.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configurable trigger thresholds
# ---------------------------------------------------------------------------
MIN_INNING_SUB = 6          # subs only considered from this inning on
MIN_INNING_SUB_FATIGUE = 5  # ...unless pitcher fatigued
FATIGUE_PITCH_COUNT = 90
FATIGUE_TTO = 3
MAX_SCORE_DIFF_SUB = 3
MAX_SCORE_DIFF_SB = 4
MAX_SCORE_DIFF_BUNT = 2
MAX_SCORE_DIFF_IBB = 2
MIN_INNING_BUNT = 7
MIN_INNING_IBB = 7
MIN_INNING_PH = 7
MIN_INNING_PH_HIGH_LEV = 6
OBP_IMPROVE_THRESH_PH = 0.08   # 8% OBP improvement needed for PH
OBP_IMPROVE_THRESH_PC = 0.05   # 5% for pitching change
OBP_DELTA_IBB = 0.060          # IBB if batter OBP this much better than next

# Run-expectancy table (2024 league averages, 24 base-out states)
# Format: RE24[outs][base_state] where base_state is 3-bit int (1B=1, 2B=2, 3B=4)
RE24 = {
    0: {0: 0.461, 1: 0.831, 2: 1.068, 3: 1.373, 4: 1.270, 5: 1.632, 6: 1.825, 7: 2.151},
    1: {0: 0.245, 1: 0.489, 2: 0.644, 3: 0.867, 4: 0.899, 5: 1.088, 6: 1.275, 7: 1.474},
    2: {0: 0.095, 1: 0.214, 2: 0.305, 3: 0.429, 4: 0.353, 5: 0.468, 6: 0.538, 7: 0.693},
}

# Probability of scoring >= 1 run from each base-out state
P_SCORE_1 = {
    0: {0: 0.255, 1: 0.415, 2: 0.608, 3: 0.637, 4: 0.831, 5: 0.855, 6: 0.874, 7: 0.869},
    1: {0: 0.153, 1: 0.265, 2: 0.390, 3: 0.413, 4: 0.650, 5: 0.660, 6: 0.676, 7: 0.699},
    2: {0: 0.065, 1: 0.130, 2: 0.222, 3: 0.226, 4: 0.263, 5: 0.278, 6: 0.270, 7: 0.326},
}

# SB break-even success rates
SB_BREAKEVEN = {
    # (from_base, outs) -> required success %
    (1, 0): 0.769,
    (1, 1): 0.699,
    (1, 2): 0.679,
    (2, 0): 0.947,  # stealing 3rd is very risky
    (2, 1): 0.746,
    (2, 2): 0.798,
}

# Module-level cache: (game_pk, key_tuple) -> (result, timestamp)
_cache: dict = {}
_CACHE_TTL = 10  # seconds


# ===================================================================
# PUBLIC API
# ===================================================================

def evaluate_decisions(feed: dict, game_pk: int) -> list[dict]:
    """
    Evaluate the current game state and return 0-2 recommendations.

    Parameters
    ----------
    feed : dict
        Raw MLB live feed from get_game_feed().
    game_pk : int
        Game primary key.

    Returns
    -------
    list of recommendation dicts, sorted by confidence descending, max 2.
    """
    ctx = _extract_game_context(feed)
    if not ctx or ctx["state"] != "live":
        return []

    # Cache key
    cache_key = (
        game_pk, ctx["inning"], ctx["half"], ctx["outs"],
        ctx["batter_id"], ctx["pitcher_id"], ctx["base_state"],
    )
    cached = _cache.get(cache_key)
    if cached:
        result, ts = cached
        if time.time() - ts < _CACHE_TTL:
            return result

    recommendations = []

    # --- Pitching change ---
    inning = ctx["inning"]
    pc_eligible = (
        inning >= MIN_INNING_SUB
        or (inning >= MIN_INNING_SUB_FATIGUE
            and (ctx["pitcher_pitch_count"] >= FATIGUE_PITCH_COUNT
                 or ctx["pitcher_tto"] >= FATIGUE_TTO))
    )
    if pc_eligible and abs(ctx["score_diff"]) <= MAX_SCORE_DIFF_SUB:
        try:
            relievers = _extract_available_relievers(feed, ctx["pitching_side"], ctx["game_date"], ctx["pitching_team_id"])
            rec = _evaluate_pitching_change(ctx, relievers)
            if rec:
                recommendations.append(rec)
        except Exception as e:
            log.debug("pitching_change eval error: %s", e)

    # --- Pinch hit ---
    ph_eligible = (
        inning >= MIN_INNING_PH
        or (inning >= MIN_INNING_PH_HIGH_LEV
            and _compute_leverage(ctx) == "high")
    )
    if ph_eligible and abs(ctx["score_diff"]) <= MAX_SCORE_DIFF_SUB:
        try:
            bench = _extract_bench_players(feed, ctx["batting_side"])
            rec = _evaluate_pinch_hit(ctx, bench)
            if rec:
                recommendations.append(rec)
        except Exception as e:
            log.debug("pinch_hit eval error: %s", e)

    # --- Stolen base (any inning) ---
    has_sb_runner = ctx["runner_1b_id"] or ctx["runner_2b_id"]
    if has_sb_runner and ctx["outs"] < 2 and abs(ctx["score_diff"]) <= MAX_SCORE_DIFF_SB:
        try:
            rec = _evaluate_stolen_base(ctx)
            if rec:
                recommendations.append(rec)
        except Exception as e:
            log.debug("stolen_base eval error: %s", e)

    # --- Bunt ---
    if ctx["outs"] == 0 and has_sb_runner and inning >= MIN_INNING_BUNT and abs(ctx["score_diff"]) <= MAX_SCORE_DIFF_BUNT:
        try:
            rec = _evaluate_bunt(ctx)
            if rec:
                recommendations.append(rec)
        except Exception as e:
            log.debug("bunt eval error: %s", e)

    # --- IBB ---
    first_open = ctx["runner_1b_id"] is None
    if first_open and inning >= MIN_INNING_IBB and abs(ctx["score_diff"]) <= MAX_SCORE_DIFF_IBB:
        try:
            rec = _evaluate_ibb(ctx)
            if rec:
                recommendations.append(rec)
        except Exception as e:
            log.debug("ibb eval error: %s", e)

    # Sort by confidence, take top 2
    recommendations.sort(key=lambda r: r.get("confidence", 0), reverse=True)
    result = recommendations[:2]

    _cache[cache_key] = (result, time.time())
    return result


# ===================================================================
# CONTEXT EXTRACTION
# ===================================================================

def _extract_game_context(feed: dict) -> dict | None:
    """Parse the MLB live feed into a normalised context dict."""
    if not feed or feed.get("scheduleOnly"):
        return None

    game_data = feed.get("gameData") or {}
    live_data = feed.get("liveData") or {}
    linescore = live_data.get("linescore") or {}
    plays = live_data.get("plays") or {}
    current_play = plays.get("currentPlay") or {}
    matchup = current_play.get("matchup") or {}
    boxscore = live_data.get("boxscore") or {}

    status = (game_data.get("status") or {}).get("abstractGameState", "")
    if status.lower() != "live":
        return None

    inning = linescore.get("currentInning") or 1
    inning_half = (linescore.get("inningHalf") or "top").lower()
    half = "top" if "top" in inning_half else "bottom"
    outs = linescore.get("outs") or 0

    # Score
    teams_ls = linescore.get("teams") or {}
    away_score = (teams_ls.get("away") or {}).get("runs") or 0
    home_score = (teams_ls.get("home") or {}).get("runs") or 0
    # Positive = batting team leading
    if half == "top":
        score_diff = away_score - home_score
        batting_side = "away"
        pitching_side = "home"
    else:
        score_diff = home_score - away_score
        batting_side = "home"
        pitching_side = "away"

    # Batter / pitcher
    batter_id = (matchup.get("batter") or {}).get("id")
    pitcher_id = (matchup.get("pitcher") or {}).get("id")
    if not batter_id or not pitcher_id:
        return None

    stand = ((matchup.get("batSide") or {}).get("code") or "R").upper()
    p_throws = ((matchup.get("pitchHand") or {}).get("code") or "R").upper()

    # Runners
    offense = linescore.get("offense") or {}
    runner_1b_id = (offense.get("first") or {}).get("id")
    runner_2b_id = (offense.get("second") or {}).get("id")
    runner_3b_id = (offense.get("third") or {}).get("id")
    base_state = (1 if runner_1b_id else 0) | (2 if runner_2b_id else 0) | (4 if runner_3b_id else 0)

    # Venue
    teams_gd = game_data.get("teams") or {}
    venue = ((teams_gd.get("home") or {}).get("abbreviation") or "").upper() or None

    # Season
    season = int((game_data.get("game") or {}).get("season", 2025) or 2025)

    # Game date
    game_date = (game_data.get("datetime") or {}).get("officialDate") or datetime.utcnow().strftime("%Y-%m-%d")

    # Team IDs
    batting_team_id = ((teams_gd.get(batting_side) or {}).get("id"))
    pitching_team_id = ((teams_gd.get(pitching_side) or {}).get("id"))

    # Times through order for current pitcher
    all_plays = plays.get("allPlays") or []
    pitcher_tto = 1
    pitcher_pitch_count = 0
    fb_velos = []
    try:
        seen_batters = set()
        total_pitches = 0
        fb_codes = {"FF", "SI", "FC"}
        for p in all_plays:
            pm = p.get("matchup") or {}
            if (pm.get("pitcher") or {}).get("id") != pitcher_id:
                continue
            bid = (pm.get("batter") or {}).get("id")
            if bid:
                seen_batters.add(bid)
            for ev in (p.get("playEvents") or []):
                if not ev.get("isPitch"):
                    continue
                total_pitches += 1
                pt_code = ((ev.get("details") or {}).get("type") or {}).get("code") or ""
                if pt_code in fb_codes:
                    spd = (ev.get("pitchData") or {}).get("startSpeed")
                    if spd:
                        fb_velos.append(float(spd))
        if seen_batters:
            pitcher_tto = max(1, len(seen_batters) // 9 + 1)
        pitcher_pitch_count = total_pitches
    except Exception:
        pass

    pitcher_velo_tonight = sum(fb_velos) / len(fb_velos) if fb_velos else None

    # Batting order for batting team (to find lineup spot & on-deck)
    bt = (boxscore.get("teams") or {}).get(batting_side) or {}
    batting_order_ids = [int(x) for x in (bt.get("battingOrder") or [])]

    # Current batter's lineup spot (1-9)
    batter_spot = None
    for i, bid in enumerate(batting_order_ids):
        if bid == batter_id:
            batter_spot = i + 1
            break

    # On-deck batter
    on_deck_id = None
    if batter_spot and batting_order_ids:
        next_idx = batter_spot % len(batting_order_ids)  # wraps 9 -> 0
        on_deck_id = batting_order_ids[next_idx]

    return {
        "state": "live",
        "inning": inning,
        "half": half,
        "outs": outs,
        "score_diff": score_diff,
        "away_score": away_score,
        "home_score": home_score,
        "batting_side": batting_side,
        "pitching_side": pitching_side,
        "batting_team_id": batting_team_id,
        "pitching_team_id": pitching_team_id,
        "batter_id": int(batter_id),
        "pitcher_id": int(pitcher_id),
        "stand": stand,
        "p_throws": p_throws,
        "batter_spot": batter_spot,
        "on_deck_id": on_deck_id,
        "batting_order": batting_order_ids,
        "runner_1b_id": runner_1b_id,
        "runner_2b_id": runner_2b_id,
        "runner_3b_id": runner_3b_id,
        "base_state": base_state,
        "venue": venue,
        "season": season,
        "game_date": game_date,
        "pitcher_tto": pitcher_tto,
        "pitcher_pitch_count": pitcher_pitch_count,
        "pitcher_velo_tonight": pitcher_velo_tonight,
        "feed": feed,  # kept for sub-extractors
    }


def _extract_bench_players(feed: dict, side: str) -> list[dict]:
    """
    Identify bench position players from the full boxscore.

    The boxscore 'players' dict contains the entire 26-man roster.
    Subtract batting-order IDs and pitcher IDs to find bench players.
    """
    boxscore = (feed.get("liveData") or {}).get("boxscore") or {}
    team_box = (boxscore.get("teams") or {}).get(side) or {}
    players = team_box.get("players") or {}
    batting_order = set(int(x) for x in (team_box.get("battingOrder") or []))
    pitcher_ids = set(int(x) for x in (team_box.get("pitchers") or []))

    game_players = (feed.get("gameData") or {}).get("players") or {}

    bench = []
    for key, pdata in players.items():
        try:
            pid = int(key.replace("ID", ""))
        except (ValueError, TypeError):
            continue

        # Skip players already in the batting order or pitching staff
        if pid in batting_order or pid in pitcher_ids:
            continue

        # Skip if player already entered the game as a sub
        game_status = (pdata.get("gameStatus") or {}).get("isCurrentBatter", False)
        gs = pdata.get("gameStatus") or {}
        if gs.get("isSubstitute") or gs.get("isOnBench") is False:
            # They've already entered — skip
            pass

        # Get player info from gameData.players
        gp = game_players.get(key) or {}
        pos = ((pdata.get("position") or gp.get("primaryPosition") or {}).get("abbreviation") or "")
        bat_side = ((gp.get("batSide") or {}).get("code") or "R").upper()

        # Skip pitchers on the bench (they'd show up in reliever eval)
        if pos in ("P", "TWP"):
            continue

        # Check they haven't already batted (stats should be zeros)
        batting = (pdata.get("stats") or {}).get("batting") or {}
        plate_appearances = batting.get("plateAppearances") or batting.get("atBats") or 0
        if int(plate_appearances) > 0:
            continue  # already used

        name = gp.get("fullName") or gp.get("lastName") or str(pid)

        bench.append({
            "id": pid,
            "name": name,
            "bat_side": bat_side,
            "position": pos,
        })

    return bench


def _extract_available_relievers(feed: dict, pitching_side: str, game_date: str, team_id: int) -> list[dict]:
    """
    Get available relievers: haven't pitched today & bullpen status is
    AVAILABLE or LIMITED.
    """
    from services.bullpen_availability import get_bullpen_availability, AVAILABLE, LIMITED

    boxscore = (feed.get("liveData") or {}).get("boxscore") or {}
    team_box = (boxscore.get("teams") or {}).get(pitching_side) or {}
    already_pitched = set(int(x) for x in (team_box.get("pitchers") or []))

    game_players = (feed.get("gameData") or {}).get("players") or {}

    # Get bullpen availability from recent workload
    bullpen = get_bullpen_availability(team_id, game_date)
    available = []
    for bp in bullpen:
        pid = bp["id"]
        if pid in already_pitched:
            continue
        if bp["status"] not in (AVAILABLE, LIMITED):
            continue

        # Get throwing hand
        key = f"ID{pid}"
        gp = game_players.get(key) or {}
        throws = ((gp.get("pitchHand") or {}).get("code") or "R").upper()

        available.append({
            "id": pid,
            "name": bp["name"],
            "throws": throws,
            "status": bp["status"],
            "workload": bp["workload"],
            "note": bp.get("note", ""),
        })

    return available[:5]  # limit to top 5 by availability


def _compute_leverage(ctx: dict) -> str:
    """Heuristic leverage level."""
    inning = ctx["inning"]
    outs = ctx["outs"]
    diff = abs(ctx["score_diff"])
    bs = ctx["base_state"]
    risp = bs & 0b110  # runner on 2nd or 3rd

    if inning >= 7 and diff <= 2 and risp and outs < 2:
        return "high"
    if inning >= 7 and diff <= 1:
        return "high"
    if inning >= 6 and diff <= 3:
        return "medium"
    if inning >= 8 and diff <= 4:
        return "medium"
    return "low"


# ===================================================================
# DECISION EVALUATORS
# ===================================================================

def _evaluate_stolen_base(ctx: dict) -> dict | None:
    """Evaluate stolen-base opportunity."""
    feed = ctx["feed"]
    game_players = (feed.get("gameData") or {}).get("players") or {}

    candidates = []
    for base, runner_id in [(1, ctx["runner_1b_id"]), (2, ctx["runner_2b_id"])]:
        if not runner_id:
            continue

        # Don't recommend steal of 3rd with runner on 1st too (double steal is complex)
        if base == 2 and ctx["runner_1b_id"]:
            continue

        # Get runner info
        key = f"ID{runner_id}"
        gp = game_players.get(key) or {}
        runner_name = gp.get("lastName") or gp.get("fullName") or str(runner_id)

        # Check season SB from boxscore or player stats
        # The live feed boxscore has season stats for batters
        boxscore = (feed.get("liveData") or {}).get("boxscore") or {}
        batting_team = (boxscore.get("teams") or {}).get(ctx["batting_side"]) or {}
        p_data = (batting_team.get("players") or {}).get(key) or {}
        season_stats = (p_data.get("seasonStats") or {}).get("batting") or {}
        stolen_bases = int(season_stats.get("stolenBases") or 0)
        caught_stealing = int(season_stats.get("caughtStealing") or 0)

        # Estimate success rate
        sb_attempts = stolen_bases + caught_stealing
        if sb_attempts >= 3:
            success_rate = stolen_bases / sb_attempts
        elif stolen_bases >= 5:
            success_rate = 0.78  # assume good base stealer
        else:
            continue  # not enough data to recommend

        breakeven = SB_BREAKEVEN.get((base, ctx["outs"]), 0.75)
        if success_rate < breakeven:
            continue

        # Compute EV gain
        current_re = RE24.get(ctx["outs"], {}).get(ctx["base_state"], 0.3)
        # Success: runner advances one base
        success_bs = ctx["base_state"] & ~(1 << (base - 1))  # remove from current
        success_bs |= (1 << base)  # add to next base (2B=bit2, 3B=bit4... using 1,2,4)
        if base == 1:
            success_bs = (ctx["base_state"] & ~1) | 2  # 1B -> 2B
        elif base == 2:
            success_bs = (ctx["base_state"] & ~2) | 4  # 2B -> 3B
        success_re = RE24.get(ctx["outs"], {}).get(success_bs, 0.3)

        # Caught stealing: runner out, add 1 out
        cs_outs = ctx["outs"] + 1
        cs_bs = ctx["base_state"] & ~(1 << (base - 1))
        cs_re = RE24.get(cs_outs, {}).get(cs_bs, 0) if cs_outs < 3 else 0

        ev_gain = success_rate * success_re + (1 - success_rate) * cs_re - current_re
        if ev_gain <= 0:
            continue

        confidence = min(0.9, 0.4 + ev_gain * 2 + (success_rate - breakeven) * 2)

        target = "2nd" if base == 1 else "3rd"
        sb_display = f"{stolen_bases}-for-{sb_attempts}" if sb_attempts >= 3 else f"{stolen_bases} SB"

        candidates.append({
            "type": "stolen_base",
            "team": ctx["batting_side"],
            "headline": f"Stolen base opportunity \u2014 {runner_name} to {target}",
            "detail": f"{runner_name} is {sb_display} on steal attempts ({success_rate:.0%} success). Break-even is {breakeven:.0%}. +{ev_gain:.3f} run expectancy.",
            "confidence": round(confidence, 2),
            "leverage": _compute_leverage(ctx),
        })

    if not candidates:
        return None
    return max(candidates, key=lambda c: c["confidence"])


def _evaluate_bunt(ctx: dict) -> dict | None:
    """Evaluate sacrifice bunt."""
    # Only with runner on 1st or 2nd, 0 outs, bottom of order
    if ctx["outs"] != 0:
        return None
    if not ctx["batter_spot"] or ctx["batter_spot"] < 7:
        return None

    # Current state
    current_re = RE24.get(0, {}).get(ctx["base_state"], 0.3)
    current_p1 = P_SCORE_1.get(0, {}).get(ctx["base_state"], 0.2)

    # After successful bunt: runner advances, 1 out
    # Runner on 1B -> runner on 2B, 1 out
    # Runner on 2B -> runner on 3B, 1 out
    # Runner on 1B+2B -> runners on 2B+3B, 1 out
    bunt_bs = 0
    if ctx["base_state"] & 1:  # runner on 1st
        bunt_bs |= 2  # advances to 2nd
    if ctx["base_state"] & 2:  # runner on 2nd
        bunt_bs |= 4  # advances to 3rd
    if ctx["base_state"] & 4:  # runner on 3rd stays
        bunt_bs |= 4

    bunt_re = RE24.get(1, {}).get(bunt_bs, 0.3)
    bunt_p1 = P_SCORE_1.get(1, {}).get(bunt_bs, 0.2)

    # Only recommend if P(score 1+) improves
    p1_gain = bunt_p1 - current_p1
    if p1_gain <= 0:
        return None

    # Confidence based on how much P(score 1+) improves
    confidence = min(0.85, 0.3 + p1_gain * 3)

    return {
        "type": "bunt",
        "team": ctx["batting_side"],
        "headline": "Sacrifice bunt consideration",
        "detail": f"Bunting advances runner to scoring position. Probability of scoring at least 1 run: {current_p1:.0%} \u2192 {bunt_p1:.0%} (+{p1_gain:.0%}). Late close game with bottom of order up.",
        "confidence": round(confidence, 2),
        "leverage": _compute_leverage(ctx),
    }


def _evaluate_pitching_change(ctx: dict, relievers: list[dict]) -> dict | None:
    """Evaluate whether to pull the current pitcher."""
    if not relievers:
        return None

    leverage = _compute_leverage(ctx)
    if leverage == "low":
        return None

    from services.matchup_predict import predict_matchup_live

    # Current pitcher vs current batter
    current_result = predict_matchup_live(
        batter_id=ctx["batter_id"],
        pitcher_id=ctx["pitcher_id"],
        stand=ctx["stand"],
        p_throws=ctx["p_throws"],
        venue=ctx["venue"],
        season=ctx["season"],
        inning=ctx["inning"],
        outs=ctx["outs"],
        runner_1b=1 if ctx["runner_1b_id"] else 0,
        runner_2b=1 if ctx["runner_2b_id"] else 0,
        runner_3b=1 if ctx["runner_3b_id"] else 0,
        n_thru_order=ctx["pitcher_tto"],
        pitcher_velo_tonight=ctx["pitcher_velo_tonight"],
        pitcher_pitch_count=ctx["pitcher_pitch_count"],
    )
    if not current_result.get("ok"):
        return None

    current_obp = current_result.get("summary", {}).get("obp", 0.320)

    # Evaluate top 3 relievers
    best = None
    for rel in relievers[:3]:
        try:
            # Resolve batter stand for switch hitters vs this reliever
            rel_stand = ctx["stand"]
            if rel_stand == "S":
                rel_stand = "L" if rel["throws"] == "R" else "R"

            rel_result = predict_matchup_live(
                batter_id=ctx["batter_id"],
                pitcher_id=rel["id"],
                stand=rel_stand,
                p_throws=rel["throws"],
                venue=ctx["venue"],
                season=ctx["season"],
                inning=ctx["inning"],
                outs=ctx["outs"],
                runner_1b=1 if ctx["runner_1b_id"] else 0,
                runner_2b=1 if ctx["runner_2b_id"] else 0,
                runner_3b=1 if ctx["runner_3b_id"] else 0,
                n_thru_order=1,
            )
            if not rel_result.get("ok"):
                continue

            rel_obp = rel_result.get("summary", {}).get("obp", 0.320)
            improvement = current_obp - rel_obp

            if improvement >= OBP_IMPROVE_THRESH_PC:
                if best is None or improvement > best["improvement"]:
                    best = {
                        "reliever": rel,
                        "improvement": improvement,
                        "rel_obp": rel_obp,
                        "rel_k_pct": rel_result.get("summary", {}).get("k_pct", 0),
                        "cur_k_pct": current_result.get("summary", {}).get("k_pct", 0),
                    }
        except Exception as e:
            log.debug("reliever eval error for %s: %s", rel.get("id"), e)
            continue

    if not best:
        return None

    rel = best["reliever"]
    reasons = []
    if ctx["pitcher_pitch_count"] >= 85:
        reasons.append(f"{ctx['pitcher_pitch_count']} pitches")
    if ctx["pitcher_tto"] >= 3:
        reasons.append(f"{ctx['pitcher_tto']}x through the order")
    platoon = ""
    if rel["throws"] != ctx["p_throws"]:
        if (ctx["stand"] == "L" and rel["throws"] == "R") or (ctx["stand"] == "R" and rel["throws"] == "L"):
            pass  # no platoon advantage for reliever
        else:
            platoon = f" ({rel['throws']}HP vs {ctx['stand']}HB platoon advantage)"
            reasons.append("platoon advantage")

    reason_str = ", ".join(reasons) if reasons else "matchup improvement"
    obp_delta_pct = round(best["improvement"] * 100, 1)

    confidence = min(0.95, 0.4 + best["improvement"] * 3 + (0.15 if leverage == "high" else 0))

    return {
        "type": "pitching_change",
        "team": ctx["pitching_side"],
        "headline": f"Consider bringing in {rel['name']}",
        "detail": f"{reason_str}. {rel['name']}{platoon} projects {obp_delta_pct}% lower OBP vs this batter. Status: {rel['status']} ({rel['note']}).",
        "confidence": round(confidence, 2),
        "leverage": leverage,
    }


def _evaluate_pinch_hit(ctx: dict, bench: list[dict]) -> dict | None:
    """Evaluate pinch-hit opportunities."""
    if not bench:
        return None
    if not ctx["batter_spot"] or ctx["batter_spot"] < 7:
        return None  # only bottom third of order

    leverage = _compute_leverage(ctx)

    from services.matchup_predict import predict_matchup_live

    # Current batter vs current pitcher
    current_result = predict_matchup_live(
        batter_id=ctx["batter_id"],
        pitcher_id=ctx["pitcher_id"],
        stand=ctx["stand"],
        p_throws=ctx["p_throws"],
        venue=ctx["venue"],
        season=ctx["season"],
        inning=ctx["inning"],
        outs=ctx["outs"],
        runner_1b=1 if ctx["runner_1b_id"] else 0,
        runner_2b=1 if ctx["runner_2b_id"] else 0,
        runner_3b=1 if ctx["runner_3b_id"] else 0,
        n_thru_order=ctx["pitcher_tto"],
        pitcher_velo_tonight=ctx["pitcher_velo_tonight"],
        pitcher_pitch_count=ctx["pitcher_pitch_count"],
    )
    if not current_result.get("ok"):
        return None

    current_obp = current_result.get("summary", {}).get("obp", 0.320)

    # Evaluate top 3 bench players (prefer platoon advantage)
    scored = []
    for bp in bench:
        # Platoon advantage score: opposite hand from pitcher = bonus
        platoon_bonus = 1 if (bp["bat_side"] == "L" and ctx["p_throws"] == "R") or \
                              (bp["bat_side"] == "R" and ctx["p_throws"] == "L") else 0
        scored.append((platoon_bonus, bp))
    scored.sort(key=lambda x: x[0], reverse=True)

    best = None
    for _, bp in scored[:3]:
        try:
            bp_stand = bp["bat_side"]
            if bp_stand == "S":
                bp_stand = "L" if ctx["p_throws"] == "R" else "R"

            bp_result = predict_matchup_live(
                batter_id=bp["id"],
                pitcher_id=ctx["pitcher_id"],
                stand=bp_stand,
                p_throws=ctx["p_throws"],
                venue=ctx["venue"],
                season=ctx["season"],
                inning=ctx["inning"],
                outs=ctx["outs"],
                runner_1b=1 if ctx["runner_1b_id"] else 0,
                runner_2b=1 if ctx["runner_2b_id"] else 0,
                runner_3b=1 if ctx["runner_3b_id"] else 0,
                n_thru_order=1,
            )
            if not bp_result.get("ok"):
                continue

            bp_obp = bp_result.get("summary", {}).get("obp", 0.320)
            improvement = bp_obp - current_obp

            if improvement >= OBP_IMPROVE_THRESH_PH:
                if best is None or improvement > best["improvement"]:
                    best = {
                        "player": bp,
                        "improvement": improvement,
                        "bp_obp": bp_obp,
                    }
        except Exception as e:
            log.debug("pinch_hit eval error for %s: %s", bp.get("id"), e)
            continue

    if not best:
        return None

    bp = best["player"]
    obp_delta_pct = round(best["improvement"] * 100, 1)
    platoon_note = ""
    if (bp["bat_side"] in ("L", "S") and ctx["p_throws"] == "R") or \
       (bp["bat_side"] == "R" and ctx["p_throws"] == "L"):
        platoon_note = " with platoon advantage"

    confidence = min(0.9, 0.35 + best["improvement"] * 3 + (0.1 if leverage == "high" else 0))

    # Get current batter name from feed
    game_players = (ctx["feed"].get("gameData") or {}).get("players") or {}
    cur_key = f"ID{ctx['batter_id']}"
    cur_name = (game_players.get(cur_key) or {}).get("lastName") or str(ctx["batter_id"])

    return {
        "type": "pinch_hit",
        "team": ctx["batting_side"],
        "headline": f"Consider pinch-hitting {bp['name']}",
        "detail": f"{bp['name']} ({bp['bat_side']}HB, {bp['position']}){platoon_note} projects +{obp_delta_pct}% OBP over {cur_name} in this matchup. Spot {ctx['batter_spot']} in the order.",
        "confidence": round(confidence, 2),
        "leverage": leverage,
    }


def _evaluate_ibb(ctx: dict) -> dict | None:
    """Evaluate intentional walk consideration."""
    if ctx["runner_1b_id"]:
        return None  # first base not open
    if not ctx["on_deck_id"]:
        return None

    leverage = _compute_leverage(ctx)
    if leverage == "low":
        return None

    from services.matchup_predict import predict_matchup_live

    # Current batter matchup
    current_result = predict_matchup_live(
        batter_id=ctx["batter_id"],
        pitcher_id=ctx["pitcher_id"],
        stand=ctx["stand"],
        p_throws=ctx["p_throws"],
        venue=ctx["venue"],
        season=ctx["season"],
        inning=ctx["inning"],
        outs=ctx["outs"],
        runner_1b=1 if ctx["runner_1b_id"] else 0,
        runner_2b=1 if ctx["runner_2b_id"] else 0,
        runner_3b=1 if ctx["runner_3b_id"] else 0,
        n_thru_order=ctx["pitcher_tto"],
        pitcher_velo_tonight=ctx["pitcher_velo_tonight"],
        pitcher_pitch_count=ctx["pitcher_pitch_count"],
    )
    if not current_result.get("ok"):
        return None

    # On-deck batter matchup (after IBB — runner now on 1st)
    game_players = (ctx["feed"].get("gameData") or {}).get("players") or {}
    od_key = f"ID{ctx['on_deck_id']}"
    od_info = game_players.get(od_key) or {}
    od_stand = ((od_info.get("batSide") or {}).get("code") or "R").upper()
    if od_stand == "S":
        od_stand = "L" if ctx["p_throws"] == "R" else "R"

    # After IBB, base state changes: add runner on 1st
    ibb_r1b = 1
    ibb_r2b = 1 if ctx["runner_2b_id"] else (1 if ctx["runner_1b_id"] else 0)
    ibb_r3b = 1 if ctx["runner_3b_id"] else (1 if ctx["runner_2b_id"] and ctx["runner_1b_id"] else 0)

    next_result = predict_matchup_live(
        batter_id=ctx["on_deck_id"],
        pitcher_id=ctx["pitcher_id"],
        stand=od_stand,
        p_throws=ctx["p_throws"],
        venue=ctx["venue"],
        season=ctx["season"],
        inning=ctx["inning"],
        outs=ctx["outs"],
        runner_1b=ibb_r1b,
        runner_2b=ibb_r2b,
        runner_3b=ibb_r3b,
        n_thru_order=ctx["pitcher_tto"],
        pitcher_velo_tonight=ctx["pitcher_velo_tonight"],
        pitcher_pitch_count=ctx["pitcher_pitch_count"],
    )
    if not next_result.get("ok"):
        return None

    current_obp = current_result.get("summary", {}).get("obp", 0.320)
    next_obp = next_result.get("summary", {}).get("obp", 0.320)
    obp_delta = current_obp - next_obp

    if obp_delta < OBP_DELTA_IBB:
        return None

    # Check for DP setup bonus
    dp_note = ""
    if ctx["outs"] < 2 and not ctx["runner_1b_id"]:
        dp_note = " Sets up force/double play at every base."

    cur_name = (game_players.get(f"ID{ctx['batter_id']}") or {}).get("lastName") or str(ctx["batter_id"])
    od_name = od_info.get("lastName") or od_info.get("fullName") or str(ctx["on_deck_id"])

    confidence = min(0.85, 0.35 + obp_delta * 2 + (0.1 if leverage == "high" else 0))

    # Check HR threat
    current_hr = current_result.get("probs", {}).get("HR", 0)
    hr_note = ""
    if current_hr >= 0.05:
        hr_note = f" {cur_name} has {current_hr:.1%} HR probability here."

    return {
        "type": "ibb",
        "team": ctx["pitching_side"],
        "headline": f"Intentional walk consideration \u2014 {cur_name}",
        "detail": f"{cur_name} projects .{int(current_obp*1000):03d} OBP vs .{int(next_obp*1000):03d} for {od_name} on deck ({obp_delta:.0%} gap).{hr_note}{dp_note}",
        "confidence": round(confidence, 2),
        "leverage": leverage,
    }
