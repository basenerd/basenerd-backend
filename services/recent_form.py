"""
Recent form service — computes rolling 14-day stats for batters and pitchers.

Used at prediction time to capture hot/cold streaks.
Queries PostgreSQL directly for the most recent data.
"""

import os
import logging
from datetime import date, timedelta
from functools import lru_cache

import psycopg

log = logging.getLogger(__name__)


def _db_url():
    url = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_URL_PG") or ""
    if not url:
        raise RuntimeError("Missing DATABASE_URL env var")
    return url


# League-average fallbacks for recent form features
LEAGUE_AVG_BATTER_R14 = {
    "bat_r14_k_pct": 0.225,
    "bat_r14_bb_pct": 0.082,
    "bat_r14_xwoba": 0.315,
    "bat_r14_barrel_rate": 0.07,
    "bat_r14_whiff_rate": 0.245,
    "bat_r14_chase_rate": 0.295,
}

LEAGUE_AVG_PITCHER_R14 = {
    "p_r14_k_pct": 0.225,
    "p_r14_bb_pct": 0.082,
    "p_r14_xwoba": 0.315,
    "p_r14_whiff_rate": 0.245,
    "p_r14_chase_rate": 0.295,
}

# Minimum pitch threshold — below this, fall back to league average
MIN_PITCHES = 20


def _query_batter_form(batter_id, start_date, end_date):
    """Query rolling batter stats from statcast_pitches."""
    sql = """
    SELECT
        COUNT(*) FILTER (WHERE events IS NOT NULL AND events != '') AS pa,
        COUNT(*) AS pitches,
        SUM(CASE WHEN events IN ('strikeout','strikeout_double_play') THEN 1 ELSE 0 END) AS ks,
        SUM(CASE WHEN events = 'walk' THEN 1 ELSE 0 END) AS bbs,
        SUM(CASE WHEN description IN (
            'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt',
            'foul','foul_bunt','hit_into_play'
        ) THEN 1 ELSE 0 END) AS swings,
        SUM(CASE WHEN description IN (
            'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt'
        ) THEN 1 ELSE 0 END) AS whiffs,
        SUM(CASE WHEN zone IN (11,12,13,14) THEN 1 ELSE 0 END) AS chase_opps,
        SUM(CASE WHEN zone IN (11,12,13,14) AND description IN (
            'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt',
            'foul','foul_bunt','hit_into_play'
        ) THEN 1 ELSE 0 END) AS chases,
        AVG(estimated_woba_using_speedangle) AS xwoba,
        SUM(CASE WHEN launch_speed >= 98 AND launch_angle BETWEEN 26 AND 30 THEN 1 ELSE 0 END) AS barrels,
        SUM(CASE WHEN events IN (
            'single','double','triple','home_run','field_out',
            'grounded_into_double_play','double_play','fielders_choice',
            'fielders_choice_out','force_out','field_error','sac_fly'
        ) THEN 1 ELSE 0 END) AS bip
    FROM statcast_pitches
    WHERE batter = %s
      AND game_date BETWEEN %s AND %s
      AND game_type = 'R'
    """
    with psycopg.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (batter_id, start_date, end_date))
            row = cur.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))


def _query_pitcher_form(pitcher_id, start_date, end_date):
    """Query rolling pitcher stats from statcast_pitches."""
    sql = """
    SELECT
        COUNT(*) FILTER (WHERE events IS NOT NULL AND events != '') AS pa,
        COUNT(*) AS pitches,
        SUM(CASE WHEN events IN ('strikeout','strikeout_double_play') THEN 1 ELSE 0 END) AS ks,
        SUM(CASE WHEN events = 'walk' THEN 1 ELSE 0 END) AS bbs,
        SUM(CASE WHEN description IN (
            'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt',
            'foul','foul_bunt','hit_into_play'
        ) THEN 1 ELSE 0 END) AS swings,
        SUM(CASE WHEN description IN (
            'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt'
        ) THEN 1 ELSE 0 END) AS whiffs,
        SUM(CASE WHEN zone IN (11,12,13,14) THEN 1 ELSE 0 END) AS chase_opps,
        SUM(CASE WHEN zone IN (11,12,13,14) AND description IN (
            'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt',
            'foul','foul_bunt','hit_into_play'
        ) THEN 1 ELSE 0 END) AS chases,
        AVG(estimated_woba_using_speedangle) AS xwoba
    FROM statcast_pitches
    WHERE pitcher = %s
      AND game_date BETWEEN %s AND %s
      AND game_type = 'R'
    """
    with psycopg.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (pitcher_id, start_date, end_date))
            row = cur.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))


def _safe_div(a, b):
    if b and b > 0:
        return float(a) / float(b)
    return None


def get_batter_recent_form(batter_id, reference_date=None, window_days=14):
    """
    Get rolling 14-day batter stats.
    Returns dict with bat_r14_* keys, falling back to league avg if insufficient data.
    """
    if reference_date is None:
        reference_date = date.today()
    elif isinstance(reference_date, str):
        reference_date = date.fromisoformat(reference_date)

    end_date = reference_date - timedelta(days=1)  # exclude today (game in progress)
    start_date = end_date - timedelta(days=window_days - 1)

    try:
        raw = _query_batter_form(batter_id, start_date, end_date)
    except Exception as e:
        log.warning("Failed to query batter recent form for %s: %s", batter_id, e)
        return dict(LEAGUE_AVG_BATTER_R14)

    if not raw or (raw.get("pitches") or 0) < MIN_PITCHES:
        return dict(LEAGUE_AVG_BATTER_R14)

    pa = raw.get("pa") or 0
    result = {
        "bat_r14_k_pct": _safe_div(raw["ks"], pa) if pa > 0 else LEAGUE_AVG_BATTER_R14["bat_r14_k_pct"],
        "bat_r14_bb_pct": _safe_div(raw["bbs"], pa) if pa > 0 else LEAGUE_AVG_BATTER_R14["bat_r14_bb_pct"],
        "bat_r14_xwoba": float(raw["xwoba"]) if raw.get("xwoba") is not None else LEAGUE_AVG_BATTER_R14["bat_r14_xwoba"],
        "bat_r14_barrel_rate": _safe_div(raw["barrels"], raw["bip"]) if raw.get("bip") else LEAGUE_AVG_BATTER_R14["bat_r14_barrel_rate"],
        "bat_r14_whiff_rate": _safe_div(raw["whiffs"], raw["swings"]) if raw.get("swings") else LEAGUE_AVG_BATTER_R14["bat_r14_whiff_rate"],
        "bat_r14_chase_rate": _safe_div(raw["chases"], raw["chase_opps"]) if raw.get("chase_opps") else LEAGUE_AVG_BATTER_R14["bat_r14_chase_rate"],
    }
    return result


def get_pitcher_recent_form(pitcher_id, reference_date=None, window_days=14):
    """
    Get rolling 14-day pitcher stats.
    Returns dict with p_r14_* keys, falling back to league avg if insufficient data.
    """
    if reference_date is None:
        reference_date = date.today()
    elif isinstance(reference_date, str):
        reference_date = date.fromisoformat(reference_date)

    end_date = reference_date - timedelta(days=1)
    start_date = end_date - timedelta(days=window_days - 1)

    try:
        raw = _query_pitcher_form(pitcher_id, start_date, end_date)
    except Exception as e:
        log.warning("Failed to query pitcher recent form for %s: %s", pitcher_id, e)
        return dict(LEAGUE_AVG_PITCHER_R14)

    if not raw or (raw.get("pitches") or 0) < MIN_PITCHES:
        return dict(LEAGUE_AVG_PITCHER_R14)

    pa = raw.get("pa") or 0
    result = {
        "p_r14_k_pct": _safe_div(raw["ks"], pa) if pa > 0 else LEAGUE_AVG_PITCHER_R14["p_r14_k_pct"],
        "p_r14_bb_pct": _safe_div(raw["bbs"], pa) if pa > 0 else LEAGUE_AVG_PITCHER_R14["p_r14_bb_pct"],
        "p_r14_xwoba": float(raw["xwoba"]) if raw.get("xwoba") is not None else LEAGUE_AVG_PITCHER_R14["p_r14_xwoba"],
        "p_r14_whiff_rate": _safe_div(raw["whiffs"], raw["swings"]) if raw.get("swings") else LEAGUE_AVG_PITCHER_R14["p_r14_whiff_rate"],
        "p_r14_chase_rate": _safe_div(raw["chases"], raw["chase_opps"]) if raw.get("chase_opps") else LEAGUE_AVG_PITCHER_R14["p_r14_chase_rate"],
    }
    return result


def get_batch_recent_form(player_ids, role="batter", reference_date=None, window_days=14):
    """
    Batch query recent form for multiple players.
    Returns dict: {player_id: {feature_dict}}.
    """
    results = {}
    for pid in player_ids:
        if role == "batter":
            results[pid] = get_batter_recent_form(pid, reference_date, window_days)
        else:
            results[pid] = get_pitcher_recent_form(pid, reference_date, window_days)
    return results
