# services/manager_game.py
"""
Backend service for the Manager Decision Game quiz.

Provides functions to fetch random scenarios, record user responses,
and compute community stats.
"""

from __future__ import annotations

import json
import logging
import os
from collections import Counter

import psycopg

log = logging.getLogger(__name__)


def _db_url():
    url = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_URL_PG") or ""
    if not url:
        raise RuntimeError("Missing DATABASE_URL env var")
    return url


def get_quiz_scenarios(n: int = 10) -> list[dict]:
    """
    Fetch n random scenarios with diversity enforcement.

    Ensures: max 3 of any single decision_type, from at least 3 different games.
    """
    # Fetch extra candidates, then filter for diversity
    fetch_limit = max(n * 4, 40)

    sql = """
        SELECT id, game_pk, game_date, away_team_abbr, home_team_abbr,
               inning, half, outs, away_score, home_score, base_state,
               batter_id, batter_name, pitcher_id, pitcher_name,
               pitcher_pitch_count, pitcher_tto,
               decision_type, actual_decision, actual_detail,
               engine_recommendation, context_json, options_json
        FROM manager_game_scenarios
        ORDER BY RANDOM()
        LIMIT %s
    """

    with psycopg.connect(_db_url(), connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (fetch_limit,))
            cols = [desc.name for desc in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    if not rows:
        return []

    # Diversity filter
    selected = []
    type_counts = Counter()
    game_pks = set()

    for row in rows:
        dt = row["decision_type"]
        gp = row["game_pk"]

        # Max 3 per decision type
        if type_counts[dt] >= 3:
            continue

        selected.append(row)
        type_counts[dt] += 1
        game_pks.add(gp)

        if len(selected) >= n:
            break

    # Parse JSON fields
    for s in selected:
        s["game_date"] = str(s["game_date"])
        for field in ("engine_recommendation", "context_json", "options_json"):
            val = s.get(field)
            if isinstance(val, str):
                try:
                    s[field] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    pass

    return selected


def record_response(scenario_id: int, session_uuid: str, user_choice: str) -> dict:
    """
    Record a user's answer for a scenario.

    Returns community stats for the scenario after recording.
    """
    sql = """
        INSERT INTO manager_game_responses (scenario_id, session_uuid, user_choice)
        VALUES (%s, %s, %s)
        ON CONFLICT (scenario_id, session_uuid) DO NOTHING
    """
    with psycopg.connect(_db_url(), connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (scenario_id, session_uuid, user_choice))
        conn.commit()

    return get_community_stats(scenario_id)


def get_community_stats(scenario_id: int) -> dict:
    """Get aggregate response stats for a scenario."""
    sql = """
        SELECT user_choice, COUNT(*) as cnt
        FROM manager_game_responses
        WHERE scenario_id = %s
        GROUP BY user_choice
    """
    with psycopg.connect(_db_url(), connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (scenario_id,))
            rows = cur.fetchall()

    counts = {"yes": 0, "no": 0}
    for choice, cnt in rows:
        if choice in counts:
            counts[choice] = cnt

    total = counts["yes"] + counts["no"]
    return {
        "yes_count": counts["yes"],
        "no_count": counts["no"],
        "total": total,
        "yes_pct": round(counts["yes"] / total, 3) if total > 0 else 0.5,
        "no_pct": round(counts["no"] / total, 3) if total > 0 else 0.5,
    }


def get_scenario_result(scenario_id: int) -> dict:
    """
    Get the full result for a scenario: actual decision, engine recommendation,
    and community stats.
    """
    sql = """
        SELECT actual_decision, actual_detail, engine_recommendation
        FROM manager_game_scenarios
        WHERE id = %s
    """
    with psycopg.connect(_db_url(), connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (scenario_id,))
            row = cur.fetchone()

    if not row:
        return {"ok": False, "reason": "scenario_not_found"}

    actual_decision, actual_detail, engine_rec = row

    if isinstance(engine_rec, str):
        try:
            engine_rec = json.loads(engine_rec)
        except (json.JSONDecodeError, TypeError):
            engine_rec = {}

    community = get_community_stats(scenario_id)

    return {
        "actual_decision": actual_decision,
        "actual_detail": actual_detail,
        "engine_recommendation": engine_rec,
        "community": community,
    }
