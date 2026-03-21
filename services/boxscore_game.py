"""Career Boxscore Guessing Game service."""

import json
import os
import random
from typing import List, Optional

_DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "boxscore_game_pool.json")
_pool: Optional[List[dict]] = None


def _load_pool() -> List[dict]:
    global _pool
    if _pool is not None:
        return _pool
    if not os.path.exists(_DATA_FILE):
        _pool = []
        return _pool
    with open(_DATA_FILE, "r") as f:
        _pool = json.load(f)
    return _pool


def reload_pool():
    """Force-reload pool from disk."""
    global _pool
    _pool = None
    return _load_pool()


DIFFICULTIES = {
    "rookie": {
        "label": "Rookie",
        "desc": "Everything visible",
        "hide_pos": False,
        "hide_teams": False,
        "hide_years": False,
        "hide_bats": False,
    },
    "veteran": {
        "label": "Veteran",
        "desc": "Position hidden",
        "hide_pos": True,
        "hide_teams": False,
        "hide_years": False,
        "hide_bats": False,
    },
    "allstar": {
        "label": "All-Star",
        "desc": "Position & teams hidden",
        "hide_pos": True,
        "hide_teams": True,
        "hide_years": False,
        "hide_bats": False,
    },
    "mvp": {
        "label": "MVP",
        "desc": "Position, teams & years hidden",
        "hide_pos": True,
        "hide_teams": True,
        "hide_years": True,
        "hide_bats": True,
    },
}

STAT_KEYS = [
    "gamesPlayed", "atBats", "runs", "hits", "doubles", "triples",
    "homeRuns", "rbi", "baseOnBalls", "strikeOuts", "stolenBases",
    "avg", "obp", "slg", "ops",
]


def _pick(stat: dict, key: str):
    v = stat.get(key, "")
    if v == "" or v is None:
        return "-"
    return v


def get_round(difficulty: str = "rookie", exclude_ids: list = None) -> dict:
    pool = _load_pool()
    if len(pool) < 5:
        return {
            "error": "Player pool not found or too small. "
                     "Run: python scripts/generate_boxscore_game_data.py"
        }

    diff = DIFFICULTIES.get(difficulty, DIFFICULTIES["rookie"])
    exclude_set = set(exclude_ids or [])
    available = [p for p in pool if p["id"] not in exclude_set]
    if len(available) < 5:
        available = pool

    chosen = random.sample(available, 5)
    answer_idx = random.randint(0, 4)
    answer = chosen[answer_idx]

    # Build year-by-year display
    years_display = []
    for i, yr in enumerate(answer.get("years", [])):
        stat = yr.get("stat", {})
        years_display.append({
            "year": f"Yr {i + 1}" if diff["hide_years"] else yr.get("year", ""),
            "team": "???" if diff["hide_teams"] else yr.get("team", ""),
            "age": "" if diff["hide_years"] else yr.get("age", ""),
            "stats": {k: _pick(stat, k) for k in STAT_KEYS},
        })

    career_stat = answer.get("career", {})
    career_display = {k: _pick(career_stat, k) for k in STAT_KEYS}

    choices = []
    for p in chosen:
        c = {"id": p["id"], "name": p["name"]}
        if not diff["hide_pos"]:
            c["pos"] = p.get("pos", "")
        choices.append(c)

    headshot = (
        f"https://img.mlbstatic.com/mlb-photos/image/upload/"
        f"w_360,q_100/v1/people/{answer['id']}/headshot/67/current"
    )

    return {
        "answer_id": answer["id"],
        "answer_name": answer["name"],
        "answer_pos": answer.get("pos", ""),
        "answer_bats": "" if diff["hide_bats"] else answer.get("bats", ""),
        "answer_headshot": headshot,
        "choices": choices,
        "years": years_display,
        "career": career_display,
        "difficulty": difficulty,
        "difficulty_label": diff["label"],
        "pool_size": len(pool),
    }
