import time
from typing import Any, Dict, Optional

import requests

BASE = "https://statsapi.mlb.com/api/v1"

# Simple in-memory cache so we don’t spam the API
_cache: Dict[str, Dict[str, Any]] = {}
CACHE_TTL_SECONDS = 60 * 5  # 5 minutes


def _get_cached(key: str) -> Optional[dict]:
    item = _cache.get(key)
    if not item:
        return None
    if time.time() - item["ts"] > CACHE_TTL_SECONDS:
        return None
    return item["data"]


def _set_cached(key: str, data: dict) -> None:
    _cache[key] = {"ts": time.time(), "data": data}


def get_standings(season: int) -> dict:
    """
    Fetch MLB standings for a season.

    Returns the raw JSON from the MLB Stats API.
    """
    cache_key = f"standings:{season}"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    params = {
        "season": season,
        "standingsTypes": "regularSeason",
        "leagueId": "103,104",  # 103=AL, 104=NL
        "hydrate": "division,league,team(division,league)",
    }

    r = requests.get(f"{BASE}/standings", params=params, timeout=10)
    r.raise_for_status()
    data = r.json()

    _set_cached(cache_key, data)
    return data
