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


import requests

def get_standings(season_year: int) -> dict:
    url = "https://statsapi.mlb.com/api/v1/standings"
    params = {
        "sportId": 1,                 # MLB
        "leagueId": "103,104",        # AL, NL
        "season": str(season_year),
        "standingsTypes": "regularSeason",
        "hydrate": "team(division,league)"
    }

    r = requests.get(url, params=params, timeout=20)

    # HARD debug to Render logs
    print("STANDINGS DEBUG URL:", r.url)
    print("STANDINGS DEBUG STATUS:", r.status_code)
    print("STANDINGS DEBUG HEAD:", r.text[:200])  # first 200 chars to see if HTML/error

    r.raise_for_status()
    data = r.json()

    # More debug
    print("STANDINGS DEBUG keys:", list(data.keys()) if isinstance(data, dict) else type(data))
    print("STANDINGS DEBUG records len:", len(data.get("records", [])) if isinstance(data, dict) else "n/a")

    return data


# Adding API helper to get list of teams for any given season. This will be used to populate the team directory page
def get_teams(season: int) -> dict:
    """
    Fetch MLB teams for a given season (includes division/league hydration).
    """
    cache_key = f"teams:{season}"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    params = {
        "sportId": 1,  # MLB
        "season": season,
        "hydrate": "division,league",
    }

    r = requests.get(f"{BASE}/teams", params=params, timeout=10)
    r.raise_for_status()
    data = r.json()

    _set_cached(cache_key, data)
    return data


def get_team(team_id: int) -> dict:
    """
    Fetch details for a single team.
    """
    cache_key = f"team:{team_id}"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    params = {
        "hydrate": "division,league,venue",
    }

    r = requests.get(f"{BASE}/teams/{team_id}", params=params, timeout=10)
    r.raise_for_status()
    data = r.json()

    _set_cached(cache_key, data)
    return data


    _set_cached(cache_key, data)
    return data
