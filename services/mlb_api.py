import time
from typing import Any, Dict, Optional
from datetime import datetime
import requests

BASE = "https://statsapi.mlb.com/api/v1"

# Simple in-memory cache so we don’t spam the API
_cache: Dict[str, Dict[str, Any]] = {}
CACHE_TTL_SECONDS = 60 * 5  # 5 minutes

def get_player(player_id: int):
    """
    Player bio + current team hydration.
    Cached like teams.
    """
    cache_key = f"player:{player_id}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    url = f"{MLB_API_BASE}/people/{player_id}"
    resp = requests.get(url, params={"hydrate": "currentTeam"}, timeout=10)
    resp.raise_for_status()
    person = resp.json().get("people", [{}])[0]

    _set_cached(cache_key, person)
    return person


def get_player_stats(player_id: int, season: int | None = None):
    """
    Season stats for hitting/pitching/fielding (whatever applies).
    Cached.
    """
    season = season or datetime.now().year
    cache_key = f"player_stats:{player_id}:{season}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    url = f"{MLB_API_BASE}/people/{player_id}/stats"
    resp = requests.get(
        url,
        params={"stats": "season", "group": "hitting,pitching,fielding", "season": season},
        timeout=10
    )
    resp.raise_for_status()
    stats = resp.json().get("stats", [])

    _set_cached(cache_key, stats)
    return stats

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
    url = f"{BASE}/standings"

    # Some MLB endpoints behave better with a real User-Agent.
    headers = {
        "User-Agent": "Mozilla/5.0 (Basenerd; +https://example.com)"
    }

    # Keep params minimal + avoid combinations that sometimes return empty records.
    params = {
        "leagueId": "103,104",            # AL, NL
        "season": str(season_year),
        "standingsTypes": "regularSeason",
        # hydrate is optional; keep it, but if you still get empty, remove this line next
        "hydrate": "team(division,league)",
    }

    r = requests.get(url, params=params, headers=headers, timeout=20)

    print("STANDINGS DEBUG URL:", r.url)
    print("STANDINGS DEBUG STATUS:", r.status_code)
    print("STANDINGS DEBUG HEAD:", r.text[:200])

    r.raise_for_status()
    data = r.json()

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

# --- Players ---

def search_players(query: str):
    """
    Search players by name via StatsAPI.
    Returns a list of people dicts (id, fullName, currentTeam, primaryPosition, etc.)
    Cached briefly because users might search the same names repeatedly.
    """
    q = (query or "").strip()
    if not q:
        return []

    cache_key = f"player_search:{q.lower()}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    url = f"{MLB_API_BASE}/people/search"
    resp = requests.get(url, params={"names": q}, timeout=10)
    resp.raise_for_status()
    people = resp.json().get("people", [])

    _set_cached(cache_key, people)
    return people

import requests
from collections import defaultdict

ROSTER_FIELDS = (
    "roster,jerseyNumber,"
    "position,name,abbreviation,type,"
    "status,code,description,"
    "person,id,fullName,"
    "person,batSide,code,description,"
    "person,pitchHand,code,description,"
    "person,primaryPosition,abbreviation"
)

def get_40man_roster_grouped(team_id: int):
    url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster/40Man"
    r = requests.get(url, params={"fields": ROSTER_FIELDS}, timeout=20)
    r.raise_for_status()
    data = r.json()

    roster = data.get("roster", [])

    # Normalize to a simple list of dicts your template can render
    players = []
    for item in roster:
        person = item.get("person", {}) or {}
        position = item.get("position", {}) or {}
        status = item.get("status", {}) or {}

        bats = (person.get("batSide", {}) or {}).get("code")
        throws = (person.get("pitchHand", {}) or {}).get("code")

        players.append({
            "id": person.get("id"),
            "name": person.get("fullName"),
            "jersey": item.get("jerseyNumber"),
            "pos": position.get("abbreviation") or position.get("name"),
            "pos_type": position.get("type"),  # Pitcher/Infielder/Outfielder/Catcher/Two-Way
            "status_code": status.get("code"),
            "status_desc": status.get("description"),
            "bt": f"{bats}/{throws}" if bats and throws else None,
        })

    # Grouping logic: you can group by pos_type + pos for a nice layout
    # buckets in the order you likely want on the page
    group_order = ["Pitcher", "Catcher", "Infielder", "Outfielder", "Two-Way"]
    grouped = {k: defaultdict(list) for k in group_order}
    other = defaultdict(list)

    for p in players:
        bucket = p["pos_type"] if p["pos_type"] in grouped else "Other"
        target = grouped[bucket] if bucket in grouped else other
        target[p["pos"]].append(p)

    # Sort players inside each position by jersey then name
    def sort_key(x):
        try:
            j = int(x["jersey"]) if x["jersey"] is not None else 999
        except:
            j = 999
        return (j, x["name"] or "")

    for bucket in grouped:
        for pos in grouped[bucket]:
            grouped[bucket][pos].sort(key=sort_key)

    for pos in other:
        other[pos].sort(key=sort_key)

    return grouped, other

