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
    cache_key = f"standings:{season_year}"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    url = f"{BASE}/standings"
    headers = {"User-Agent": "Mozilla/5.0 (Basenerd)"}

    params = {
        "leagueId": "103,104",
        "season": str(season_year),
        "standingsTypes": "regularSeason",
        "hydrate": "team(division,league)",
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict):
            _set_cached(cache_key, data)
            return data
        return {"records": []}
    except Exception as e:
        # Return an empty-but-valid payload so the page renders with an error message upstream
        print(f"[get_standings] failed season={season_year}: {e}")
        return {"records": [], "error": str(e)}



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

def get_team_schedule(team_id: int, season: int) -> dict:
    """
    Fetch schedule for a team + season.
    Includes Spring Training, Regular Season, Postseason.
    Cached.
    """
    cache_key = f"team_schedule:{team_id}:{season}"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    # Schedule endpoint lives under api/v1/schedule
    url = f"{BASE}/schedule"

    # Include Spring + Regular + Postseason (and exhibition just in case)
    # E=Exhibition, S=Spring, R=Regular, F=Wild Card, D=DS, L=LCS, W=WS
    params = {
        "sportId": 1,
        "teamId": team_id,
        "season": season,
        "gameTypes": "E,S,R,F,D,L,W",
        # hydrate gives us probable pitchers & decisions when available
        "hydrate": "probablePitchers,decisions,team"
    }

    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()

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

# services/db.py
import os
from urllib.parse import urlparse

import psycopg

def get_conn():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")

    # Render sometimes provides postgres:// — psycopg expects postgresql://
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    return psycopg.connect(db_url)

def get_standings_from_db(season: int):
    sql = """
      SELECT season, league, division, team_id, team_abbrev, team_name,
             w, l, pct, gb, wc_gb, rs, ra, streak, last_updated
      FROM standings
      WHERE season = %s
      ORDER BY league, division, w DESC, pct DESC, team_name;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (season,))
            cols = [d.name for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return rows

import requests
from collections import defaultdict

from collections import defaultdict
import requests

from collections import defaultdict
import requests

def get_40man_roster_grouped(team_id: int):
    """
    Groups into exactly: Pitcher, Catcher, Infielder, Outfielder.
    Sorts each group by jersey number asc; missing jersey numbers last.
    Pitchers show Pos as RHP/LHP based on throwing hand.
    Also populates bt (bats/throws) and status if available.
    """
    url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster/40Man"

    # Hydrate person so batSide/pitchHand come through reliably
    params = {"hydrate": "person"}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()

    roster = data.get("roster", []) or []

    buckets = ["Pitcher", "Catcher", "Infielder", "Outfielder"]
    grouped = {b: [] for b in buckets}
    other = []

    def jersey_sort_val(j):
        # Missing jerseys go last
        try:
            return (0, int(j))
        except Exception:
            return (1, 9999)

    for item in roster:
        person = item.get("person") or {}
        position = item.get("position") or {}

        # B/T
        bat_obj = person.get("batSide") or {}
        throw_obj = person.get("pitchHand") or {}
        bats = bat_obj.get("code")
        throws = throw_obj.get("code")
        bt = f"{bats}/{throws}" if bats and throws else None

        # Status (try multiple likely locations)
        status = item.get("status") or {}
        status_code = status.get("code") or item.get("statusCode") or item.get("rosterStatus")
        status_desc = status.get("description") or status.get("status") or None

        # Bucket selection
        pos_type = position.get("type")  # Pitcher/Infielder/Outfielder/Catcher/Two-Way
        if pos_type not in buckets:
            # try primary position type if present
            primary = person.get("primaryPosition") or {}
            primary_type = primary.get("type")
            pos_type = primary_type if primary_type in buckets else "Other"

        # Position column
        pos_abbrev = position.get("abbreviation") or position.get("name") or None
        if pos_type == "Pitcher":
            if throws == "R":
                pos_abbrev = "RHP"
            elif throws == "L":
                pos_abbrev = "LHP"
            else:
                pos_abbrev = "P"

        row = {
            "id": person.get("id"),
            "name": person.get("fullName"),
            "jersey": item.get("jerseyNumber"),
            "bt": bt,
            "pos": pos_abbrev,
            "status_code": status_code,
            "status_desc": status_desc,
        }

        if pos_type in grouped:
            grouped[pos_type].append(row)
        else:
            other.append(row)

    # Sort each bucket by jersey number only (missing last). Tie-breaker: name.
    for b in grouped:
        grouped[b].sort(key=lambda x: (jersey_sort_val(x.get("jersey")), (x.get("name") or "")))

    other.sort(key=lambda x: (jersey_sort_val(x.get("jersey")), (x.get("name") or "")))

    return grouped, other



