import time
from typing import Any, Dict, Optional
from datetime import datetime
import requests
import random
import os
import requests

BASE = "https://statsapi.mlb.com/api/v1"

# Simple in-memory cache so we don’t spam the API
_cache: Dict[str, Dict[str, Any]] = {}
CACHE_TTL_SECONDS = 60 * 5  # 5 minutes

# services/mlb_api.py
import json
import os
import random
import requests

_PLAYERS_CACHE = None

def _load_player_pool(path="players_index.json"):
    global _PLAYERS_CACHE
    if _PLAYERS_CACHE is None:
        # Resolve path relative to your project root so it works on Render too
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # /basenerd
        full_path = os.path.join(base_dir, path)
        with open(full_path, "r", encoding="utf-8") as f:
            _PLAYERS_CACHE = json.load(f)  # list[int]
    return _PLAYERS_CACHE

def get_random_player_id(path="players_index.json"):
    pool = _load_player_pool(path)
    return int(random.choice(pool))

def get_player_full(pid: int):
    url = f"https://statsapi.mlb.com/api/v1/people/{pid}"
    params = {"hydrate": "stats(group=[hitting,pitching],type=[yearByYear])"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    people = r.json().get("people", [])
    if not people:
        raise ValueError("No player returned")
    return people[0]

def extract_year_by_year_rows(player: dict):
    rows = []
    stats = player.get("stats", [])

    for grp in stats:
        group_name = (grp.get("group") or {}).get("displayName", "").lower()
        splits = grp.get("splits") or []
        if not splits:
            continue

        kind = "hitting" if "hitting" in group_name else ("pitching" if "pitching" in group_name else None)
        if not kind:
            continue

        for s in splits:
            stat = s.get("stat") or {}
            season = s.get("season")  # "2019"
            team = (s.get("team") or {}).get("name") or ""
            league = (s.get("league") or {}).get("name") or ""
            sport = (s.get("sport") or {}).get("name") or ""

            # Skip non-MLB lines sometimes returned
            if sport and "Major League Baseball" not in sport:
                continue

            base = {
                "kind": kind,
                "year": season,
                "team": team,
                "league": league,
                "stat": stat
            }
            rows.append(base)

    # Sort by year asc
    rows.sort(key=lambda x: (x["kind"], x["year"] or "0", x["team"]))
    return rows


def get_player_headshot_url(pid: int, size=360):
    # MLB headshots usually exist here; some IDs won't have images
    return f"https://img.mlbstatic.com/mlb-photos/image/upload/w_{size},q_100/v1/people/{pid}/headshot/67/current"

def extract_career_statline(player: dict):
    """
    Returns (kind, statdict) where kind is 'hitting', 'pitching', or None.
    """
    stats = player.get("stats", [])
    # stats is a list of groups; each has splits; split[0].stat
    # We'll prefer hitting if present, else pitching.
    hitting = None
    pitching = None

    for grp in stats:
        group = (grp.get("group") or {}).get("displayName", "").lower()
        splits = grp.get("splits") or []
        if not splits:
            continue
        stat = (splits[0].get("stat") or {})
        if "hitting" in group:
            hitting = stat
        elif "pitching" in group:
            pitching = stat

    if hitting:
        return "hitting", hitting
    if pitching:
        return "pitching", pitching
    return None, None

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

import requests

STATSAPI_BASE = "https://statsapi.mlb.com/api/v1"

def get_postseason_series(season: int):
    """
    Returns postseason schedule grouped by series for a given season.
    Uses: /schedule/postseason/series
    """
    url = f"{STATSAPI_BASE}/schedule/postseason/series"
    params = {"season": str(season), "sportId": "1"}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def build_playoff_bracket(series_json: dict):
    """
    Normalizes the StatsAPI response into something easy to render.

    Output shape:
    {
      "AL": { "Wild Card Series": [...], "Division Series": [...], "League Championship Series": [...], "World Series": [...] },
      "NL": { ... },
      "WS": { "World Series": [...] }   # optional convenience
    }
    """
    out = {"AL": {}, "NL": {}, "WS": {}}

    series_list = series_json.get("series", []) or []

    for s in series_list:
        # These keys can vary a bit by season; we keep this defensive.
        # Common fields typically include round/name-like descriptors and teams.
        round_name = (
            s.get("round", {}).get("name")
            or s.get("roundName")
            or s.get("seriesDescription")
            or s.get("name")
            or "Postseason"
        )

        # league label (AL/NL/WS). Sometimes it’s nested, sometimes not present.
        lg = (
            (s.get("league") or {}).get("abbreviation")
            or (s.get("league") or {}).get("name")
            or s.get("leagueName")
            or ""
        )
        lg = "AL" if "American" in lg or lg == "AL" else "NL" if "National" in lg or lg == "NL" else "WS"

        matchup = {
            "seriesNumber": s.get("seriesNumber"),
            "bestOf": s.get("gamesInSeries") or s.get("bestOf") or None,
            "status": (s.get("status") or {}).get("detailedState") or (s.get("status") or {}).get("abstractGameState") or None,
            "teams": [],
            "link": s.get("link"),
        }

        # teams may appear as 'teams' or 'matchupTeams' depending on season
        teams_blob = s.get("teams") or s.get("matchupTeams") or {}

        # attempt to read both sides (home/away OR team1/team2)
        candidates = []
        if isinstance(teams_blob, dict):
            for k in ["home", "away", "team1", "team2"]:
                if teams_blob.get(k):
                    candidates.append(teams_blob.get(k))

        for t in candidates:
            team_obj = t.get("team") or t.get("club") or {}
            team_id = team_obj.get("id")
            abbrev = (team_obj.get("abbreviation") or team_obj.get("abbrev") or "").upper()

            wins = t.get("wins") if t.get("wins") is not None else (t.get("seriesWins") or t.get("score"))
            # some seasons store as 'isWinner' bool
            is_winner = t.get("isWinner")

            matchup["teams"].append({
                "team_id": team_id,
                "abbrev": abbrev,
                "logo_url": f"https://www.mlbstatic.com/team-logos/{team_id}.svg" if team_id else None,
                "wins": wins,
                "is_winner": is_winner,
            })

        out[lg].setdefault(round_name, []).append(matchup)

    return out


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



