# services/mlb_api.py

import time
import json
import os
import random
from datetime import datetime
from typing import Any, Dict, Optional, List, Tuple

import requests

BASE = "https://statsapi.mlb.com/api/v1"
MLB_API_BASE = BASE  # for older helpers that reference MLB_API_BASE

# ----------------------------
# Simple in-memory cache
# ----------------------------
_cache: Dict[str, Dict[str, Any]] = {}
CACHE_TTL_SECONDS = 60 * 5  # 5 minutes


def _get_cached(key: str):
    item = _cache.get(key)
    if not item:
        return None
    if time.time() - item["ts"] > CACHE_TTL_SECONDS:
        return None
    return item["data"]


def _set_cached(key: str, data):
    _cache[key] = {"ts": time.time(), "data": data}


# ----------------------------
# Random player pool
# ----------------------------
_PLAYERS_CACHE = None


def _load_player_pool(path: str = "players_index.json") -> List[int]:
    """
    Loads a list[int] of player IDs from a JSON file.
    Resolved relative to project root so it works on Render.
    """
    global _PLAYERS_CACHE
    if _PLAYERS_CACHE is None:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # project root
        full_path = os.path.join(base_dir, path)
        with open(full_path, "r", encoding="utf-8") as f:
            _PLAYERS_CACHE = json.load(f)
    return _PLAYERS_CACHE


def get_random_player_id(path: str = "players_index.json") -> int:
    pool = _load_player_pool(path)
    return int(random.choice(pool))


# ----------------------------
# Player fetch + stats hydration
# ----------------------------
def get_player_full(pid: int) -> dict:
    """
    Full player hydration used for Random Player:
      /people/{id}?hydrate=stats(group=[hitting,pitching],type=[yearByYear])
    """
    url = f"{BASE}/people/{pid}"
    params = {"hydrate": "stats(group=[hitting,pitching],type=[yearByYear])"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    people = r.json().get("people", []) or []
    if not people:
        raise ValueError("No player returned")
    return people[0]


def get_player_headshot_url(pid: int, size: int = 360) -> str:
    return (
        "https://img.mlbstatic.com/mlb-photos/image/upload/"
        f"w_{size},q_100/v1/people/{pid}/headshot/67/current"
    )


def extract_career_statline(player: dict) -> Tuple[Optional[str], Optional[dict]]:
    """
    Returns (kind, statdict) where kind is 'hitting', 'pitching', or None.
    Uses the hydrated stats in get_player_full.
    """
    stats = player.get("stats", []) or []
    hitting = None
    pitching = None

    for grp in stats:
        group = ((grp.get("group") or {}).get("displayName") or "").lower()
        splits = grp.get("splits") or []
        if not splits:
            continue
        stat = splits[0].get("stat") or {}
        if "hitting" in group:
            hitting = stat
        elif "pitching" in group:
            pitching = stat

    if hitting:
        return "hitting", hitting
    if pitching:
        return "pitching", pitching
    return None, None


# ----------------------------
# Team abbrev helper (used in year-by-year rows)
# ----------------------------
_TEAM_ABBREV_CACHE: Dict[int, str] = {}


def get_team_abbrev(team_id: int) -> str:
    if not team_id:
        return ""
    if team_id in _TEAM_ABBREV_CACHE:
        return _TEAM_ABBREV_CACHE[team_id]

    url = f"{BASE}/teams/{team_id}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json() or {}
    team = (data.get("teams") or [{}])[0]
    abbrev = (team.get("abbreviation") or "").upper()

    _TEAM_ABBREV_CACHE[team_id] = abbrev
    return abbrev


# ----------------------------
# Year-by-year extraction + grouping
# ----------------------------
def extract_year_by_year_rows(player: dict) -> List[dict]:
    """
    Flattens hydrated yearByYear splits into rows like:
      {"kind":"hitting","year":"2019","team":"NYY","stat":{...},"team_id":147}
    Includes both hitting and pitching groups when present.
    Filters out non-MLB sport lines.
    """
    rows: List[dict] = []
    stats = player.get("stats", []) or []

    for grp in stats:
        group_name = ((grp.get("group") or {}).get("displayName") or "").lower()
        splits = grp.get("splits") or []
        if not splits:
            continue

        kind = "hitting" if "hitting" in group_name else ("pitching" if "pitching" in group_name else None)
        if not kind:
            continue

        for s in splits:
            stat = s.get("stat") or {}
            season = s.get("season")  # "2019"
            team_obj = s.get("team") or {}
            team_id = team_obj.get("id")
            team = get_team_abbrev(team_id)
            league = (s.get("league") or {}).get("name") or ""
            sport = (s.get("sport") or {}).get("name") or ""

            # Skip non-MLB lines sometimes returned
            if sport and "Major League Baseball" not in sport:
                continue

            rows.append(
                {
                    "kind": kind,
                    "year": season,
                    "team": team,
                    "league": league,
                    "stat": stat,
                    "team_id": team_id,
                }
            )

    rows.sort(key=lambda x: (x["kind"], x["year"] or "0", x["team"] or ""))
    return rows


def group_year_by_year(rows: List[dict], kind: str) -> List[dict]:
    """
    Groups extract_year_by_year_rows output into:
      [
        {"year":"2025","total": row_or_None, "parts":[team_rows...]},
        ...
      ]
    - If the blank-team total row exists, we call it team='Total'
    - Otherwise single-team year shows as total with no parts
    """
    krows = [r for r in (rows or []) if r.get("kind") == kind]
    by_year: Dict[str, List[dict]] = {}

    for r in krows:
        year = str(r.get("year") or "")
        if not year:
            continue
        by_year.setdefault(year, []).append(r)

    groups: List[dict] = []
    for year, yr_rows in by_year.items():
        total_row = next((x for x in yr_rows if not (x.get("team") or "").strip()), None)
        parts = [x for x in yr_rows if (x.get("team") or "").strip()]

        if total_row:
            total_row = dict(total_row)
            total_row["team"] = "Total"
        else:
            if len(parts) == 1:
                total_row = parts[0]
                parts = []
            elif len(parts) > 1:
                total_row = dict(parts[0])
                total_row["team"] = "Total"

        groups.append({"year": year, "total": total_row, "parts": parts})

    groups.sort(key=lambda g: g["year"])
    return groups


# ----------------------------
# Career totals (true career endpoint)
# ----------------------------
def get_player_career_totals(pid: int, kind: str) -> Optional[dict]:
    """
    Pull true career totals from StatsAPI, rather than summing year-by-year.
    kind: "hitting" or "pitching"
    """
    if kind not in ("hitting", "pitching"):
        return None

    cache_key = f"player_career_totals:{pid}:{kind}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    url = f"{BASE}/people/{pid}/stats"
    params = {"stats": "career", "group": kind}

    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json() or {}
    stats = data.get("stats", []) or []

    out = None
    if stats and (stats[0].get("splits") or []):
        out = stats[0]["splits"][0].get("stat") or None

    _set_cached(cache_key, out)
    return out


# ----------------------------
# Awards
# ----------------------------
def get_player_awards(pid: int) -> List[dict]:
    """
    Raw awards feed: /people/{id}/awards
    """
    cache_key = f"player_awards:{pid}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    url = f"{BASE}/people/{pid}/awards"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json() or {}
    awards = data.get("awards", []) or []

    _set_cached(cache_key, awards)
    return awards


def build_award_year_map(awards: List[dict]) -> Dict[str, List[str]]:
    """
    Returns { "2021": ["mvp","goldglove"], ... }
    Keys match pill keys:
      mvp, cyyoung, roy, goldglove, platinumglove, silverslugger, allstar,
      battingchamp, hrderby, wsmvp, wschamp, hof

    Your rules:
      - WS Champ counted per ring (per season occurrence)
      - All-Star counted as total selections
      - AL/NL MVP treated the same
      - Fielding awards ONLY: Gold/Platinum
      - Add HR Derby Winner
    """
    if not awards:
        return {}

    def clean_name(a):
        return (a.get("name") or "").strip().lower()

    def season_of(a):
        return str(a.get("season") or a.get("year") or "")

    year_map: Dict[str, List[str]] = {}

    for a in awards:
        n = clean_name(a)
        season = season_of(a)
        if not season:
            continue

        key = None

        if n == "hall of fame":
            key = "hof"

        elif n in ("al mvp", "nl mvp"):
            key = "mvp"

        elif n in ("al cy young", "nl cy young"):
            key = "cyyoung"

        # ROY: Jackie Robinson AL/NL and other ROY strings
        elif "rookie of the year" in n:
            key = "roy"

        # Gloves: only gold/platinum
        elif "gold glove" in n:
            key = "platinumglove" if "platinum" in n else "goldglove"

        elif "silver slugger" in n:
            key = "silverslugger"

        elif n in ("al all-star", "nl all-star"):
            key = "allstar"

        elif ("batting champion" in n) or ("batting title" in n) or ("batting" in n and "champ" in n):
            key = "battingchamp"

        elif "home run derby" in n and "winner" in n:
            key = "hrderby"

        elif "world series" in n and "mvp" in n:
            key = "wsmvp"

        elif "world series championship" in n:
            key = "wschamp"

        if key:
            year_map.setdefault(season, []).append(key)

    return year_map


def build_accolade_pills(awards: List[dict]) -> List[dict]:
    """
    Deterministic award normalizer for Random Player page.
    Produces pills with counts (All-Star×N, WS Champ×N, etc.).
    """

    if not awards:
        return []

    def clean_name(a):
        return (a.get("name") or "").strip().lower()

    def season_of(a):
        return str(a.get("season") or a.get("year") or "")

    counts = {
        "mvp": set(),
        "cyyoung": set(),
        "roy": set(),
        "goldglove": set(),
        "platinumglove": set(),
        "silverslugger": set(),
        "allstar": set(),
        "battingchamp": set(),
        "hrderby": set(),
        "wsmvp": set(),
        "wschamp": set(),  # rings counted per season
        "hof": set(),
    }

    for a in awards:
        n = clean_name(a)
        season = season_of(a)

        if n == "hall of fame":
            counts["hof"].add("HOF")
            continue

        if n in ("al mvp", "nl mvp"):
            counts["mvp"].add(season)
            continue

        if n in ("al cy young", "nl cy young"):
            counts["cyyoung"].add(season)
            continue

        if "rookie of the year" in n:
            counts["roy"].add(season)
            continue

        # ONLY fielding awards: gold/platinum gloves
        if "gold glove" in n:
            if "platinum" in n:
                counts["platinumglove"].add(season)
            else:
                counts["goldglove"].add(season)
            continue

        if "silver slugger" in n:
            counts["silverslugger"].add(season)
            continue

        if n in ("al all-star", "nl all-star"):
            counts["allstar"].add(season)
            continue

        if ("batting champion" in n) or ("batting title" in n) or ("batting" in n and "champ" in n):
            counts["battingchamp"].add(season)
            continue

        if "home run derby" in n and "winner" in n:
            counts["hrderby"].add(season)
            continue

        if "world series" in n and "mvp" in n:
            counts["wsmvp"].add(season)
            continue

        if "world series championship" in n:
            counts["wschamp"].add(season)
            continue

    def label(name: str, c: int) -> str:
        return f"{name}×{c}" if c > 1 else name

    pills: List[dict] = []

    if counts["hof"]:
        pills.append({"key": "hof", "label": "HOF"})

    if counts["mvp"]:
        pills.append({"key": "mvp", "label": label("MVP", len(counts["mvp"]))})

    if counts["cyyoung"]:
        pills.append({"key": "cyyoung", "label": label("Cy Young", len(counts["cyyoung"]))})

    if counts["roy"]:
        pills.append({"key": "roy", "label": label("ROY", len(counts["roy"]))})

    if counts["goldglove"]:
        pills.append({"key": "goldglove", "label": label("Gold Glove", len(counts["goldglove"]))})

    if counts["platinumglove"]:
        pills.append({"key": "platinumglove", "label": label("Platinum Glove", len(counts["platinumglove"]))})

    if counts["silverslugger"]:
        pills.append({"key": "silverslugger", "label": label("Silver Slugger", len(counts["silverslugger"]))})

    if counts["battingchamp"]:
        pills.append({"key": "battingchamp", "label": label("Batting Champ", len(counts["battingchamp"]))})

    if counts["allstar"]:
        pills.append({"key": "allstar", "label": label("All-Star", len(counts["allstar"]))})

    if counts["hrderby"]:
        pills.append({"key": "hrderby", "label": label("HR Derby Winner", len(counts["hrderby"]))})

    if counts["wsmvp"]:
        pills.append({"key": "wsmvp", "label": label("WS MVP", len(counts["wsmvp"]))})

    if counts["wschamp"]:
        pills.append({"key": "wschamp", "label": label("WS Champ", len(counts["wschamp"]))})

    # consistent ordering
    order = [
        "hof",
        "mvp",
        "cyyoung",
        "roy",
        "goldglove",
        "platinumglove",
        "silverslugger",
        "battingchamp",
        "allstar",
        "hrderby",
        "wsmvp",
        "wschamp",
    ]
    pills.sort(key=lambda p: order.index(p["key"]) if p["key"] in order else 999)
    return pills


# ----------------------------
# Standings / Teams / Team / Schedule
# ----------------------------
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
        print(f"[get_standings] failed season={season_year}: {e}")
        return {"records": [], "error": str(e)}


def get_teams(season: int) -> dict:
    cache_key = f"teams:{season}"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    params = {"sportId": 1, "season": season, "hydrate": "division,league"}
    r = requests.get(f"{BASE}/teams", params=params, timeout=10)
    r.raise_for_status()
    data = r.json()

    _set_cached(cache_key, data)
    return data


def get_team(team_id: int) -> dict:
    cache_key = f"team:{team_id}"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    params = {"hydrate": "division,league,venue"}
    r = requests.get(f"{BASE}/teams/{team_id}", params=params, timeout=10)
    r.raise_for_status()
    data = r.json()

    _set_cached(cache_key, data)
    return data


def get_team_schedule(team_id: int, season: int) -> dict:
    cache_key = f"team_schedule:{team_id}:{season}"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    url = f"{BASE}/schedule"
    params = {
        "sportId": 1,
        "teamId": team_id,
        "season": season,
        "gameTypes": "E,S,R,F,D,L,W",
        "hydrate": "probablePitchers,decisions,team",
    }

    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()

    _set_cached(cache_key, data)
    return data


# ----------------------------
# Player directory / player page helpers
# ----------------------------
def search_players(query: str) -> List[dict]:
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
    people = resp.json().get("people", []) or []

    _set_cached(cache_key, people)
    return people


def get_player(player_id: int) -> dict:
    cache_key = f"player:{player_id}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    url = f"{MLB_API_BASE}/people/{player_id}"
    resp = requests.get(url, params={"hydrate": "currentTeam"}, timeout=10)
    resp.raise_for_status()
    person = (resp.json() or {}).get("people", [{}])[0]

    _set_cached(cache_key, person)
    return person


def get_player_stats(player_id: int, season: Optional[int] = None) -> List[dict]:
    season = season or datetime.now().year
    cache_key = f"player_stats:{player_id}:{season}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    url = f"{MLB_API_BASE}/people/{player_id}/stats"
    resp = requests.get(
        url,
        params={"stats": "season", "group": "hitting,pitching,fielding", "season": season},
        timeout=10,
    )
    resp.raise_for_status()
    stats = (resp.json() or {}).get("stats", []) or []

    _set_cached(cache_key, stats)
    return stats
