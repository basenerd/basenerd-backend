# services/mlb_api.py

import time
import json
import os
import random
import bisect
from datetime import datetime, date, timedelta
from typing import Any, Dict, Optional, List, Tuple

import requests

BASE = "https://statsapi.mlb.com/api/v1"
MLB_API_BASE = BASE  # for older helpers that reference MLB_API_BASE

# ----------------------------
# Simple in-memory cache
# ----------------------------
_cache: Dict[str, Dict[str, Any]] = {}
CACHE_TTL_SECONDS = 60 * 5  # 5 minutes

def get_player_role(player: dict) -> str:
    """
    Returns: "pitching" or "hitting" (or "two-way")
    Uses primaryPosition.type first (most reliable), falls back to stats.
    """
    pos = (player.get("primaryPosition") or {})
    pos_type = (pos.get("type") or "").lower()
    pos_name = (pos.get("name") or "").lower()

    # Most reliable:
    if pos_type == "pitcher" or pos_name == "pitcher":
        return "pitching"

    # Some players might show as "Two-Way Player"
    if "two-way" in pos_name or "two way" in pos_name:
        return "two-way"

    return "hitting"

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
# League-qualified stat pools (for gradients / percentiles)
# ----------------------------

_LEAGUE_SHORT_TO_ID = {"AL": 103, "NL": 104}

def league_name_to_short(name: str) -> Optional[str]:
    n = (name or "").lower()
    if "american" in n:
        return "AL"
    if "national" in n:
        return "NL"
    return None

def to_float(x) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        try:
            return float(x)
        except Exception:
            return None
    s = str(x).strip()
    if not s or s == "—" or s == "-":
        return None
    try:
        return float(s)
    except Exception:
        return None

def pct_to_bg(p: float) -> str:
    """Matches the standings pct_badge feel: red good / blue bad around 0.5."""
    try:
        p = float(p)
    except Exception:
        p = 0.5
    if p < 0:
        p = 0.0
    if p > 1:
        p = 1.0

    if p >= 0.5:
        a = (p - 0.5) / 0.5
        if a > 1:
            a = 1.0
        alpha = 0.10 + 0.35 * a
        return f"rgba(255, 70, 70, {alpha:.3f})"
    else:
        a = (0.5 - p) / 0.5
        if a > 1:
            a = 1.0
        alpha = 0.10 + 0.35 * a
        return f"rgba(70, 140, 255, {alpha:.3f})"

def percentile_from_sorted(sorted_vals: List[float], v: float) -> float:
    """Percentile rank in [0,1] using mid-rank for ties."""
    if not sorted_vals:
        return 0.5
    if len(sorted_vals) == 1:
        return 0.5
    left = bisect.bisect_left(sorted_vals, v)
    right = bisect.bisect_right(sorted_vals, v)
    mid = (left + right - 1) / 2.0
    return mid / (len(sorted_vals) - 1)

def get_qualified_league_player_stats(season: int, kind: str, league_short: str) -> Dict[int, dict]:
    """
    Returns {player_id: stat_dict} for QUALIFIED players in the given league+season.

    kind: "hitting" or "pitching"
    league_short: "AL" or "NL"
    """
    league_id = _LEAGUE_SHORT_TO_ID.get((league_short or "").upper())
    if kind not in ("hitting", "pitching") or not league_id:
        return {}

    key = f"qualpool:{season}:{kind}:{league_id}"
    cached = _get_cached(key)
    if cached is not None:
        return cached

    url = f"{BASE}/stats"
    params = {
        "stats": "season",
        "group": kind,
        "season": season,
        "gameType": "R",
        "sportIds": 1,
        "playerPool": "QUALIFIED",
        "limit": 10000,
        "leagueId": league_id,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    stats = (data.get("stats") or [])
    splits = (stats[0].get("splits") if stats else []) or []

    out: Dict[int, dict] = {}
    for sp in splits:
        pid = ((sp.get("player") or {}).get("id"))
        if not pid:
            continue
        stat = sp.get("stat") or {}
        try:
            out[int(pid)] = stat
        except Exception:
            continue

    _set_cached(key, out)
    return out

def build_stat_distributions(season: int, kind: str, league_short: str) -> Dict[str, List[float]]:
    """
    Builds {stat_key: sorted list of float values} from the qualified pool.
    Cached via the underlying qualified pool fetch.
    """
    pool = get_qualified_league_player_stats(season, kind, league_short)
    dists: Dict[str, List[float]] = {}

    for stat in pool.values():
        if not isinstance(stat, dict):
            continue
        for k, raw in stat.items():
            v = to_float(raw)
            if v is None:
                continue
            dists.setdefault(k, []).append(v)

    for k in list(dists.keys()):
        dists[k].sort()
    return dists


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

        # Keep API split order within each season (stable sort).
    rows.sort(key=lambda x: (x["kind"], int(x["year"] or 0)))
    return rows


def group_year_by_year(rows: List[dict], kind: str) -> List[dict]:
    """
    Groups extract_year_by_year_rows output into:
      [
        {"year":"2025","total": row_or_None, "parts":[team_rows...]},
        ...
      ]
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

def get_player_game_log(player_id: int, season: int) -> List[dict]:
    """
    Raw StatsAPI gamelog blocks.
    """
    url = f"{MLB_API_BASE}/people/{player_id}/stats"
    resp = requests.get(
        url,
        params={"stats": "gameLog", "group": "hitting,pitching", "season": season},
        timeout=15,
    )
    resp.raise_for_status()
    return (resp.json() or {}).get("stats", []) or []

def extract_game_log_rows(game_log_blocks: List[dict]) -> Dict[str, List[dict]]:
    """
    Returns:
      {
        "hitting": [{"date":"2025-04-02","opponent":"@ LAD","stat":{...}}, ...],
        "pitching": [...]
      }
    Always forces opponent abbreviations using team ID.
    """
    out = {"hitting": [], "pitching": []}

    for block in game_log_blocks or []:
        gname = ((block.get("group") or {}).get("displayName") or "").lower()
        kind = "pitching" if "pitching" in gname else ("hitting" if "hitting" in gname else None)
        if not kind:
            continue

        for s in block.get("splits") or []:
            stat = s.get("stat") or {}

            # Date
            d = (s.get("date") or s.get("gameDate") or "")[:10] or ""

            # Opponent abbreviation
            opp = ""
            opponent = s.get("opponent") or {}
            opp_id = opponent.get("id")

            if opp_id:
                try:
                    # reuse your existing helper
                    from services.mlb_api import get_team_abbrev
                    opp_abbrev = get_team_abbrev(opp_id)
                except Exception:
                    opp_abbrev = opponent.get("abbreviation") or opponent.get("name") or ""

                is_home = s.get("isHome")
                prefix = "vs" if is_home else "@"
                opp = f"{prefix} {opp_abbrev}"
            else:
                opp = ""

            out[kind].append({
                "date": d,
                "opponent": opp,
                "stat": stat
            })

    # newest first
    out["hitting"].sort(key=lambda r: r.get("date") or "", reverse=True)
    out["pitching"].sort(key=lambda r: r.get("date") or "", reverse=True)

    return out


def build_award_year_map(awards: List[dict]) -> Dict[str, List[str]]:
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
        elif "rookie of the year" in n and "jackie robinson" in n:
            key = "roy"
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
        "wschamp": set(),
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

        if "rookie of the year" in n and "jackie robinson" in n:
            counts["roy"].add(season)
            continue

        # ONLY fielding awards: gold/platinum gloves
        if "al gold glove" in n or "nl gold glove" in n:
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

        if ("nl batting champion" in n) or ("al batting champion" in n):
            counts["battingchamp"].add(season)
            continue

        if n == "home run derby winner":
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

    order = [
        "hof", "mvp", "cyyoung", "roy",
        "goldglove", "platinumglove",
        "silverslugger", "battingchamp",
        "allstar", "hrderby",
        "wsmvp", "wschamp",
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

def league_name_to_short(league_name):
    """
    Convert StatsAPI league names to short codes.
    """
    if not league_name:
        return None
    name = league_name.lower()
    if "american" in name:
        return "AL"
    if "national" in name:
        return "NL"
    return None

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
# 40-man roster (you need this)
# ----------------------------
def get_40man_roster_grouped(team_id: int):
    """
    Groups roster into exactly: Pitcher, Catcher, Infielder, Outfielder.
    Sorts each group by jersey number asc; missing jersey numbers last.
    Pitchers show Pos as RHP/LHP based on throwing hand.
    Also populates bt (bats/throws) and status if available.
    """
    url = f"{BASE}/teams/{team_id}/roster/40Man"
    params = {"hydrate": "person"}  # ensure batSide/pitchHand show up
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json() or {}

    roster = data.get("roster", []) or []

    buckets = ["Pitcher", "Catcher", "Infielder", "Outfielder"]
    grouped = {b: [] for b in buckets}
    other = []

    def jersey_sort_val(j):
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

        # Status
        status = item.get("status") or {}
        status_code = status.get("code") or item.get("statusCode") or item.get("rosterStatus")
        status_desc = status.get("description") or status.get("status") or None

        # Bucket selection
        pos_type = position.get("type")
        if pos_type not in buckets:
            primary = person.get("primaryPosition") or {}
            primary_type = primary.get("type")
            pos_type = primary_type if primary_type in buckets else "Other"

        # Position abbreviation
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

    for b in grouped:
        grouped[b].sort(key=lambda x: (jersey_sort_val(x.get("jersey")), (x.get("name") or "")))
    other.sort(key=lambda x: (jersey_sort_val(x.get("jersey")), (x.get("name") or "")))

    return grouped, other

# ----------------------------
# Transactions (team)
# ----------------------------
def get_team_transactions(team_id: int, start_date: str, end_date: str) -> dict:
    """
    Fetch MLB transactions for a team within a date range.
    Dates must be YYYY-MM-DD.

    Endpoint: /transactions?teamId=...&startDate=...&endDate=...
    """
    cache_key = f"team_tx:{team_id}:{start_date}:{end_date}"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    url = f"{BASE}/transactions"
    params = {
        "sportId": 1,
        "teamId": team_id,
        "startDate": start_date,
        "endDate": end_date,
        # hydrate can help ensure person/team objects are present when available
        "hydrate": "person,fromTeam,toTeam",
    }

    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json() or {}

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
    people = (resp.json() or {}).get("people", []) or []

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
    

# --- ADD helper: compute age ---
def _calc_age(birth_date_str: Optional[str]) -> Optional[int]:
    if not birth_date_str:
        return None
    try:
        b = datetime.strptime(birth_date_str[:10], "%Y-%m-%d").date()
        today = date.today()
        return today.year - b.year - ((today.month, today.day) < (b.month, b.day))
    except Exception:
        return None


# --- ADD: uncached season stats fetch (live each request) ---
def get_player_season_stats_live(
    player_id: int,
    season: int,
    stat_type: str = "season",  # "season" or "seasonAdvanced"
    groups: str = "hitting,pitching,fielding",
) -> List[dict]:
    """
    Live pull every request (NO cache) per your requirement.
    Returns the raw 'stats' array from /people/{id}/stats.
    """
    url = f"{MLB_API_BASE}/people/{player_id}/stats"
    resp = requests.get(
        url,
        params={"stats": stat_type, "group": groups, "season": season},
        timeout=10,
    )
    resp.raise_for_status()
    return (resp.json() or {}).get("stats", []) or []


# --- ADD: game logs ---
def get_player_game_log(player_id: int, season: int, groups: str = "hitting,pitching") -> List[dict]:
    """
    /people/{id}/stats?stats=gameLog&group=hitting,pitching&season=YYYY
    """
    url = f"{MLB_API_BASE}/people/{player_id}/stats"
    resp = requests.get(
        url,
        params={"stats": "gameLog", "group": groups, "season": season},
        timeout=15,
    )
    resp.raise_for_status()
    return (resp.json() or {}).get("stats", []) or []


# --- ADD: player transactions ---
def get_player_transactions(player_id: int) -> List[dict]:
    """
    Uses /transactions with playerId.
    Returns newest-first.
    """
    # cache is OK here, but you said LIVE only for season stats.
    # We'll keep a short cache to avoid hammering transactions.
    cache_key = f"player_transactions:{player_id}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    url = f"{BASE}/transactions"
    resp = requests.get(
        url,
        params={"sportId": 1, "playerId": player_id, "hydrate": "person,fromTeam,toTeam"},
        timeout=20,
    )
    resp.raise_for_status()
    txs = (resp.json() or {}).get("transactions", []) or []

    def pick_date(t: dict) -> str:
        # try a few common fields
        for k in ("date", "transactionDate", "effectiveDate"):
            v = t.get(k)
            if v:
                return str(v)[:10]
        return ""

    out = []
    for t in txs:
        person = t.get("person") or {}
        desc = (t.get("description") or t.get("note") or t.get("notes") or "").strip()
        out.append(
            {
                "date": pick_date(t),
                "description": desc or "—",
                "player": {
                    "id": person.get("id"),
                    "name": (person.get("fullName") or "").strip(),
                },
                # optional extras if you want later:
                "type": (t.get("type") or {}).get("name"),
                "fromTeam": (t.get("fromTeam") or {}).get("name"),
                "toTeam": (t.get("toTeam") or {}).get("name"),
            }
        )

    out.sort(key=lambda x: (x.get("date") or ""), reverse=True)
    _set_cached(cache_key, out)
    return out


# --- ADD: find best season with stats (current -> back to debut year) ---
def find_best_season_with_stats(
    player_id: int,
    debut_year: Optional[int],
    role: str,
    start_year: int,
    stat_type: str = "season",
) -> Tuple[Optional[int], List[dict]]:
    """
    Walk backward from start_year until we find a season that has splits.
    Stops at debut_year (inclusive).
    role: "hitting" | "pitching" | "two-way"
    """
    if not debut_year:
        debut_year = 1900

    for yr in range(start_year, debut_year - 1, -1):
        try:
            blocks = get_player_season_stats_live(
                player_id,
                season=yr,
                stat_type=stat_type,
                groups="hitting,pitching,fielding",
            )
        except Exception:
            blocks = []

        def _has_group(group_display: str) -> bool:
            for b in blocks:
                g = ((b.get("group") or {}).get("displayName") or "").lower()
                if group_display in g:
                    splits = b.get("splits") or []
                    if splits and splits[0].get("stat"):
                        return True
            return False

        if role == "two-way":
            if _has_group("hitting") or _has_group("pitching"):
                return yr, blocks
        elif role == "pitching":
            if _has_group("pitching"):
                return yr, blocks
        else:
            if _has_group("hitting"):
                return yr, blocks

    return None, []


# --- OPTIONAL: enrich bio in one place (used by player page) ---
def build_player_header(bio: dict) -> dict:
    """
    Normalizes header fields + hides missing later in template.
    """
    debut = (bio.get("mlbDebutDate") or "")[:10] or None
    debut_year = None
    if debut:
        try:
            debut_year = int(debut[:4])
        except Exception:
            debut_year = None

    return {
        "id": bio.get("id"),
        "fullName": bio.get("fullName"),
        "currentTeam": (bio.get("currentTeam") or {}).get("name"),
        "currentTeamId": (bio.get("currentTeam") or {}).get("id"),
        "primaryPosition": (bio.get("primaryPosition") or {}).get("name"),
        "batSide": (bio.get("batSide") or {}).get("code") or (bio.get("batSide") or {}).get("description"),
        "pitchHand": (bio.get("pitchHand") or {}).get("code") or (bio.get("pitchHand") or {}).get("description"),
        "height": bio.get("height"),
        "weight": bio.get("weight"),
        "birthDate": (bio.get("birthDate") or "")[:10] or None,
        "age": _calc_age(bio.get("birthDate")),
        "birthCity": bio.get("birthCity"),
        "birthStateProvince": bio.get("birthStateProvince"),
        "birthCountry": bio.get("birthCountry"),
        "mlbDebutDate": debut,
        "debutYear": debut_year,
        # these exist for many players; template will hide if missing:
        "draftYear": bio.get("draftYear"),
        "draftPick": bio.get("draftPick"),
        "draftRound": bio.get("draftRound"),
        "education": bio.get("education"),
    }

# ----------------------------
# Games / Schedule helpers
# ----------------------------

from zoneinfo import ZoneInfo

def _safe_int(x, default=None):
    try:
        return int(x)
    except Exception:
        return default

def _to_user_tz(dt_str: str, tz_name: str) -> datetime:
    """
    StatsAPI schedule gameDate is ISO like '2026-03-01T20:10:00Z'
    Convert to user tz.
    """
    if not dt_str:
        return None
    s = dt_str.strip()
    # Normalize Z -> +00:00
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        return None
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("America/Phoenix")
    return dt.astimezone(tz)

def get_schedule_for_dates(start_date_ymd: str, end_date_ymd: str) -> dict:
    """
    Fetch MLB schedule for a date range (inclusive).
    Dates: YYYY-MM-DD
    """
    cache_key = f"schedule:{start_date_ymd}:{end_date_ymd}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    url = f"{BASE}/schedule"
    params = {
        "sportId": 1,
        "startDate": start_date_ymd,
        "endDate": end_date_ymd,
        # linescore gives inning info; probables/decisions are nice-to-have
        "hydrate": "team,linescore,probablePitchers,decisions,venue,seriesStatus",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json() or {}

    _set_cached(cache_key, data)
    return data

def find_next_date_with_games(start_date_ymd: str, max_days_ahead: int = 120) -> Optional[str]:
    """
    Fast: ONE schedule call from start_date -> start_date+max_days_ahead, then scan.
    Avoids long multi-request loops that can cause Render 502 timeouts.
    """
    try:
        start_dt = datetime.strptime(start_date_ymd, "%Y-%m-%d").date()
    except Exception:
        start_dt = datetime.utcnow().date()

    end_dt = start_dt + timedelta(days=max_days_ahead)

    try:
        data = get_schedule_for_dates(start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"))
    except Exception:
        return None

    for d in (data.get("dates") or []):
        games = d.get("games") or []
        if games:
            return d.get("date")  # YYYY-MM-DD

    return None


def _status_pill_text(game: dict) -> str:
    """
    Returns a short status string:
      - Live: 'Top 5' / 'Bot 9'
      - Final: 'Final'
      - Scheduled: 'Scheduled'
      - Other: detailedState
    """
    status = (game.get("status") or {})
    detailed = (status.get("detailedState") or "").strip()
    abstract = (status.get("abstractGameState") or "").strip()  # Live/Final/Preview/etc.

    # Live inning
    if abstract.lower() == "live":
        ls = game.get("linescore") or {}
        inning = _safe_int(ls.get("currentInning"))
        is_top = ls.get("isTopInning")
        if inning:
            return f"{'Top' if is_top else 'Bot'} {inning}"
        # fallback
        return "Live"

    # Final
    if abstract.lower() == "final" or "final" in detailed.lower():
        return "Final"

    # Preview / Scheduled
    if abstract.lower() in ("preview", "pre-game") or "scheduled" in detailed.lower():
        return "Scheduled"

    return detailed or "—"

def _series_short_label(series_desc: str) -> str:
    """
    Convert seriesDescription into a short label like NLDS/ALDS/NLCS/ALCS/WS/WC.
    Falls back to series_desc if unknown.
    """
    s = (series_desc or "").lower()

    if "world series" in s:
        return "WS"
    if "wild card" in s:
        return "WC"

    # League + round
    is_al = "american league" in s or s.startswith("al ")
    is_nl = "national league" in s or s.startswith("nl ")

    if "division series" in s:
        return ("ALDS" if is_al else "NLDS" if is_nl else "DS")
    if "championship series" in s:
        return ("ALCS" if is_al else "NLCS" if is_nl else "CS")

    # Other postseason rounds or unknown:
    # (keep it readable but short)
    return (series_desc or "").strip()

def _record_str(team_wrap: dict) -> str:
    """
    team_wrap is teams.home or teams.away object from schedule game.
    leagueRecord is typically present for MLB schedule responses.
    """
    lr = (team_wrap or {}).get("leagueRecord") or {}
    w = lr.get("wins")
    l = lr.get("losses")
    if w is None or l is None:
        return ""
    return f"{w}-{l}"

def _build_series_line(game: dict, home_abbrev: str, away_abbrev: str) -> str:
    """
    Returns:
      - "LAD leads 2-1"
      - "LAD wins 3-1"
      - "Series tied 1-1"
    Uses seriesStatus when available (multiple schema variants).
    """
    ss = (game.get("seriesStatus") or {})

    # Many schedule responses already include a nice human-ready string:
    # e.g., ss.shortDescription == "LAD leads 2-1" / "LAD wins 3-1"
    short = (ss.get("shortDescription") or ss.get("result") or "").strip()
    if short:
        return short

    # Fallback to numeric wins schema (when present)
    home_w = ss.get("homeWins")
    away_w = ss.get("awayWins")
    if home_w is None or away_w is None:
        return ""

    is_over = bool(ss.get("isOver"))

    # If series is over, try winner
    if is_over:
        winning_team = ss.get("winningTeam") or {}
        winning_id = winning_team.get("id")

        teams = game.get("teams") or {}
        home_team = (teams.get("home") or {}).get("team") or {}
        away_team = (teams.get("away") or {}).get("team") or {}
        home_id = home_team.get("id")
        away_id = away_team.get("id")

        winner_abbrev = None
        if winning_id and winning_id == home_id:
            winner_abbrev = home_abbrev
        elif winning_id and winning_id == away_id:
            winner_abbrev = away_abbrev
        else:
            winner_abbrev = home_abbrev if home_w > away_w else away_abbrev if away_w > home_w else None

        if winner_abbrev:
            return f"{winner_abbrev} wins {max(home_w, away_w)}-{min(home_w, away_w)}"
        return f"Series decided {max(home_w, away_w)}-{min(home_w, away_w)}"

    # Not over
    if home_w == away_w:
        return f"Series tied {home_w}-{away_w}"
    lead_abbrev = home_abbrev if home_w > away_w else away_abbrev
    return f"{lead_abbrev} leads {max(home_w, away_w)}-{min(home_w, away_w)}"


def get_games_for_date(date_ymd: str, tz_name: str = "America/Phoenix") -> List[dict]:
    """
    Returns a list of normalized game cards for games.html.
    Adds:
      - competitionLabel: "Spring Training" or "NLDS Game 4" etc.
      - subline: regular season records OR postseason series score line
    """
    data = get_schedule_for_dates(date_ymd, date_ymd)
    out = []

    for d in (data.get("dates") or []):
        for g in (d.get("games") or []):
            teams = g.get("teams") or {}
            home = teams.get("home") or {}
            away = teams.get("away") or {}
            home_team = home.get("team") or {}
            away_team = away.get("team") or {}

            home_id = home_team.get("id")
            away_id = away_team.get("id")

            game_dt_local = _to_user_tz(g.get("gameDate") or "", tz_name)
            time_local = game_dt_local.strftime("%-I:%M %p") if game_dt_local else ""

            # status pill (includes inning for live)
            pill = _status_pill_text(g)

            # scores (may be None)
            home_score = home.get("score")
            away_score = away.get("score")

            # probables / decisions
            probables = g.get("probablePitchers") or {}
            decisions = g.get("decisions") or {}
            season = _season_from_date(date_ymd)
            home_pp_obj = (probables.get("home") or {})
            away_pp_obj = (probables.get("away") or {})
            home_pp = _pp_text(home_pp_obj, season)
            away_pp = _pp_text(away_pp_obj, season)
            home_rec = _record_str(home)
            away_rec = _record_str(away)
            win_p = (decisions.get("winner") or {}).get("fullName")
            lose_p = (decisions.get("loser") or {}).get("fullName")
            venue = g.get("venue") or {}
            venue_name = venue.get("name")        
            venue_loc = ""
            city = venue.get("city")
            state = venue.get("state") or venue.get("stateAbbrev")
            if city and state:
                venue_loc = f"{city}, {state}"
            elif city:
                venue_loc = city


            home_abbrev = (home_team.get("abbreviation") or "").upper()
            away_abbrev = (away_team.get("abbreviation") or "").upper()

            # ---- Competition label + subline (records/series) ----
            game_type = (g.get("gameType") or "").upper()  # R/S/P etc.
            series_desc = (g.get("seriesDescription") or "").strip()
            series_game_num = g.get("seriesGameNumber")
            is_spring = (game_type == "S")
            is_post = (game_type == "P") or ("series" in series_desc.lower()) or ("world series" in series_desc.lower()) or ("wild card" in series_desc.lower())

            competition_label = ""
            subline = ""

            if is_spring:
                competition_label = "Spring Training"
                # spring training: show records if available (often 0-0 early)
                hr = _record_str(home)
                ar = _record_str(away)
                if hr and ar:
                    subline = f"{away_abbrev} {ar} • {home_abbrev} {hr}"

            elif is_post:
                short = _series_short_label(series_desc) or "Postseason"
                if series_game_num:
                    competition_label = f"{short} Game {series_game_num}"
                else:
                    competition_label = short

                # postseason: series score line
                subline = _build_series_line(g, home_abbrev=home_abbrev, away_abbrev=away_abbrev)

            else:
                # regular season
                hr = _record_str(home)
                ar = _record_str(away)
                if hr and ar:
                    subline = f"{away_abbrev} {ar} • {home_abbrev} {hr}"

            out.append({
                "gamePk": g.get("gamePk"),
                "date": date_ymd,
                "timeLocal": time_local,
                "statusPill": pill,
                "detailedState": ((g.get("status") or {}).get("detailedState") or ""),
                "competitionLabel": competition_label,
                "subline": subline,
                "isPostseason": is_post,
                "isSpring": is_spring,
                "home": {
                    "id": home_id,
                    "name": home_team.get("name"),
                    "abbrev": home_abbrev,
                    "record": home_rec,
                    "pp": home_pp,
                    "logo": f"https://www.mlbstatic.com/team-logos/{home_id}.svg" if home_id else None,
                    "score": home_score,
                },
                "away": {
                    "id": away_id,
                    "name": away_team.get("name"),
                    "abbrev": away_abbrev,
                    "record": away_rec,
                    "pp": away_pp,
                    "logo": f"https://www.mlbstatic.com/team-logos/{away_id}.svg" if away_id else None,
                    "score": away_score,
                },
                "pitching": {
                    "winner": win_p,
                    "loser": lose_p,
                },
                "venue": venue_name,
                "venueLocation": venue_loc,
            })

    # Sort by local time when possible
    def _sort_key(x):
        return x.get("timeLocal") or ""
    out.sort(key=_sort_key)

    return out


    # Sort by local time when possible; fallback to gamePk
    def _sort_key(x):
        try:
            # reconstruct a comparable time using the displayed time (ok for same-day sorting)
            return x.get("timeLocal") or ""
        except Exception:
            return ""
    out.sort(key=_sort_key)

    return out

def get_game_feed(game_pk: int) -> dict:
    """
    Try live feed. If not available (404 for future games) OR any network/API error,
    fall back to schedule lookup.
    """
    cache_key = f"gamefeed:{game_pk}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    url = f"{BASE}/game/{game_pk}/feed/live"

    try:
        r = requests.get(url, timeout=30)

        # If MLB returns non-JSON HTML sometimes (rare), guard it
        if r.status_code == 200:
            data = r.json() or {}
            _set_cached(cache_key, data)
            return data

        # If not 200, treat as fallback case
        if r.status_code == 404:
            sched_game = get_schedule_game_by_pk(game_pk)
            data = {"scheduleOnly": True, "scheduleGame": sched_game}
            _set_cached(cache_key, data)
            return data

        # Any other status: fallback
        sched_game = get_schedule_game_by_pk(game_pk)
        data = {"scheduleOnly": True, "scheduleGame": sched_game, "_fallback_reason": f"feed_status={r.status_code}"}
        _set_cached(cache_key, data)
        return data

    except Exception as e:
        # Timeout / connection / JSON decode / etc -> fallback
        sched_game = None
        try:
            sched_game = get_schedule_game_by_pk(game_pk)
        except Exception:
            pass

        data = {"scheduleOnly": True, "scheduleGame": sched_game, "_fallback_reason": f"exception={type(e).__name__}"}
        _set_cached(cache_key, data)
        return data

def normalize_schedule_game(g: dict, tz_name: str = "America/Phoenix") -> dict:
    def _to_user_tz_iso(iso_str: str) -> str:
        if not iso_str:
            return ""
        try:
            import pytz
            dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
            tz = pytz.timezone(tz_name)
            return dt.astimezone(tz).strftime("%Y-%m-%d %I:%M %p %Z")
        except Exception:
            return iso_str

    teams = g.get("teams") or {}
    home_wrap = teams.get("home") or {}
    away_wrap = teams.get("away") or {}
    home = (home_wrap.get("team") or {})
    away = (away_wrap.get("team") or {})

    status = g.get("status") or {}
    venue = (g.get("venue") or {}).get("name") or ""

    date_ymd = (g.get("gameDate") or "")[:10]
    season = _season_from_date(date_ymd)

    prob = g.get("probablePitchers") or {}
    home_pp = _pp_text(prob.get("home") or {}, season) if prob else None
    away_pp = _pp_text(prob.get("away") or {}, season) if prob else None

    home_id = home.get("id")
    away_id = away.get("id")

    return {
        "gamePk": g.get("gamePk"),
        "when": _to_user_tz_iso(g.get("gameDate") or ""),
        "statusPill": status.get("detailedState") or "Scheduled",
        "detailedState": status.get("detailedState") or "",
        "venue": venue,
        "weather": {"condition": None, "temp": None, "wind": None},
        "probables": {"home": home_pp, "away": away_pp},
        "decisions": {"winner": None, "loser": None, "save": None},
        "home": {
            "id": home_id,
            "name": home.get("name") or "",
            "abbrev": (home.get("abbreviation") or "").upper(),
            "logo": f"https://www.mlbstatic.com/team-logos/{home_id}.svg" if home_id else None,
            "record": None,
            "score": (home_wrap.get("score") if isinstance(home_wrap, dict) else None),
        },
        "away": {
            "id": away_id,
            "name": away.get("name") or "",
            "abbrev": (away.get("abbreviation") or "").upper(),
            "logo": f"https://www.mlbstatic.com/team-logos/{away_id}.svg" if away_id else None,
            "record": None,
            "score": (away_wrap.get("score") if isinstance(away_wrap, dict) else None),
        },
        "linescore": {},
        "box": None,
        "scoring": None,
        "pas": None,
        "pbp": None,
    }

def normalize_game_detail(feed: dict, tz_name: str = "America/Phoenix") -> dict:
    """
    Normalizes /game/{gamePk}/feed/live for game.html.
    Must work across statuses:
      - Scheduled / Pre-game (may lack liveData pieces)
      - Live / Final (has linescore/boxscore/plays)
    """ 
        # If the feed is a schedule-only fallback, normalize from schedule and return.
    if (feed or {}).get("scheduleOnly"):
        sg = (feed or {}).get("scheduleGame") or {}
        if sg:
            base = normalize_schedule_game(sg, tz_name=tz_name)
            # Ensure fields your template checks exist, even if empty
            base.setdefault("linescore", None)
            base.setdefault("box", None)
            base.setdefault("scoring", [])
            base.setdefault("pas", [])
            base.setdefault("pbp", [])
            return base
    def _safe(obj, *keys, default=None):
        cur = obj
        for k in keys:
            if not isinstance(cur, dict):
                return default
            cur = cur.get(k)
        return cur if cur is not None else default
    def _to_user_tz_iso(iso_str: str) -> str:
        if not iso_str:
            return ""
        # You already have a similar helper elsewhere; keeping this defensive
        try:
            from datetime import datetime
            import pytz
            # Example input: "2025-04-01T01:10:00Z"
            dt_utc = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
            tz = pytz.timezone(tz_name or "America/Phoenix")
            return dt_utc.astimezone(tz).strftime("%Y-%m-%d %I:%M %p").lstrip("0")
        except Exception:
            return iso_str

    # If feed is missing or malformed, return empty-ish structure
    gd = (feed or {}).get("gameData") or {}
    ld = (feed or {}).get("liveData") or {}
    status = gd.get("status") or {}
    teams = gd.get("teams") or {}
    venue_obj = gd.get("venue") or {}
    weather_obj = gd.get("weather") or {}
    prob = gd.get("probablePitchers") or {}

    # Helper fields
    game_date = gd.get("datetime") or {}
    iso = game_date.get("dateTime") or ""
    when = _to_user_tz_iso(iso)

    detailed = status.get("detailedState") or ""
    abstract = status.get("abstractGameState") or status.get("abstractGameCode") or ""

    # A little "pill" string for header
    if detailed:
        pill = detailed
    elif abstract:
        pill = str(abstract)
    else:
        pill = ""

    # Venue text
    venue = venue_obj.get("name") or ""

    # Weather
    weather = None
    if weather_obj:
        temp = weather_obj.get("temp")
        cond = weather_obj.get("condition")
        wind = weather_obj.get("wind")
        parts = []
        if temp:
            parts.append(str(temp))
        if cond:
            parts.append(str(cond))
        if wind:
            parts.append(str(wind))
        if parts:
            weather = " • ".join(parts)

    # Teams
    away = teams.get("away") or {}
    home = teams.get("home") or {}
    away_id = _safe(away, "id", default=None)
    home_id = _safe(home, "id", default=None)

    # Scores (safe across states)
    linescore = (ld.get("linescore") or {})
    away_runs = _safe(linescore, "teams", "away", "runs", default=None)
    home_runs = _safe(linescore, "teams", "home", "runs", default=None)

    # Probables
    probables = None
    if prob:
        ap = prob.get("away") or {}
        hp = prob.get("home") or {}
        app = ap.get("fullName") or ""
        hpp = hp.get("fullName") or ""
        probables = {
            "home": f"PP: {hpp}" if hpp else None,
            "away": f"PP: {app}" if app else None,
        }

    # Decisions
    dec = gd.get("decisions") or {}
    decisions = {}
    if dec:
        decisions = {
            "winner": (dec.get("winner") or {}).get("fullName"),
            "loser": (dec.get("loser") or {}).get("fullName"),
            "save": (dec.get("save") or {}).get("fullName"),
        }

    out = {
        # ✅ FIX: gamePk is top-level in the live feed (feed["gamePk"]), not under gameData
        "gamePk": _safe(feed, "gamePk", default=None) or _safe(gd, "game", "pk", default=None) or gd.get("gamePk"),

        "when": when,
        "statusPill": pill,
        "detailedState": status.get("detailedState") or "",
        "venue": venue,
        "weather": weather,
        "probables": probables,
        "decisions": decisions,

        "home": {
            "id": home_id,
            "name": home.get("name"),
            "abbrev": (home.get("abbreviation") or "").upper(),
            "logo": f"https://www.mlbstatic.com/team-logos/{home_id}.svg" if home_id else None,
            "record": _safe(home, "record", "summary", default=None),
            "score": home_runs,
        },
        "away": {
            "id": away_id,
            "name": away.get("name"),
            "abbrev": (away.get("abbreviation") or "").upper(),
            "logo": f"https://www.mlbstatic.com/team-logos/{away_id}.svg" if away_id else None,
            "record": _safe(away, "record", "summary", default=None),
            "score": away_runs,
        },
    }

    # =====================
    # LINESCORE (innings)
    # =====================
    innings = linescore.get("innings") or []
    if innings:
        inn_list = []
        for inn in innings:
            inn_list.append({
                "num": inn.get("num"),
                "away": _safe(inn, "away", "runs", default=None),
                "home": _safe(inn, "home", "runs", default=None),
            })
        out["innings"] = inn_list
        out["totals"] = {
            "away": {
                "R": _safe(linescore, "teams", "away", "runs", default=None),
                "H": _safe(linescore, "teams", "away", "hits", default=None),
                "E": _safe(linescore, "teams", "away", "errors", default=None),
            },
            "home": {
                "R": _safe(linescore, "teams", "home", "runs", default=None),
                "H": _safe(linescore, "teams", "home", "hits", default=None),
                "E": _safe(linescore, "teams", "home", "errors", default=None),
            },
        }

    # =====================
    # BOXSCORE (batting/pitching)
    # =====================
    box = ld.get("boxscore") or {}
    if box:
        bteams = box.get("teams") or {}
        for side in ("home", "away"):
            bt = bteams.get(side) or {}
            bat = bt.get("batters") or []
            pit = bt.get("pitchers") or []
            players = (bt.get("players") or {})

            # Batting lines (minimal, safe)
            bat_lines = []
            for pid in bat:
                p = players.get(f"ID{pid}") or {}
                person = p.get("person") or {}
                stats = _safe(p, "stats", "batting", default={}) or {}
                if not person:
                    continue
                bat_lines.append({
                    "id": person.get("id"),
                    "name": person.get("fullName"),
                    "pos": _safe(p, "position", "abbreviation", default=""),
                    "ab": stats.get("atBats"),
                    "r": stats.get("runs"),
                    "h": stats.get("hits"),
                    "rbi": stats.get("rbi"),
                    "bb": stats.get("baseOnBalls"),
                    "so": stats.get("strikeOuts"),
                    "avg": stats.get("avg"),
                    "ops": stats.get("ops"),
                })

            # Pitching lines (minimal, safe)
            pit_lines = []
            for pid in pit:
                p = players.get(f"ID{pid}") or {}
                person = p.get("person") or {}
                stats = _safe(p, "stats", "pitching", default={}) or {}
                if not person:
                    continue
                pit_lines.append({
                    "id": person.get("id"),
                    "name": person.get("fullName"),
                    "ip": stats.get("inningsPitched"),
                    "h": stats.get("hits"),
                    "r": stats.get("runs"),
                    "er": stats.get("earnedRuns"),
                    "bb": stats.get("baseOnBalls"),
                    "so": stats.get("strikeOuts"),
                    "era": stats.get("era"),
                    "pitches": stats.get("pitchesThrown"),
                    "strikes": stats.get("strikes"),
                })

            out[side]["batting"] = bat_lines if bat_lines else None
            out[side]["pitching"] = pit_lines if pit_lines else None

    # =====================
    # PLAY-BY-PLAY (last plays + PAs)
    # =====================
    plays = ld.get("plays") or {}
    all_plays = plays.get("allPlays") or []
    if all_plays:
        pbp = []
        pas = []

        for p in all_plays[-75:]:  # keep it bounded
            about = p.get("about") or {}
            res = p.get("result") or {}
            matchup = p.get("matchup") or {}
            batter = matchup.get("batter") or {}
            pitcher = matchup.get("pitcher") or {}

            pbp.append({
                "inning": about.get("inning"),
                "halfInning": about.get("halfInning"),
                "startTime": _to_user_tz_iso(about.get("startTime") or ""),
                "endTime": _to_user_tz_iso(about.get("endTime") or ""),
                "event": res.get("event") or "",
                "description": res.get("description") or "",
                "rbi": res.get("rbi"),
                "awayScore": res.get("awayScore"),
                "homeScore": res.get("homeScore"),
                "batter": batter.get("fullName") or "",
                "batterId": batter.get("id"),
                "pitcher": pitcher.get("fullName") or "",
                "pitcherId": pitcher.get("id"),
            })

            # Plate appearance pitch list (if present)
            pitch_list = []
            for ev in (p.get("playEvents") or []):
                details = ev.get("details") or {}
                pitch = ev.get("pitchData") or {}
                if pitch:
                    pitch_list.append({
                        "description": details.get("description"),
                        "code": details.get("call") and (details.get("call") or {}).get("code"),
                        "startSpeed": _safe(pitch, "startSpeed", default=None),
                        "endSpeed": _safe(pitch, "endSpeed", default=None),
                        "breakAngle": _safe(pitch, "breakAngle", default=None),
                        "zone": _safe(pitch, "zone", default=None),
                    })

            if pitch_list:
                pas.append({
                    "inning": about.get("inning"),
                    "halfInning": about.get("halfInning"),
                    "event": res.get("event") or "",
                    "description": res.get("description") or "",
                    "rbi": res.get("rbi"),
                    "awayScore": res.get("awayScore"),
                    "homeScore": res.get("homeScore"),
                    "batter": batter.get("fullName") or "",
                    "batterId": batter.get("id"),
                    "pitcher": pitcher.get("fullName") or "",
                    "pitcherId": pitcher.get("id"),
                    "pitches": pitch_list,
                })

        out["pbp"] = pbp if pbp else None
        out["pas"] = pas if pas else None

    return out



def get_schedule_game_by_pk(game_pk: int) -> Optional[dict]:
    """
    Fallback for scheduled games where feed/live may 404.
    Returns the schedule 'game' object (the one inside dates[].games[]).
    """
    cache_key = f"schedule_game:{game_pk}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    url = f"{BASE}/schedule"
    params = {
        "sportId": 1,
        "gamePk": game_pk,
        "hydrate": "team,probablePitchers,venue",
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json() or {}

    game = None
    for d in (data.get("dates") or []):
        games = d.get("games") or []
        if games:
            game = games[0]
            break

    _set_cached(cache_key, game)
    return game

def _season_from_date(date_ymd: str) -> int:
    try:
        return int((date_ymd or "")[:4])
    except Exception:
        return datetime.utcnow().year

def get_pitcher_season_line(person_id: int, season: int) -> dict:
    """
    Returns {'w': int|None, 'l': int|None, 'era': str|None}
    Cached.
    """
    if not person_id:
        return {"w": None, "l": None, "era": None}

    cache_key = f"pstats:{person_id}:{season}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    try:
        url = f"{BASE}/people/{person_id}/stats"
        params = {"stats": "season", "group": "pitching", "season": season}
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json() or {}

        splits = (((data.get("stats") or [])[0] or {}).get("splits") or [])
        stat = (splits[0].get("stat") if splits else {}) or {}

        out = {
            "w": _safe_int(stat.get("wins")),
            "l": _safe_int(stat.get("losses")),
            "era": str(stat.get("era")) if stat.get("era") is not None else None,
        }
    except Exception:
        out = {"w": None, "l": None, "era": None}

    _set_cached(cache_key, out)
    return out

def _pp_text(pp_obj: dict, season: int) -> str:
    """
    pp_obj is schedule probablePitchers.home/away object (may include id + fullName).
    """
    if not pp_obj:
        return "PP: TBD"

    name = (pp_obj.get("fullName") or "").strip()
    pid = pp_obj.get("id")

    if not name:
        return "PP: TBD"

    line = get_pitcher_season_line(pid, season) if pid else {"w": None, "l": None, "era": None}
    w = line.get("w")
    l = line.get("l")
    era = line.get("era")

    if w is None or l is None or not era:
        return f"PP: {name}"

    return f"PP: {name} ({w}-{l} • {era})"

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
