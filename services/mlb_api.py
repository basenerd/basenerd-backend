# services/mlb_api.py

import time
import json
import os
import random
import bisect
from datetime import datetime, date, timedelta
from typing import Any, Dict, Optional, List, Tuple
import unicodedata
import difflib
import requests

BASE = "https://statsapi.mlb.com/api/v1"
MLB_API_BASE = BASE  # for older helpers that reference MLB_API_BASE

# ----------------------------
# Simple in-memory cache
# ----------------------------
_cache: Dict[str, Dict[str, Any]] = {}
CACHE_TTL_SECONDS = 60 * 5  # 5 minutes
# ----------------------------
# 40-man directory (All teams)
# ----------------------------

FORTYMAN_TTL_SECONDS = 60 * 60 * 24  # 24 hours

def _norm_txt(s: str) -> str:
    """
    Normalize for loose matching:
    - strip accents/diacritics
    - lowercase
    - remove punctuation
    - collapse whitespace
    """
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    # keep letters/numbers/spaces
    s = "".join(ch if (ch.isalnum() or ch.isspace()) else " " for ch in s)
    s = " ".join(s.split())
    return s

def get_40man_directory(season: int = None) -> List[dict]:
    """
    Returns a sorted list of 40-man players across all MLB teams.
    Each item: {id, fullName, firstName, lastName, pos, team}
    Cached for 24 hours.
    """
    if season is None:
        season = datetime.now().year

    cache_key = f"40man_directory:{season}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    teams_data = get_teams(season) or {}
    teams = (teams_data.get("teams") or [])

    # teamId -> abbreviation (e.g., 146 -> MIA)
    team_abbrev = {}
    for t in teams:
        tid = t.get("id")
        ab = t.get("abbreviation") or t.get("abbrev")
        if tid and ab:
            team_abbrev[int(tid)] = ab

    out: List[dict] = []
    seen_ids = set()

    for tid, ab in team_abbrev.items():
        url = f"{BASE}/teams/{tid}/roster/40Man"
        params = {
            "hydrate": "person",
            "fields": "roster,person,id,fullName,firstName,lastName,position,abbreviation"
        }
        r = requests.get(url, params=params, timeout=25)
        r.raise_for_status()
        roster = (r.json() or {}).get("roster", []) or []

        for row in roster:
            person = (row.get("person") or {})
            pid = person.get("id")
            if not pid:
                continue
            pid = int(pid)
            if pid in seen_ids:
                continue
            seen_ids.add(pid)

            full = person.get("fullName") or ""
            first = person.get("firstName") or ""
            last = person.get("lastName") or ""

            pos_obj = (row.get("position") or {})
            pos = pos_obj.get("abbreviation") or pos_obj.get("code") or pos_obj.get("name") or ""

            out.append({
                "id": pid,
                "fullName": full,
                "firstName": first,
                "lastName": last,
                "pos": pos,
                "team": ab,
                "_k": _norm_txt(f"{full} {last} {first} {ab} {pos}"),
            })

    def _sort_key(p: dict):
        ln = (p.get("lastName") or "").strip()
        fn = (p.get("firstName") or "").strip()
        if not ln:
            full = (p.get("fullName") or "").strip()
            ln = full.split()[-1] if full else ""
        return (_norm_txt(ln), _norm_txt(fn), _norm_txt(p.get("fullName") or ""))

    out.sort(key=_sort_key)

    _set_cached(cache_key, out, ttl=FORTYMAN_TTL_SECONDS)
    return out

def _fuzzy_score(qn: str, kn: str) -> float:
    """
    Score query normalized string vs key normalized string.
    Combines substring/prefix boosts with similarity ratio (misspellings).
    """
    if not qn or not kn:
        return 0.0

    score = 0.0
    if kn.startswith(qn):
        score += 2.2
    if f" {qn}" in kn:
        score += 1.6
    if qn in kn:
        score += 1.2

    score += difflib.SequenceMatcher(None, qn, kn).ratio()
    return score

def suggest_40man_players(query: str, season: int = None, limit: int = 10) -> List[dict]:
    """
    Returns top matches from the 40-man directory for autocomplete.
    Each item: {id, label, url}
    """
    q = _norm_txt(query or "")
    if not q:
        return []

    players = get_40man_directory(season=season)
    scored = []
    for p in players:
        kn = p.get("_k") or ""
        s = _fuzzy_score(q, kn)
        if s > 0.55:
            scored.append((s, p))

    scored.sort(key=lambda x: x[0], reverse=True)
    top = [p for _, p in scored[:max(1, int(limit))]]

    out = []
    for p in top:
        label = f"{p.get('fullName','')} \u2022 {p.get('pos','')} \u2022 {p.get('team','')}"
        out.append({
            "id": p["id"],
            "label": label,
            "url": f"/player/{p['id']}",
        })
    return out

def filter_40man_by_letter(letter: str, season: int = None) -> List[dict]:
    """
    Filter directory by starting letter of last name.
    letter: "A".."Z" or "#"
    """
    players = get_40man_directory(season=season)
    L = (letter or "A").upper()

    def last_initial(p: dict) -> str:
        ln = (p.get("lastName") or "").strip()
        if not ln:
            full = (p.get("fullName") or "").strip()
            ln = full.split()[-1] if full else ""
        ln_norm = _norm_txt(ln)
        ch = (ln_norm[:1] or "")
        if ch and ch[0].isalpha():
            return ch[0].upper()
        return "#"

    if L == "#":
        return [p for p in players if last_initial(p) == "#"]
    return [p for p in players if last_initial(p) == L]
    
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
    ttl = item.get("ttl", CACHE_TTL_SECONDS)
    if time.time() - item["ts"] > ttl:
        return None
    return item["data"]

def _set_cached(key: str, data, ttl: int = None):
    _cache[key] = {"ts": time.time(), "data": data}
    if ttl is not None:
        _cache[key]["ttl"] = int(ttl)



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
    
def _safe_float(x, default: Optional[float] = 0.0) -> Optional[float]:
    """
    Like to_float(), but returns `default` instead of None on parse failure.
    """
    v = to_float(x)
    return default if v is None else v
    
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

def normalize_gamecast(feed: dict) -> dict:
    """
    Minimal payload for the GameCast tab polling /game/<pk>/gamecast.json

    Front-end expects:
      ok, inning, half, balls, strikes, outs,
      score:{away,home}, lastPlay,
      batter, pitcher,
      runners:{first,second,third},
      pitches:[...],
      lineups:{away,home}
    """

    if not feed or (feed.get("scheduleOnly") is True):
        return {"ok": False, "reason": (feed or {}).get("_fallback_reason") or "scheduleOnly"}

    game_data = feed.get("gameData") or {}
    live_data = feed.get("liveData") or {}

    status = game_data.get("status") or {}
    state = (status.get("abstractGameState") or "").lower()  # "live", "final", etc.

    linescore = live_data.get("linescore") or {}
    teams_ls = linescore.get("teams") or {}
    offense = linescore.get("offense") or {}

    inning = linescore.get("currentInning")
    half_raw = (linescore.get("inningHalf") or linescore.get("inningState") or "")
    half = "Top" if str(half_raw).lower().startswith("top") else ("Bottom" if str(half_raw).lower().startswith("bot") else "")

    # Prefer linescore counts; fallback to current play count
    balls = linescore.get("balls")
    strikes = linescore.get("strikes")
    outs = linescore.get("outs")

    # Plays
    plays = (live_data.get("plays") or {})
    all_plays = plays.get("allPlays") or []
    current_play = plays.get("currentPlay") or {}
    matchup = current_play.get("matchup") or {}
    count = current_play.get("count") or {}

    if balls is None:
        balls = count.get("balls")
    if strikes is None:
        strikes = count.get("strikes")
    if outs is None:
        outs = count.get("outs")

    def _short_name(full: str) -> str:
        s = (full or "").strip()
        if not s:
            return ""
        parts = s.split()
        if len(parts) == 1:
            return s
    
        # Handle suffixes so "Jazz Chisholm Jr." -> "J. Chisholm, Jr."
        suffix_map = {
            "jr": "Jr.", "jr.": "Jr.",
            "sr": "Sr.", "sr.": "Sr.",
            "ii": "II", "iii": "III", "iv": "IV", "v": "V",
        }
    
        last = parts[-1].strip()
        last_lc = last.lower().strip(".")
        # normalize key for map lookup
        last_key = (last.lower() if last.lower() in suffix_map else (last_lc if last_lc in suffix_map else last.lower()))
    
        if last_key in suffix_map and len(parts) >= 3:
            suffix = suffix_map[last_key]
            base_last = parts[-2]
            last_name = f"{base_last}, {suffix}"
        else:
            last_name = parts[-1]
    
        return f"{parts[0][0]}. {last_name}"

    def _headshot(pid: int | None) -> str | None:
        try:
            return get_player_headshot_url(int(pid), size=120) if pid else None
        except Exception:
            return None

    batter_obj = matchup.get("batter") or {}
    pitcher_obj = matchup.get("pitcher") or {}

    batter_id = batter_obj.get("id")
    pitcher_id = pitcher_obj.get("id")

    batter = {
        "id": batter_id,
        "name": batter_obj.get("fullName") or batter_obj.get("name"),
        "shortName": _short_name(batter_obj.get("fullName") or batter_obj.get("name") or ""),
        "headshot": _headshot(batter_id),
        "pos": "",
        "line": "",
    }
    pitcher = {
        "id": pitcher_id,
        "name": pitcher_obj.get("fullName") or pitcher_obj.get("name"),
        "shortName": _short_name(pitcher_obj.get("fullName") or pitcher_obj.get("name") or ""),
        "headshot": _headshot(pitcher_id),
        "pos": "",
        "line": "",
    }

    # -------------------------
    # LAST PLAY: real at-bat outcome, not "Ball/Strike/Foul"
    # Prefer most recent completed play's result.description.
    # -------------------------
    def _play_desc(p: dict) -> str:
        if not isinstance(p, dict):
            return ""
        res = p.get("result") or {}
        return (res.get("description") or "").strip()

    def _is_complete(p: dict) -> bool:
        if not isinstance(p, dict):
            return False
        about = p.get("about") or {}
        return bool(about.get("isComplete"))

    # Find last completed play with a meaningful description
    last_complete = None
    for p in reversed(all_plays or []):
        if _is_complete(p) and _play_desc(p):
            last_complete = p
            break

    # If current_play is complete (rare timing), allow it
    if _is_complete(current_play) and _play_desc(current_play):
        last_play = _play_desc(current_play)
    else:
        last_play = _play_desc(last_complete) if last_complete else ""

    # Fallback if game just started and no completed play exists
    if not last_play:
        last_play = "—"

    # -------------------------
    # Bases / runners
    # -------------------------
    def _runner(base_key: str):
        o = offense.get(base_key) or {}
        pid = o.get("id") or (o.get("player") or {}).get("id")
        nm = o.get("fullName") or o.get("name") or (o.get("player") or {}).get("fullName")
        return {
            "id": pid,
            "name": nm,
            "shortName": _short_name(nm or ""),
            "headshot": _headshot(pid),
        } if (pid or nm) else None

    runners = {
        "first": _runner("first"),
        "second": _runner("second"),
        "third": _runner("third"),
    }

    # -------------------------
    # Pitches for zone + table (current PA)
    # -------------------------
    pitches_out = []
    play_events = current_play.get("playEvents") or []
    n = 0
    for ev in play_events:
        if not ev or not ev.get("isPitch"):
            continue
        n += 1
        details = ev.get("details") or {}
        pitch_data = ev.get("pitchData") or {}
        coords = pitch_data.get("coordinates") or {}
        breaks = pitch_data.get("breaks") or {}

        call = details.get("call") or {}
        pitches_out.append({
            "n": n,
            "px": coords.get("pX"),
            "pz": coords.get("pZ"),
            "sz_top": coords.get("strikeZoneTop"),
            "sz_bot": coords.get("strikeZoneBottom"),

            "pitchType": (details.get("type") or {}).get("description") or "",
            "mph": pitch_data.get("startSpeed"),
            "spinRate": breaks.get("spinRate"),
            "vertMove": breaks.get("inducedVerticalBreak") if breaks.get("inducedVerticalBreak") is not None else breaks.get("breakVertical"),
            "horizMove": breaks.get("horizontalBreak") if breaks.get("horizontalBreak") is not None else breaks.get("breakHorizontal"),

            # Outcome flags (frontend colors dots)
            "isBall": details.get("isBall"),
            "isStrike": details.get("isStrike"),
            "isInPlay": details.get("isInPlay"),
            "isFoul": details.get("isFoul"),
            "call": call.get("description") or call.get("code") or "",
            "desc": details.get("description") or "",
        })
        # -------------------------
    # -------------------------
    # Ball-in-play (BIP) takeover payload
    # NOTE: computed later in a single canonical block (see below).
    # Keep a placeholder here so 'bip' is always defined.
    # -------------------------
    bip = None
    # -------------------------
    # PA LOG: build from completed allPlays so lineup expansions work
    # -------------------------
    def _inning_label(p: dict) -> str:
        if not isinstance(p, dict):
            return ""
        about = p.get("about") or {}
        inn = about.get("inning")
        half_inn = (about.get("halfInning") or "").lower()
        if not inn:
            return ""
        half_short = "Top" if half_inn.startswith("top") else ("Bot" if half_inn.startswith("bot") else "")
        return f"{half_short} {inn}".strip()

    pa_log: dict[int, list[dict]] = {}
    for p in (all_plays or []):
        if not _is_complete(p):
            continue
        desc = _play_desc(p)
        if not desc:
            continue
        matchup_p = p.get("matchup") or {}
        batter_p = matchup_p.get("batter") or {}
        pid = batter_p.get("id")
        if not pid:
            continue
        try:
            pid_int = int(pid)
        except Exception:
            continue
        pa_log.setdefault(pid_int, []).append({
            "inning": _inning_label(p),
            "desc": desc,
        })

    # -------------------------
    # Lineups: include per-player PA logs (pas) so UI doesn't show "No PAs yet"
    # -------------------------
    lineups = {"away": [], "home": []}
    try:
        box = (live_data.get("boxscore") or {}).get("teams") or {}
        for side in ("away", "home"):
            t = box.get(side) or {}
            players = t.get("players") or {}

            # Prefer battingOrder if present; fallback to batters list.
            order = t.get("battingOrder") or t.get("batters") or []
            out = []
            spot = 0
            for pid in order:
                spot += 1
                p = players.get(f"ID{pid}") or {}
                person = p.get("person") or {}
                pos = (p.get("position") or {}).get("abbreviation") or ""
                stat = (p.get("stats") or {}).get("batting") or {}
                ab = stat.get("atBats")
                h = stat.get("hits")
                hr = stat.get("homeRuns")
                rbi = stat.get("rbi")
                bb = stat.get("baseOnBalls")

                base = f"{h}-{ab}" if (ab is not None and h is not None) else ""
                tags = []
                try:
                    if hr is not None and int(hr) > 0:
                        tags.append("HR" if int(hr) == 1 else f"{int(hr)} HR")
                except Exception:
                    pass
                try:
                    if rbi is not None and int(rbi) > 0:
                        tags.append(f"{int(rbi)} RBI")
                except Exception:
                    pass
                try:
                    if bb is not None and int(bb) > 0:
                        tags.append("BB" if int(bb) == 1 else f"{int(bb)} BB")
                except Exception:
                    pass

                bat_line = base
                game_summary = base + (", " + ", ".join(tags) if tags else "")


                nm = person.get("fullName") or person.get("name") or ""
                try:
                    pid_int = int(pid) if pid is not None else None
                except Exception:
                    pid_int = None

                out.append({
                    "id": pid_int,
                    "name": nm,
                    "shortName": _short_name(nm),
                    "headshot": _headshot(pid_int),
                    "pos": pos,
                    "batLine": bat_line,
                                        "gameSummary": game_summary,
                    "spot": spot,
                    "pas": (pa_log.get(pid_int) or []) if pid_int else [],
                })
            lineups[side] = out
    except Exception:
        pass
        # -------------------------
    # BETWEEN INNINGS + DUE UP (next 3 hitters)
    # -------------------------
    inning_state_raw = (linescore.get("inningState") or linescore.get("inningHalf") or "")
    inning_state = str(inning_state_raw).strip().lower()

    # "Middle" = between Top/Bottom (home due up)
    # "End"    = between innings (away due up next inning)
    between_innings = inning_state in ("middle", "end") or ("between" in inning_state)

    due_up = {"show": False, "team": None, "players": []}

    if between_innings:
        # Decide who bats next
        due_team = None
        if inning_state.startswith("mid"):
            due_team = "home"
        elif inning_state.startswith("end"):
            due_team = "away"
        else:
            # fallback: if we can't tell, pick opposite of current half
            due_team = "home" if half == "Top" else "away"

        # Build pid -> player object map from existing lineup payload
        pid_to_player = {}
        for side in ("away", "home"):
            for p in (lineups.get(side) or []):
                pid = p.get("id")
                if pid:
                    pid_to_player[int(pid)] = p

        # Pull batting order + current spot from boxscore (best-effort)
        try:
            box = (live_data.get("boxscore") or {}).get("teams") or {}
            team_box = box.get(due_team) or {}
            order = team_box.get("battingOrder") or team_box.get("batters") or []
            spot = team_box.get("battingOrderSpot")

            # Some feeds use 0-based spot; some 1-based; normalize gently
            try:
                spot_i = int(spot) if spot is not None else 0
            except Exception:
                spot_i = 0
            if spot_i < 0:
                spot_i = 0
            if spot_i >= len(order):
                spot_i = 0

            # Next 3, wrapping around
            nxt = []
            for j in range(3):
                if not order:
                    break
                pid = order[(spot_i + j) % len(order)]
                try:
                    pid_int = int(pid)
                except Exception:
                    continue
                pobj = pid_to_player.get(pid_int)
                if pobj:
                    nxt.append(pobj)

            if nxt:
                due_up = {"show": True, "team": due_team, "players": nxt}
        except Exception:
            pass
    score_away = (teams_ls.get("away") or {}).get("runs")
    score_home = (teams_ls.get("home") or {}).get("runs")
        # -------------------------
    # Inning state / Between innings / Due up
    # -------------------------
    inning_state = ((linescore or {}).get("inningState") or "").strip()
    # MLB feed uses values like: "Top", "Middle", "Bottom", "End"
    between_innings = inning_state.lower() in ("middle", "end", "between innings")
    inning_summary = None
    try:
        innings = (linescore.get("innings") or [])
        cur_inn = linescore.get("currentInning")
        if between_innings and cur_inn and innings and cur_inn >= 1:
            inn_obj = innings[cur_inn - 1] if (cur_inn - 1) < len(innings) else None
            if isinstance(inn_obj, dict):
                # Middle = end of TOP (away batted). End = end of BOTTOM (home batted).
                if str(inning_state).lower().startswith("mid"):
                    side_key = "away"
                    half_done = "Top"
                elif str(inning_state).lower().startswith("end"):
                    side_key = "home"
                    half_done = "Bottom"
                else:
                    side_key = "away" if half == "Top" else "home"
                    half_done = half or ""
    
                side = (inn_obj.get(side_key) or {})
                inning_summary = {
                    "inning": int(cur_inn),
                    "halfDone": half_done,
                    "runs": side.get("runs"),
                    "hits": side.get("hits"),
                    "errors": side.get("errors"),
                    "lob": side.get("leftOnBase"),
                }
    except Exception:
        inning_summary = None
    def _dueup_person(o: dict):
        if not isinstance(o, dict):
            return None
        pid = o.get("id") or (o.get("person") or {}).get("id")
        nm = o.get("fullName") or o.get("name") or (o.get("person") or {}).get("fullName")
        nm = (nm or "").strip()
        if not pid and not nm:
            return None
        try:
            pid_int = int(pid) if pid else None
        except Exception:
            pid_int = None

        return {
            "id": pid_int,
            "name": nm,
            "shortName": _short_name(nm),
            "headshot": _headshot(pid_int),
        }

    due_up = []
    try:
        # linescore.offense has batter/onDeck/inHole in many live games
        off = (linescore or {}).get("offense") or {}
        for k in ("batter", "onDeck", "inHole"):
            p = _dueup_person(off.get(k))
            if p:
                due_up.append(p)
    except Exception:
        due_up = []

    # Only send dueUp during between-innings (keeps frontend logic clean)
    if not between_innings:
        due_up = []

    # -------------------------
    # Ball-in-play (BIP) info for the takeover animation
    # -------------------------
    bip = None
    try:
        about = current_play.get("about") or {}
        at_bat_index = about.get("atBatIndex")
        play_events = current_play.get("playEvents") or []

        hit_ev = None
        pitch_n = 0
        hit_pitch_n = 0
        for ev in play_events:
            if not ev or not ev.get("isPitch"):
                continue
            pitch_n += 1
            hd = ev.get("hitData")
            if isinstance(hd, dict) and hd:
                hit_ev = ev
                hit_pitch_n = pitch_n

                # Pull best available hitData:
        # 1) from the pitch event that contains hitData (your existing approach)
        # 2) fall back to current_play.hitData (some feeds put it here)
        # 3) fall back to scanning any playEvents for hitData
        hd_best = None

        if hit_ev is not None:
            hd_best = hit_ev.get("hitData")

        if not isinstance(hd_best, dict) or not hd_best:
            hd_best = current_play.get("hitData")

        if not isinstance(hd_best, dict) or not hd_best:
            for ev in play_events:
                hd_try = (ev or {}).get("hitData")
                if isinstance(hd_try, dict) and hd_try:
                    hd_best = hd_try
                    break

        if isinstance(hd_best, dict) and hd_best:
            coords = hd_best.get("coordinates") if isinstance(hd_best.get("coordinates"), dict) else {}
            coord_x = to_float(coords.get("coordX"))
            coord_y = to_float(coords.get("coordY"))

            # Only build bip if coords exist (so mapping can render)
            if coord_x is not None and coord_y is not None:
                # Event label + description from play result
                res = (current_play.get("result") or {})
                ev_name = (res.get("event") or res.get("eventType") or "").strip()
                desc = (res.get("description") or "").strip()

                # Metrics (often missing; include if present)
                evv = to_float(hd_best.get("launchSpeed"))
                la = to_float(hd_best.get("launchAngle"))
                dist = to_float(hd_best.get("totalDistance"))

                # A stable id so the browser only animates once per BIP
                # Add coords so repeated balls in play within same PA don’t collide
                bip_id = f"{at_bat_index}:{hit_pitch_n or pitch_n}:{coord_x}:{coord_y}"

                bip = {
                    "id": bip_id,
                    "has": True,
                    "x": coord_x,
                    "y": coord_y,
                    "event": ev_name or "In play",
                    "description": desc or "Ball in play",
                    "ev": evv,
                    "la": la,
                    "dist": dist,
                }
    except Exception:
        bip = None
    # If not live yet, still return ok False so UI shows a friendly message
    if state != "live" and not inning:
        return {"ok": False, "reason": f"state={state or 'unknown'}"}

    return {
        "ok": True,
        "state": state,
        "inning": inning,
        "half": half,
        "balls": balls,
        "strikes": strikes,
        "outs": outs,
        "score": {"away": score_away, "home": score_home},
        "lastPlay": last_play,

        "batter": batter,
        "pitcher": pitcher,
        "runners": runners,
        "inningSummary": inning_summary,
        "pitches": pitches_out,
        "lineups": lineups,
        "inningState": inning_state_raw,
        "betweenInnings": between_innings,
        "dueUp": due_up,
        "bip": bip,
    }
    
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
    Adds: headshot, ht, wt, age, service (best-effort).
    """
    url = f"{BASE}/teams/{team_id}/roster/40Man"
    params = {"hydrate": "person"}  # ensures batSide/pitchHand/height/weight/birthDate show up
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

    def _height_to_dash(h: str) -> Optional[str]:
        if not h:
            return None
        # common formats: 6' 2"   or  6'2"
        s = str(h).replace('"', "").replace(" ", "").strip()
        if "'" in s:
            parts = s.split("'")
            ft = parts[0]
            inch = parts[1] if len(parts) > 1 else ""
            inch = inch.replace("-", "")
            if ft.isdigit() and (inch.isdigit() or inch == ""):
                return f"{int(ft)}-{int(inch) if inch else 0}"
        return str(h)

    def _age_from_birth(birth_ymd: str) -> Optional[int]:
        try:
            bd = datetime.strptime(birth_ymd, "%Y-%m-%d").date()
            today = datetime.utcnow().date()
            return today.year - bd.year - ((today.month, today.day) < (bd.month, bd.day))
        except Exception:
            return None

    def _service_years_from_debut(debut_ymd: str) -> Optional[str]:
        # MLB "service time" isn't directly exposed consistently; best-effort from mlbDebutDate.
        try:
            dd = datetime.strptime(debut_ymd, "%Y-%m-%d").date()
            today = datetime.utcnow().date()
            yrs = (today - dd).days / 365.25
            if yrs < 0:
                return None
            return f"{yrs:.1f}"
        except Exception:
            return None

    # simple silhouette fallback (data URI)
    headshot_fallback = (
        "data:image/svg+xml;utf8,"
        "<svg xmlns='http://www.w3.org/2000/svg' width='64' height='64' viewBox='0 0 64 64'>"
        "<rect width='64' height='64' rx='32' fill='%23222'/>"
        "<circle cx='32' cy='26' r='12' fill='%23555'/>"
        "<path d='M14 60c3-14 14-20 18-20s15 6 18 20' fill='%23555'/>"
        "</svg>"
    )

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

        pid = person.get("id")
        headshot = get_player_headshot_url(pid, size=80) if pid else None

        ht = _height_to_dash(person.get("height"))
        wt = person.get("weight")
        wt = str(wt) if wt is not None else None

        age = _age_from_birth(person.get("birthDate"))
        age = str(age) if age is not None else None

        service = None
        # if API ever includes serviceYears, prefer it
        if person.get("serviceYears") is not None:
            try:
                service = str(person.get("serviceYears"))
            except Exception:
                service = None
        if not service:
            service = _service_years_from_debut(person.get("mlbDebutDate"))

        row = {
            "id": pid,
            "name": person.get("fullName"),
            "jersey": item.get("jerseyNumber"),
            "bt": bt,
            "pos": pos_abbrev,
            "status_code": status_code,
            "status_desc": status_desc,
            "headshot": headshot,
            "headshot_fallback": headshot_fallback,
            "ht": ht,
            "wt": wt,
            "age": age,
            "service": service,
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
            home_pp_obj = _get_pp_obj_from_game(g, "home")
            away_pp_obj = _get_pp_obj_from_game(g, "away")
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
                        # -----------------------------
            # Decide what shows in the "pitcher line" slots on games.html
            # Scheduled: Probable Pitchers
            # Live: current Hitter (away slot) + current Pitcher (home slot)
            # Final: Winner pitcher on winning team slot, Loser pitcher on losing team slot
            # -----------------------------
            status = (g.get("status") or {})
            abstract = (status.get("abstractGameState") or "").lower()  # "live", "final", "preview", etc.
            detailed = (status.get("detailedState") or "").lower()

            def _short_name(full: str) -> str:
                s = (full or "").strip()
                if not s:
                    return ""
                parts = s.split()
                if len(parts) == 1:
                    return s
                return f"{parts[0][0]}. {parts[-1]}"

            away_slot = away_pp
            home_slot = home_pp

            # Live: show current batter/pitcher
            if abstract == "live" or "in progress" in detailed:
                try:
                    feed = get_game_feed(int(g.get("gamePk")))
                    gc = normalize_gamecast(feed) if feed else {}
                    if gc.get("ok"):
                        b = (gc.get("batter") or {})
                        p = (gc.get("pitcher") or {})
                        batter_nm = b.get("shortName") or _short_name(b.get("name") or "")
                        pitcher_nm = p.get("shortName") or _short_name(p.get("name") or "")
                        away_slot = f"H: {batter_nm}" if batter_nm else "H: —"
                        home_slot = f"P: {pitcher_nm}" if pitcher_nm else "P: —"
                    else:
                        away_slot = "H: —"
                        home_slot = "P: —"
                except Exception:
                    away_slot = "H: —"
                    home_slot = "P: —"

            # Final: show W/L in the same slots (on the winning/losing team row)
            elif abstract == "final" or "final" in detailed:
                winner_short = _short_name(win_p) if win_p else ""
                loser_short = _short_name(lose_p) if lose_p else ""

                # Determine which team won using score
                try:
                    a = away_score if away_score is not None else -999
                    h = home_score if home_score is not None else -999
                except Exception:
                    a, h = -999, -999

                away_won = (a > h)
                home_won = (h > a)

                if away_won:
                    away_slot = f"W: {winner_short}" if winner_short else "W: —"
                    home_slot = f"L: {loser_short}" if loser_short else "L: —"
                elif home_won:
                    away_slot = f"L: {loser_short}" if loser_short else "L: —"
                    home_slot = f"W: {winner_short}" if winner_short else "W: —"
                else:
                    # tie/unknown (shouldn't happen often)
                    away_slot = f"W: {winner_short}" if winner_short else "W: —"
                    home_slot = f"L: {loser_short}" if loser_short else "L: —"

            # Scheduled/Preview: keep probables, but ONLY if game hasn't started
            else:
                away_slot = away_pp
                home_slot = home_pp
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
                    "slot": home_slot,
                    "logo": f"https://www.mlbstatic.com/team-logos/{home_id}.svg" if home_id else None,
                    "score": home_score,
                },
                "away": {
                    "id": away_id,
                    "name": away_team.get("name"),
                    "abbrev": away_abbrev,
                    "record": away_rec,
                    "slot": away_slot,
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

    url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"

    try:
        r = requests.get(url, timeout=30)

        if r.status_code == 200:
            data = r.json() or {}

            status = ((data.get("gameData") or {}).get("status") or {})
            state = (status.get("abstractGameState") or "").lower()

            # IMPORTANT:
            # - live polling UX wants fresh data quickly
            # - keep TTL short when "live"
            if state == "live":
                ttl = 5
            elif state == "final":
                ttl = 60 * 60 * 24
            else:
                # preview / warmup / other
                ttl = 15

            _set_cached(cache_key, data, ttl=ttl)
            return data

        # Any other status: fallback
        sched_game = get_schedule_game_by_pk(game_pk)
        data = {
            "scheduleOnly": True,
            "scheduleGame": sched_game,
            "_fallback_reason": f"feed_status={r.status_code}"
        }

        # CRITICAL: don't cache fallback for long, or GameCast gets "stuck"
        _set_cached(cache_key, data, ttl=10)
        return data

    except Exception as e:
        # Timeout / connection / JSON decode / etc -> fallback
        sched_game = None
        try:
            sched_game = get_schedule_game_by_pk(game_pk)
        except Exception:
            pass

        data = {
            "scheduleOnly": True,
            "scheduleGame": sched_game,
            "_fallback_reason": f"exception={type(e).__name__}"
        }

        # CRITICAL: short TTL so we retry soon
        _set_cached(cache_key, data, ttl=10)
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

def get_stats_leaderboard(
    group: str,
    season: int,
    sort_stat: str,
    order: str = "desc",
    team_id: Optional[int] = None,
    position: Optional[str] = None,
    league_id: Optional[int] = None, 
    player_pool: str = "ALL",  # "ALL" or "QUALIFIED"
    limit: int = 50,
    offset: int = 0,
    game_type: str = "R",
) -> Dict[str, Any]:
    """
    Generic leaderboard pull via /api/v1/stats.

    group: "hitting" | "pitching" | "fielding" | "running"
    sort_stat: stat key to sort by (e.g., "ops", "homeRuns", "era", "stolenBases")
    order: "asc" or "desc"
    position: optional position code (e.g., "1B", "SS", "OF", "P")
    player_pool: "ALL" or "QUALIFIED"
    """
    group = (group or "").strip().lower()
    if group not in ("hitting", "pitching", "fielding", "running"):
        group = "hitting"

    order = (order or "desc").strip().lower()
    if order not in ("asc", "desc"):
        order = "desc"

    player_pool = (player_pool or "ALL").strip().upper()
    if player_pool not in ("ALL", "QUALIFIED"):
        player_pool = "ALL"

    try:
        season = int(season)
    except Exception:
        season = datetime.utcnow().year

    try:
        limit = int(limit)
    except Exception:
        limit = 50
    limit = max(1, min(limit, 200))

    try:
        offset = int(offset)
    except Exception:
        offset = 0
    offset = max(0, offset)

    sort_stat = (sort_stat or "").strip()
    if not sort_stat:
        # safe-ish defaults
        sort_stat = "ops" if group == "hitting" else ("era" if group == "pitching" else ("fielding" if group == "fielding" else "stolenBases"))

    cache_key = f"leaders:{group}:{season}:{sort_stat}:{order}:{team_id}:{position}:{league_id}:{player_pool}:{limit}:{offset}:{game_type}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    url = f"{BASE}/stats"
    params = {
        "stats": "season",
        "group": group,
        "season": season,
        "gameType": game_type,
        "sportIds": 1,
        "limit": limit,
        "offset": offset,
        "sortStat": sort_stat,
        "order": order,
        "playerPool": player_pool,
        # hydrate person + team info for rendering
        "hydrate": "person,currentTeam,team",
    }
    if team_id:
        params["teamId"] = int(team_id)
    if position:
        params["position"] = position
    if league_id:
        params["leagueId"] = int(league_id)

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json() or {}

    # Normalize into a simple structure for the template/JSON
    stats = (data.get("stats") or [])
    splits = (stats[0].get("splits") if stats else []) or []

    rows = []
    for sp in splits:
        person = sp.get("player") or sp.get("person") or {}
        team = sp.get("team") or (sp.get("team") or {})
        stat = sp.get("stat") or {}

        pid = person.get("id")
        tid = (team.get("id") if isinstance(team, dict) else None)

        rows.append(
            {
                "playerId": pid,
                "name": person.get("fullName") or person.get("name") or "—",
                "teamId": tid,
                "teamName": (team.get("name") if isinstance(team, dict) else None),
                "stat": stat,
                "headshot": get_player_headshot_url(int(pid), size=120) if pid else None,
                "teamLogo": f"https://www.mlbstatic.com/team-logos/{tid}.svg" if tid else None,
            }
        )

    out = {
        "group": group,
        "season": season,
        "sortStat": sort_stat,
        "order": order,
        "teamId": team_id,
        "position": position,
        "playerPool": player_pool,
        "limit": limit,
        "offset": offset,
        "rows": rows,
        # Note: StatsAPI doesn't always return a total count here; we page by offset anyway.
    }

    _set_cached(cache_key, out, ttl=60)  # short cache is fine for leaderboards
    return out

def normalize_game_detail(feed: dict, tz_name: str = "America/Phoenix") -> dict:
    """
    Normalizes /api/v1.1/game/{gamePk}/feed/live for game.html.

    Outputs used by game.html:
      - game.decisions: winner/loser/save (names) + legacy w/l/s
      - game.linescore: innings + team totals (uses '-' instead of None when missing)
      - game.box: away/home batting (pitchers filtered unless they actually batted) + pitching
      - game.scoring: scoring plays with awayScore/homeScore
      - game.pbp: grouped by inning+half with ordinal inning labels
      - game.pas: plate appearances with:
          - summaryEvent (event only)
          - pitches[] with plot fields (px/pz/sz_top/sz_bot/call/desc/isBall/isStrike/isInPlay/n)
            + table fields (pitchType/mph/spinRate/vertMove/horizMove)
          - battedBall (exitVelo, launchAngle, sprayAngle, xBA, directionLabel) when ball put in play
    """

    # ---------------------
    # scheduleOnly fallback
    # ---------------------
    if (feed or {}).get("scheduleOnly"):
        sg = (feed or {}).get("scheduleGame") or {}
        if sg:
            # ✅ Use the same normalizer everywhere so templates always get the same keys
            game_obj = normalize_schedule_game(sg, tz_name=tz_name)
    
            # ✅ Add empty placeholders that game.html may expect for non-live games
            game_obj.setdefault("linescore", {})
            game_obj.setdefault("box", {"away": {"batting": [], "pitching": []},
                                        "home": {"batting": [], "pitching": []}})
            game_obj.setdefault("scoring", [])
            game_obj.setdefault("pas", [])
            game_obj.setdefault("pbp", [])
    
            return game_obj
    
        # If scheduleGame is missing, still return something safe
        return {
            "gamePk": None,
            "statusPill": "Scheduled",
            "when": "",
            "venue": "",
            "away": {},
            "home": {},
            "linescore": {},
            "box": {"away": {"batting": [], "pitching": []}, "home": {"batting": [], "pitching": []}},
            "scoring": [],
            "pas": [],
            "pbp": [],
        }

    # ---------------------
    # small helpers
    # ---------------------
    def _safe(d: dict, *keys, default=None):
        cur = d
        for k in keys:
            if not isinstance(cur, dict):
                return default
            cur = cur.get(k)
        return cur if cur is not None else default

    def _safe_int(v, default=0):
        try:
            if v is None or v == "":
                return default
            return int(float(v))
        except Exception:
            return default

    def _safe_float(v, default=0.0):
        try:
            if v is None or v == "":
                return default
            return float(v)
        except Exception:
            return default

    def _half_norm(half_inning: str) -> str:
        """
        Normalize half inning labels to match template expectations:
        template uses 'Top' and 'Bottom'
        """
        s = (half_inning or "").strip()
        sl = s.lower()
        if sl == "top":
            return "Top"
        if sl == "bottom":
            return "Bottom"
        # If it's already 'Top'/'Bottom' or something else, keep best-effort
        if s in ("Top", "Bottom"):
            return s
        return s

    def _ordinal(n: int) -> str:
        if n is None:
            return ""
        if 10 <= (n % 100) <= 20:
            suf = "th"
        else:
            suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
        return f"{n}{suf}"

    # ✅ Change 1: capitalize Inning
    def _inning_label(n: int) -> str:
        return f"{_ordinal(n)} Inning" if n else ""

    # ✅ For batted ball direction label
    def _spray_label(spray_angle):
        if spray_angle is None:
            return None
        try:
            a = float(spray_angle)
        except Exception:
            return None
        if a <= -15:
            return "Pull"
        if a >= 15:
            return "Oppo"
        return "Center"

    def _pretty_pitch_type(pitch_data: dict, details: dict) -> str:
        t = _safe(details, "type", "description", default=None)
        if t:
            return t
        code = _safe(details, "type", "code", default=None) or ""
        code = str(code).upper()
        mp = {
            "FF": "4-Seam Fastball",
            "FT": "2-Seam Fastball",
            "SI": "Sinker",
            "FC": "Cutter",
            "SL": "Slider",
            "CU": "Curveball",
            "KC": "Knuckle Curve",
            "CH": "Changeup",
            "FS": "Splitter",
            "FO": "Forkball",
            "EP": "Eephus",
            "KN": "Knuckleball",
            "SV": "Slurve",
        }
        return mp.get(code) or (t or code or "Pitch")

    # ---------------------
    # main containers from feed
    # ---------------------
    game_data = (feed or {}).get("gameData") or {}
    live_data = (feed or {}).get("liveData") or {}
    linescore = (live_data.get("linescore") or {})
    boxscore = (live_data.get("boxscore") or {})
    plays = (live_data.get("plays") or {})
    all_plays = plays.get("allPlays") or []
    scoring_plays = plays.get("scoringPlays") or []

    # ---------------------
    # teams & header
    # ---------------------
    away_team = _safe(game_data, "teams", "away", default={}) or {}
    home_team = _safe(game_data, "teams", "home", default={}) or {}

    away_id = away_team.get("id")
    home_id = home_team.get("id")

    def _logo(tid):
        return f"https://www.mlbstatic.com/team-logos/{tid}.svg" if tid else None

    status = _safe(game_data, "status", "detailedState", default="") or ""
    status_pill = status or "Unknown"
    venue = _safe(game_data, "venue", "name", default=None)

    game_obj = {
        "statusPill": status_pill,
        "when": _safe(game_data, "datetime", "dateTime", default=None),
        "venue": venue,
        "away": {
            "id": away_id,
            "name": away_team.get("name"),
            "abbrev": away_team.get("abbreviation"),
            "logo_url": _logo(away_id),
            "record": None,
            "score": None,
        },
        "home": {
            "id": home_id,
            "name": home_team.get("name"),
            "abbrev": home_team.get("abbreviation"),
            "logo_url": _logo(home_id),
            "record": None,
            "score": None,
        },
        "probables": {"away": None, "home": None},
        "decisions": {},
        "linescore": None,
        "box": None,
        "scoring": [],
        "pbp": [],
        "pas": [],
    }

    # scores if present
    game_obj["away"]["score"] = _safe_int(_safe(linescore, "teams", "away", "runs", default=None), default=None)
    game_obj["home"]["score"] = _safe_int(_safe(linescore, "teams", "home", "runs", default=None), default=None)

    # probables (best effort)
    game_obj["probables"]["away"] = _safe(game_data, "probablePitchers", "away", "fullName", default=None)
    game_obj["probables"]["home"] = _safe(game_data, "probablePitchers", "home", "fullName", default=None)

    # decisions (best effort)
    decisions = _safe(live_data, "decisions", default={}) or {}
    game_obj["decisions"] = {
        "winner": _safe(decisions, "winner", "fullName", default=None),
        "loser": _safe(decisions, "loser", "fullName", default=None),
        "save": _safe(decisions, "save", "fullName", default=None),
        "w": _safe(decisions, "winner", "fullName", default=None),
        "l": _safe(decisions, "loser", "fullName", default=None),
        "s": _safe(decisions, "save", "fullName", default=None),
    }

    # ---------------------
    # linescore table
    # ---------------------
    innings_out = []
    inn_list = linescore.get("innings") or []
    for inn in inn_list:
        innings_out.append(
            {
                "num": _safe_int(inn.get("num"), default=None),
                "away": {"runs": inn.get("away") and inn["away"].get("runs")},
                "home": {"runs": inn.get("home") and inn["home"].get("runs")},
            }
        )

    if inn_list:
        game_obj["linescore"] = {
            "innings": innings_out,
            "teams": {
                "away": {
                    "runs": _safe(linescore, "teams", "away", "runs", default=None),
                    "hits": _safe(linescore, "teams", "away", "hits", default=None),
                    "errors": _safe(linescore, "teams", "away", "errors", default=None),
                },
                "home": {
                    "runs": _safe(linescore, "teams", "home", "runs", default=None),
                    "hits": _safe(linescore, "teams", "home", "hits", default=None),
                    "errors": _safe(linescore, "teams", "home", "errors", default=None),
                },
            },
        }

    # ---------------------
    # scoring plays
    # ---------------------
    scoring_out = []
    for idx in scoring_plays:
        try:
            play = all_plays[int(idx)]
        except Exception:
            continue
        about = play.get("about") or {}
        inning = _safe_int(about.get("inning"), default=None)
        half = _half_norm(about.get("halfInning"))
        scoring_out.append(
            {
                "inning": inning,
                "half": half,
                "inningLabel": _inning_label(inning),
                "description": _safe(play, "result", "description", default="") or "",
                "awayScore": _safe_int(_safe(play, "result", "awayScore", default=None), default=None),
                "homeScore": _safe_int(_safe(play, "result", "homeScore", default=None), default=None),
            }
        )
    game_obj["scoring"] = scoring_out
    # ---------------------
    # boxscore (batting + pitching)
    # ---------------------
        # ---------------------
    # play-by-play (PBP) cards
    # Build directly from allPlays so the tab never goes blank
    # ---------------------
    pbp_bucket = {}  # (inning, half) -> group

    def _half_sort_key(h):
        return 0 if (h or "").lower().startswith("top") else 1

    for p in all_plays:
        about = p.get("about") or {}
        inning = _safe_int(about.get("inning"), default=None)
        half = _half_norm(about.get("halfInning"))

        # skip if we can't place it
        if inning is None or not half:
            continue

        key = (inning, half)
        if key not in pbp_bucket:
            pbp_bucket[key] = {
                "inning": inning,
                "half": half,
                "inningLabel": _inning_label(inning),
                "plays": []
            }

        # Useful display fields
        result = p.get("result") or {}
        matchup = p.get("matchup") or {}
        batter = (matchup.get("batter") or {}).get("fullName")
        pitcher = (matchup.get("pitcher") or {}).get("fullName")

        desc = (result.get("description") or "").strip()
        event = (result.get("event") or result.get("eventType") or "").strip()

        pbp_bucket[key]["plays"].append({
            "atBatIndex": about.get("atBatIndex"),
            "startTime": about.get("startTime"),
            "endTime": about.get("endTime"),
            "batter": batter,
            "pitcher": pitcher,
            "event": event,
            "description": desc,
            "awayScore": result.get("awayScore"),
            "homeScore": result.get("homeScore"),
            "isScoringPlay": bool(result.get("isScoringPlay")),
        })

    # sort plays within each half-inning by atBatIndex
    for grp in pbp_bucket.values():
        grp["plays"].sort(key=lambda x: _safe_int(x.get("atBatIndex"), default=10**9))

    # order groups by inning asc, Top then Bottom
    pbp_out = list(pbp_bucket.values())
    pbp_out.sort(key=lambda g: (_safe_int(g.get("inning"), default=10**9), _half_sort_key(g.get("half"))))

    game_obj["pbp"] = pbp_out

    def _ip_to_outs(ip_val):
        """Convert inningsPitched string like '5.2' to outs (5*3+2)."""
        if ip_val is None or ip_val == "" or ip_val == "-":
            return 0
        try:
            s = str(ip_val).strip()
            if "." in s:
                a, b = s.split(".", 1)
                whole = int(a or 0)
                frac = int(b or 0)
                return whole * 3 + frac
            return int(float(s)) * 3
        except Exception:
            return 0

    def _outs_to_ip(outs: int) -> str:
        try:
            outs = int(outs or 0)
        except Exception:
            outs = 0
        whole = outs // 3
        frac = outs % 3
        return f"{whole}.{frac}" if frac else str(whole)

    def _lerp(a, b, t):
        return int(a + (b - a) * t)

    def _clamp(x, lo, hi):
        return max(lo, min(hi, x))

    def _grad_style(val, avg, span, alpha=0.22):
        """Blue->Red based on (val-avg)/span."""
        if val is None:
            return ""
        try:
            v = float(val)
        except Exception:
            return ""
        # blue (slow) -> red (fast)
        blue = (59, 130, 246)
        red = (239, 68, 68)
        t = _clamp((v - avg) / float(span), -1.0, 1.0)
        t = (t + 1.0) / 2.0
        r = _lerp(blue[0], red[0], t)
        g = _lerp(blue[1], red[1], t)
        b = _lerp(blue[2], red[2], t)
        return f"background: linear-gradient(180deg, rgba({r},{g},{b},{alpha}), rgba({r},{g},{b},{alpha*0.55})); border:1px solid rgba({r},{g},{b},0.25); color:#0f172a;"

    # Rough MLB velo baselines by pitch type code (mph). Used only for UI gradients.
    PITCH_AVG = {
        "FF": 94.5, "FT": 93.3, "SI": 93.2, "FC": 91.5,
        "FS": 86.5, "CH": 85.5,
        "SL": 85.5, "CU": 79.5, "KC": 80.5, "SV": 82.5,
        "KN": 70.0, "EP": 60.0,
    }

    def _extract_batting(side_key: str):
        team = _safe(boxscore, "teams", side_key, default={}) or {}
        players = team.get("players") or {}

        rows = []
        for _, pdata in players.items():
            person = pdata.get("person") or {}
            stats = _safe(pdata, "stats", "batting", default={}) or {}
            pos = _safe(pdata, "position", "abbreviation", default=None)

            # include only players who appeared (started/subbed) OR have any batting counting
            gs = pdata.get("gameStatus")

            # StatsAPI is inconsistent: sometimes gameStatus is a string, sometimes a dict.
            if isinstance(gs, dict):
                # best-effort extract any meaningful stringy status
                gs_str = (
                    gs.get("status")
                    or gs.get("detailedState")
                    or gs.get("abstractGameState")
                    or gs.get("code")
                    or ""
                )
            else:
                gs_str = gs or ""
            
            game_status = str(gs_str).lower()
            appeared = game_status in ("started", "substituted", "entered")
            has_bat = any((stats.get(k) not in (None, "")) for k in ("atBats", "plateAppearances", "hits", "runs", "rbi", "baseOnBalls", "strikeOuts", "homeRuns"))
            if not (appeared or has_bat):
                continue

            bo_raw = pdata.get("battingOrder")
            try:
                bo = int(str(bo_raw)) if bo_raw not in (None, "") else None
            except Exception:
                bo = None

            indent = False
            if bo is not None:
                indent = (bo % 100) != 0

            rows.append(
                {
                    "playerId": person.get("id"),
                    "name": person.get("fullName"),
                    "pos": pos,
                    "AB": stats.get("atBats"),
                    "R": stats.get("runs"),
                    "H": stats.get("hits"),
                    "RBI": stats.get("rbi"),
                    "BB": stats.get("baseOnBalls"),
                    "SO": stats.get("strikeOuts"),
                    "HR": stats.get("homeRuns"),
                    "battingOrder": bo,
                    "indent": indent,
                }
            )

        # Sort by batting order (subs sit under starter via 100/101/102 scheme)
        rows.sort(key=lambda r: (999999 if r.get("battingOrder") is None else r.get("battingOrder")))

        # Totals row
        def _sum_int(key):
            s = 0
            for r in rows:
                s += _safe_int(r.get(key), default=0)
            return s

        totals = {
            "playerId": None,
            "name": "Totals",
            "pos": "",
            "AB": _sum_int("AB"),
            "R": _sum_int("R"),
            "H": _sum_int("H"),
            "RBI": _sum_int("RBI"),
            "BB": _sum_int("BB"),
            "SO": _sum_int("SO"),
            "HR": _sum_int("HR"),
            "isTotals": True,
            "indent": False,
        }
        rows.append(totals)

        return rows

    def _extract_pitching(side_key: str):
        team = _safe(boxscore, "teams", side_key, default={}) or {}
        players = team.get("players") or {}

        # appearance order is usually in team['pitchers']
        order_ids = team.get("pitchers") or []
        seen = set()
        rows = []

        def _add(pid):
            if pid in seen:
                return
            seen.add(pid)
            pdata = players.get(f"ID{pid}") or {}
            person = pdata.get("person") or {}
            stats = _safe(pdata, "stats", "pitching", default={}) or {}
            # include only pitchers who threw at least one pitch
            pt = stats.get("pitchesThrown")
            ip = stats.get("inningsPitched")
            if _safe_int(pt, default=0) <= 0 and _ip_to_outs(ip) <= 0:
                return
            rows.append(
                {
                    "playerId": person.get("id"),
                    "name": person.get("fullName"),
                    "IP": stats.get("inningsPitched"),
                    "H": stats.get("hits"),
                    "R": stats.get("runs"),
                    "ER": stats.get("earnedRuns"),
                    "BB": stats.get("baseOnBalls"),
                    "SO": stats.get("strikeOuts"),
                    "HR": stats.get("homeRuns"),
                    "PS": f"{stats.get('pitchesThrown','-')}-{stats.get('strikes','-')}",
                    "_outs": _ip_to_outs(stats.get("inningsPitched")),
                    "_pitches": _safe_int(stats.get("pitchesThrown"), default=0),
                    "_strikes": _safe_int(stats.get("strikes"), default=0),
                }
            )

        for pid in order_ids:
            try:
                _add(int(pid))
            except Exception:
                continue

        # Add any missing pitchers who threw a pitch but weren't in order list
        for k, pdata in players.items():
            if not str(k).startswith("ID"):
                continue
            pid = _safe_int(str(k).replace("ID", ""), default=None)
            if pid is None:
                continue
            _add(pid)

        # Sort by outs if needed (fallback)
        # rows already mostly in appearance order; keep as-is

        # Totals
        outs = sum(r.get("_outs", 0) for r in rows)
        totals = {
            "playerId": None,
            "name": "Totals",
            "IP": _outs_to_ip(outs),
            "H": sum(_safe_int(r.get("H"), 0) for r in rows),
            "R": sum(_safe_int(r.get("R"), 0) for r in rows),
            "ER": sum(_safe_int(r.get("ER"), 0) for r in rows),
            "BB": sum(_safe_int(r.get("BB"), 0) for r in rows),
            "SO": sum(_safe_int(r.get("SO"), 0) for r in rows),
            "HR": sum(_safe_int(r.get("HR"), 0) for r in rows),
            "PS": f"{sum(r.get('_pitches',0) for r in rows)}-{sum(r.get('_strikes',0) for r in rows)}",
            "isTotals": True,
        }
        rows.append(totals)
        return rows

    if boxscore.get("teams"):
        game_obj["box"] = {
            "away": {"batting": _extract_batting("away"), "pitching": _extract_pitching("away")},
            "home": {"batting": _extract_batting("home"), "pitching": _extract_pitching("home")},
        }

    # ---------------------
    # plate appearances (PAS) for PBP tab
    # ---------------------
    pas_out = []
    for p in all_plays:
        about = p.get("about") or {}
        inning = _safe_int(about.get("inning"), default=None)
        half = _half_norm(about.get("halfInning"))
        summary_event = _safe(p, "result", "event", default=None) or _safe(p, "result", "eventType", default=None) or ""

        # --- build pitches list ---
        pitches_out = []
        pitch_n = 0

        for ev in (p.get("playEvents") or []):
            if ev.get("isPitch") is True:
                pitch_n += 1
            if ev.get("isPitch") is not True:
                continue

            details = ev.get("details") or {}
            pitch_data = ev.get("pitchData") or {}
            breaks = pitch_data.get("breaks") or {}

            px = _safe_float(pitch_data.get("coordinates", {}).get("pX") if isinstance(pitch_data.get("coordinates"), dict) else None, default=None)
            pz = _safe_float(pitch_data.get("coordinates", {}).get("pZ") if isinstance(pitch_data.get("coordinates"), dict) else None, default=None)

            sz_top = _safe_float(_safe(ev, "pitchData", "strikeZoneTop", default=None), default=None)
            sz_bot = _safe_float(_safe(ev, "pitchData", "strikeZoneBottom", default=None), default=None)

            call = _safe(details, "call", "description", default=None) or ""

            # boolean helpers
            is_ball = call.lower().startswith("ball")
            is_strike = "strike" in call.lower() or call.lower().startswith("foul")
            is_in_play = "in play" in call.lower()

            pitches_out.append(
                {
                    "n": pitch_n,
                    "px": px,
                    "pz": pz,
                    "sz_top": sz_top,
                    "sz_bot": sz_bot,
                    "call": call,
                    "typeCode": (str(_safe(details, "type", "code", default="") or "")).upper(),
                    "isBall": call in ("Ball", "Intent Ball", "Pitchout", "Hit By Pitch"),
                    "isStrike": call in ("Called Strike", "Swinging Strike", "Swinging Strike (Blocked)", "Foul Tip", "Missed Bunt", "Swinging Pitchout", "Foul Bunt", "Strike"),
                    "isFoul": call in ("Foul", "Foul Tip", "Foul Bunt"),
                    "isInPlay": call in ("In play, no out", "In play, out(s)", "In play, run(s)"),

                    "desc": _safe(details, "description", default=None) or "",
                    "isBall": is_ball,
                    "isStrike": is_strike,
                    "isInPlay": is_in_play,
                    # --- your table fields ---
                    "pitchType": _pretty_pitch_type(pitch_data, details),
                    "mph": _safe_float(pitch_data.get("startSpeed"), default=None),
                    "mphStyle": _grad_style(_safe_float(pitch_data.get("startSpeed"), default=None), PITCH_AVG.get((str(_safe(details, "type", "code", default="") or "")).upper(), 93.0), span=8.0),
                    "mphFire": (_safe_float(pitch_data.get("startSpeed"), default=None) is not None and _safe_float(pitch_data.get("startSpeed"), default=None) >= 98.0),
                    # ✅ Change 2: spin rate fallback (some feeds use pitchData.breaks.spinRate)
                    "spinRate": _safe_float(
                        pitch_data.get("spinRate") if pitch_data.get("spinRate") is not None else breaks.get("spinRate"),
                        default=None,
                    ),
                    "vertMove": _safe_float(breaks.get("breakVertical"), default=None),
                    "horizMove": _safe_float(breaks.get("breakHorizontal"), default=None),
                }
            )

        # ✅ Change 4: batted ball info (hitData is on a pitch event, not on the play)
        batted_ball = None
        hit_ev = None

        for ev in (p.get("playEvents") or []):
            if ev.get("isPitch") is not True:
                continue
            if isinstance(ev.get("hitData"), dict) and ev.get("hitData"):
                hit_ev = ev
                break

        if hit_ev is not None:
            hit = hit_ev.get("hitData") or {}
            evv = _safe_float(hit.get("launchSpeed"), default=None)
            la = _safe_float(hit.get("launchAngle"), default=None)
            spray = _safe_float(hit.get("sprayAngle"), default=None)

            xba = _safe_float(hit.get("estimatedBA"), default=None)
            if xba is None:
                xba = _safe_float(hit.get("estimatedBattingAverage"), default=None)

            coords = hit.get("coordinates") if isinstance(hit.get("coordinates"), dict) else {}
            coord_x = _safe_float(coords.get("coordX"), default=None)
            coord_y = _safe_float(coords.get("coordY"), default=None)

            batted_ball = {
                "exitVelo": evv,
                "launchAngle": la,
                "sprayAngle": spray,
                "xBA": xba,
                "distance": _safe_float(hit.get("totalDistance"), default=None),
                "evStyle": _grad_style(evv, 88.0, span=12.0),
                "evFire": (evv is not None and evv >= 100.0),
                "directionLabel": _spray_label(spray),
                "coordX": coord_x,
                "coordY": coord_y,
            }

        # ✅ IMPORTANT: matchup must be OUTSIDE the hit_ev / batted_ball block
        matchup = p.get("matchup") or {}
        batter_obj = matchup.get("batter") or {}
        pitcher_obj = matchup.get("pitcher") or {}

        pas_out.append(
            {
                "inning": inning,
                "half": half,
                "inningLabel": _inning_label(inning),
                "summaryEvent": summary_event,
                "event": summary_event,
                "description": _safe(p, "result", "description", default="") or "",
                "pitches": pitches_out,
                "battedBall": batted_ball,

                # ✅ add these for Overview tables/headshots
                "batterId": batter_obj.get("id"),
                "batterName": (batter_obj.get("fullName") or "").strip(),
                "pitcherId": pitcher_obj.get("id"),
                "pitcherName": (pitcher_obj.get("fullName") or "").strip(),
            }
        )



    game_obj["pas"] = pas_out

    # Keep legacy pbp as empty (template is using pas now)
    game_obj["pbp"] = []

    return game_obj




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
def _get_pp_obj_from_game(g: dict, side: str) -> dict:
    """
    side = "home" or "away"
    Try all known places schedule might put probables.
    Returns a dict that ideally includes name/id either top-level or under person.
    """
    teams = (g.get("teams") or {})
    team_blob = (teams.get(side) or {})

    # 1) The usual hydrated location
    probables = (g.get("probablePitchers") or {})
    pp = probables.get(side)
    if isinstance(pp, dict) and pp:
        return pp

    # 2) Sometimes schedule uses teams.home.probablePitcher
    pp2 = team_blob.get("probablePitcher")
    if isinstance(pp2, dict) and pp2:
        return pp2

    # 3) Sometimes schedule uses teams.home.probablePitcher (different capitalization / weirdness)
    # Keep as a safe fallback: nothing found
    return {}


def _pp_name_and_id(pp_obj: dict):
    """
    Returns (name, id) from either top-level or nested person.
    """
    if not pp_obj:
        return ("", None)

    person = pp_obj.get("person") or {}
    name = (pp_obj.get("fullName") or person.get("fullName") or person.get("name") or "").strip()
    pid = pp_obj.get("id") or person.get("id")
    return (name, pid)
    
def _pp_text(pp_obj: dict, season: int) -> str:
    if not pp_obj:
        return "PP: TBD"

    name, pid = _pp_name_and_id(pp_obj)
    if not name:
        return "PP: TBD"

    try:
        pid = int(pid) if pid is not None else None
    except Exception:
        pid = None

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
