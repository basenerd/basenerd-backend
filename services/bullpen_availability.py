# services/bullpen_availability.py
"""
Estimate bullpen availability based on recent pitching workload.

For each team, looks at the last 4 days of games and tracks how many
pitches each reliever threw and how many days ago. Combines workload
and rest into an availability status.
"""

from __future__ import annotations
import json
import logging
import urllib.request
from datetime import datetime, timedelta

from services.mlb_api import get_player_headshot_url

log = logging.getLogger(__name__)

# Module cache: (team_id, game_date_str) → bullpen list
_cache: dict = {}

# Availability tiers
AVAILABLE = "available"
LIMITED = "limited"
UNLIKELY = "unlikely"
UNAVAILABLE = "unavailable"


def _fetch_team_recent_games(team_id, game_date_str, lookback_days=4):
    """Fetch recent completed games for a team from MLB schedule API."""
    try:
        dt = datetime.strptime(game_date_str, "%Y-%m-%d")
    except (ValueError, TypeError):
        dt = datetime.utcnow()

    end = dt - timedelta(days=1)  # yesterday
    start = dt - timedelta(days=lookback_days)

    url = (
        f"https://statsapi.mlb.com/api/v1/schedule"
        f"?sportId=1&teamId={team_id}"
        f"&startDate={start.strftime('%Y-%m-%d')}"
        f"&endDate={end.strftime('%Y-%m-%d')}"
        f"&hydrate=linescore"
    )
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())

    games = []
    for d in data.get("dates") or []:
        for g in d.get("games") or []:
            status = (g.get("status") or {}).get("abstractGameState", "")
            if status.lower() == "final":
                games.append({
                    "game_pk": g.get("gamePk"),
                    "date": d.get("date"),
                    "home_id": (g.get("teams", {}).get("home", {}).get("team", {}) or {}).get("id"),
                    "away_id": (g.get("teams", {}).get("away", {}).get("team", {}) or {}).get("id"),
                })
    return games


def _fetch_pitching_from_boxscore(game_pk, team_id):
    """
    Fetch pitcher stats from a game's boxscore.
    Returns list of {id, name, pitches, outs, is_starter}.
    """
    url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())

    boxscore = (data.get("liveData") or {}).get("boxscore") or {}
    teams = boxscore.get("teams") or {}

    # Determine which side this team is on
    home_team = teams.get("home") or {}
    away_team = teams.get("away") or {}
    home_id = ((home_team.get("team") or {}).get("id"))
    away_id = ((away_team.get("team") or {}).get("id"))

    if team_id == home_id:
        team_box = home_team
    elif team_id == away_id:
        team_box = away_team
    else:
        return []

    players = team_box.get("players") or {}
    pitcher_order = team_box.get("pitchers") or []  # ordered list of pitcher IDs

    starter_id = pitcher_order[0] if pitcher_order else None
    all_players = (data.get("gameData") or {}).get("players") or {}

    result = []
    for pid in pitcher_order:
        pid = int(pid)
        key = f"ID{pid}"
        pdata = players.get(key) or {}
        stats = (pdata.get("stats") or {}).get("pitching") or {}

        # Get name from gameData.players
        person = (all_players.get(key) or {})
        name = person.get("fullName") or person.get("lastFirstName") or str(pid)

        pitches = stats.get("numberOfPitches") or stats.get("pitchesThrown") or 0
        ip_str = stats.get("inningsPitched", "0")

        # Convert IP string to outs
        try:
            parts = str(ip_str).split(".")
            outs = int(parts[0]) * 3 + (int(parts[1]) if len(parts) > 1 else 0)
        except (ValueError, IndexError):
            outs = 0

        result.append({
            "id": pid,
            "name": name,
            "pitches": int(pitches),
            "outs": outs,
            "is_starter": (pid == starter_id),
        })

    return result


def _compute_availability(appearances):
    """
    Given a list of recent appearances [{days_ago, pitches, outs}],
    compute availability status and a workload score (0-100).

    Rules based on standard MLB bullpen management:
    - Pitched today (days_ago=0): UNAVAILABLE
    - 1 day rest + high pitch count (30+): UNLIKELY
    - 1 day rest + moderate (15-29): LIMITED
    - 1 day rest + low (<15): AVAILABLE
    - 2 days rest: AVAILABLE (unless very heavy workload)
    - 3+ days rest: fully AVAILABLE
    - Pitched 3 of last 4 days: UNLIKELY regardless
    - Pitched 2 consecutive days: at least LIMITED
    """
    if not appearances:
        return AVAILABLE, 0, "Fully rested"

    days_pitched = set(a["days_ago"] for a in appearances)
    total_pitches_3d = sum(a["pitches"] for a in appearances if a["days_ago"] <= 3)
    total_apps_4d = len(appearances)
    most_recent = min(a["days_ago"] for a in appearances)
    most_recent_pitches = max(
        (a["pitches"] for a in appearances if a["days_ago"] == most_recent), default=0
    )

    # Workload score: 0 = fully rested, 100 = completely gassed
    workload = 0

    for a in appearances:
        day_weight = {1: 1.0, 2: 0.6, 3: 0.3, 4: 0.15}.get(a["days_ago"], 0.1)
        workload += a["pitches"] * day_weight

    # Normalize: 30 pitches yesterday = ~30, 60 pitches over 2 days = ~42
    workload = min(100, workload * 1.2)

    # Consecutive day penalty
    consecutive_days = 0
    for d in range(1, 5):
        if d in days_pitched:
            consecutive_days += 1
        else:
            break

    # Determine status
    if most_recent == 0:
        return UNAVAILABLE, 100, "Pitched today"

    if total_apps_4d >= 3:
        return UNLIKELY, min(100, workload + 20), f"Pitched {total_apps_4d}x in last 4 days"

    if consecutive_days >= 2 and most_recent_pitches >= 20:
        return UNLIKELY, min(100, workload + 15), f"{consecutive_days} consecutive days, {most_recent_pitches}P last"

    if consecutive_days >= 2:
        return LIMITED, min(100, workload + 10), f"{consecutive_days} consecutive days"

    if most_recent == 1:
        if most_recent_pitches >= 30:
            return UNLIKELY, min(100, workload), f"{most_recent_pitches}P yesterday"
        elif most_recent_pitches >= 15:
            return LIMITED, min(100, workload), f"{most_recent_pitches}P yesterday"
        else:
            return AVAILABLE, min(100, workload), f"{most_recent_pitches}P yesterday"

    if most_recent == 2:
        if total_pitches_3d >= 50:
            return LIMITED, min(100, workload), f"{total_pitches_3d}P last 3 days"
        return AVAILABLE, min(100, workload), f"2 days rest"

    return AVAILABLE, max(0, workload), f"{most_recent} days rest"


def get_bullpen_availability(team_id, game_date_str, team_abbrev=""):
    """
    Compute bullpen availability for a team heading into a game.

    Returns list of dicts sorted by availability, each with:
      id, name, headshot, status, workload, note, appearances[]
    """
    cache_key = (team_id, game_date_str)
    if cache_key in _cache:
        return _cache[cache_key]

    try:
        recent_games = _fetch_team_recent_games(team_id, game_date_str)
    except Exception as e:
        log.warning("Failed to fetch recent games for team %s: %s", team_id, e)
        return []

    # For each game, get pitching boxscore
    # {pitcher_id: [{days_ago, pitches, outs}]}
    pitcher_appearances: dict[int, list] = {}
    pitcher_names: dict[int, str] = {}
    starter_ids: set[int] = set()

    try:
        game_dt = datetime.strptime(game_date_str, "%Y-%m-%d")
    except (ValueError, TypeError):
        game_dt = datetime.utcnow()

    for game in recent_games:
        try:
            game_date = datetime.strptime(game["date"], "%Y-%m-%d")
            days_ago = (game_dt - game_date).days
            if days_ago < 1:
                continue

            pitchers = _fetch_pitching_from_boxscore(game["game_pk"], team_id)
            for p in pitchers:
                pid = p["id"]
                pitcher_names[pid] = p["name"]
                if p["is_starter"]:
                    starter_ids.add(pid)
                else:
                    pitcher_appearances.setdefault(pid, []).append({
                        "days_ago": days_ago,
                        "pitches": p["pitches"],
                        "outs": p["outs"],
                    })
        except Exception as e:
            log.warning("Failed to fetch boxscore for game %s: %s", game.get("game_pk"), e)
            continue

    # Build bullpen list (exclude starters unless they also appeared in relief)
    bullpen = []
    for pid, apps in pitcher_appearances.items():
        if pid in starter_ids and len(apps) == 0:
            continue  # pure starter, skip

        status, workload, note = _compute_availability(apps)

        # Build appearance summary
        app_summary = []
        for a in sorted(apps, key=lambda x: x["days_ago"]):
            app_summary.append({
                "days_ago": a["days_ago"],
                "pitches": a["pitches"],
                "outs": a["outs"],
            })

        headshot = None
        try:
            headshot = get_player_headshot_url(pid, size=60)
        except Exception:
            pass

        bullpen.append({
            "id": pid,
            "name": pitcher_names.get(pid, str(pid)),
            "headshot": headshot,
            "status": status,
            "workload": round(workload),
            "note": note,
            "appearances": app_summary,
            "total_pitches_4d": sum(a["pitches"] for a in apps),
            "last_pitched_days_ago": min(a["days_ago"] for a in apps) if apps else None,
        })

    # If no appearance data (early season, off days, etc.), fall back to active roster
    if not bullpen:
        bullpen = _fetch_roster_fallback(team_id)

    # Sort: available first, then limited, then unlikely, then unavailable
    status_order = {AVAILABLE: 0, LIMITED: 1, UNLIKELY: 2, UNAVAILABLE: 3}
    bullpen.sort(key=lambda x: (status_order.get(x["status"], 4), x["workload"]))

    _cache[cache_key] = bullpen
    return bullpen


def _fetch_roster_fallback(team_id):
    """
    Fallback: fetch active roster and return all relief pitchers as fully available.
    Used when no recent appearance data exists (early season, off days, etc.).
    """
    try:
        url = (
            f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"
            f"?rosterType=active&hydrate=person"
        )
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read())

        roster = data.get("roster") or []
        bullpen = []
        for entry in roster:
            pos = (entry.get("position") or {}).get("abbreviation", "")
            # Include pitchers who are not starters (RP, CP, etc.)
            if pos not in ("SP",):
                person = entry.get("person") or {}
                pid = person.get("id")
                name = person.get("fullName") or str(pid)
                if not pid:
                    continue
                headshot = None
                try:
                    headshot = get_player_headshot_url(pid, size=60)
                except Exception:
                    pass
                bullpen.append({
                    "id": pid,
                    "name": name,
                    "headshot": headshot,
                    "status": AVAILABLE,
                    "workload": 0,
                    "note": "No recent appearances",
                    "appearances": [],
                    "total_pitches_4d": 0,
                    "last_pitched_days_ago": None,
                })
        return bullpen
    except Exception as e:
        log.warning("Roster fallback failed for team %s: %s", team_id, e)
        return []
