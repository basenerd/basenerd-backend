
from flask import Flask, render_template, jsonify, request
import requests
import logging
import time
from datetime import datetime
try:
    import pytz
    ET_TZ = pytz.timezone("America/New_York")
except Exception:
    ET_TZ = None

app = Flask(__name__)

# ---- Logging ----
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("basenerd")

# ===========================
# STANDINGS (existing)
# ===========================
SEASON = 2025
BASE_URL = "https://statsapi.mlb.com/api/v1/standings"
HEADERS = {"User-Agent": "basenerd/1.0"}

LEAGUE_NAME = {103: "American League", 104: "National League"}
DIVISION_NAME = {
    200: "American League West",
    201: "American League East",
    202: "American League Central",
    203: "National League West",
    204: "National League East",
    205: "National League Central",
}
TEAM_ABBR = {
    109: "ARI", 144: "ATL", 110: "BAL", 111: "BOS", 112: "CHC", 145: "CHW", 113: "CIN",
    114: "CLE", 115: "COL", 116: "DET", 117: "HOU", 118: "KCR", 108: "LAA", 119: "LAD",
    146: "MIA", 158: "MIL", 142: "MIN", 121: "NYM", 147: "NYY", 133: "OAK", 143: "PHI",
    134: "PIT", 135: "SDP", 136: "SEA", 137: "SFG", 138: "STL", 139: "TBR", 140: "TEX",
    141: "TOR", 120: "WSH",
}
PRIMARY_PARAMS = {"leagueId": "103,104", "season": str(SEASON), "standingsTypes": "byDivision"}
FALLBACK_PARAMS = {"leagueId": "103,104", "season": str(SEASON)}

def fetch_standings():
    r = requests.get(BASE_URL, params=PRIMARY_PARAMS, headers=HEADERS, timeout=15)
    r.raise_for_status()
    data = r.json() or {}
    recs = data.get("records") or []
    if recs:
        return recs
    log.warning("Primary standings empty. Retrying without standingsTypes.")
    r2 = requests.get(BASE_URL, params=FALLBACK_PARAMS, headers=HEADERS, timeout=15)
    r2.raise_for_status()
    data2 = r2.json() or {}
    return data2.get("records") or []

def normalize_pct(pct_str):
    if pct_str in (None, ""): return 0.0
    try:
        s = str(pct_str).strip()
        return float("0" + s) if s.startswith(".") else float(s)
    except Exception:
        return 0.0

def get_last10(tr: dict) -> str:
    recs = (tr.get("records") or {}).get("splitRecords") or []
    for rec in recs:
        if rec.get("type") == "lastTen":
            return f"{rec.get('wins', 0)}-{rec.get('losses', 0)}"
    return ""

def hardcoded_abbr(team: dict) -> str:
    tid = team.get("id")
    if tid in TEAM_ABBR:
        return TEAM_ABBR[tid]
    name = (team.get("name") or "").replace(" ", "")
    return (name[:3] or "TBD").upper()

def simplify_standings(records):
    leagues = {"National League": [], "American League": []}
    total_rows = 0
    for block in (records or []):
        league_obj = block.get("league") or {}
        division_obj = block.get("division") or {}
        league_id = league_obj.get("id")
        division_id = division_obj.get("id")
        league_name = LEAGUE_NAME.get(league_id, league_obj.get("name") or "League")
        division_name = DIVISION_NAME.get(division_id, division_obj.get("name") or "Division")
        rows = []
        for tr in (block.get("teamRecords") or []):
            team = tr.get("team", {}) or {}
            rows.append({
                "team_name": team.get("name", "Team"),
                "team_abbr": hardcoded_abbr(team),
                "team_id": team.get("id"),
                "w": tr.get("wins"),
                "l": tr.get("losses"),
                "pct": normalize_pct(tr.get("winningPercentage")),
                "gb": tr.get("gamesBack"),
                "streak": (tr.get("streak") or {}).get("streakCode", ""),
                "last10": get_last10(tr),
                "runDiff": tr.get("runDifferential"),
            })
        if league_name in leagues:
            leagues[league_name].append({"division": division_name, "rows": rows})
        elif league_id == 103:
            leagues["American League"].append({"division": division_name, "rows": rows})
        elif league_id == 104:
            leagues["National League"].append({"division": division_name, "rows": rows})
        else:
            leagues["National League"].append({"division": division_name, "rows": rows})
        total_rows += len(rows)
    log.info("simplify_standings: NL_divs=%d AL_divs=%d total_rows=%d",
             len(leagues["National League"]), len(leagues["American League"]), total_rows)
    return leagues

# ===========================
# TODAY'S GAMES (with probables W-L/ERA) 
# ===========================
STATS = "https://statsapi.mlb.com/api/v1"
LIVE  = "https://statsapi.mlb.com/api/v1.1"

_GAMES_CACHE = {}       # date_str -> {ts, ttl, data}
_PITCHER_CACHE = {}     # (pitcher_id, season) -> {name, wins, losses, era}

def _to_et(iso_z: str) -> str:
    """Return an ET-formatted time like '10:40 PM ET' from a Zulu ISO string."""
    if not iso_z:
        return ""
    try:
        dt = datetime.fromisoformat(iso_z.replace("Z","+00:00"))
        if ET_TZ:
            dt = dt.astimezone(ET_TZ)
        # %-I not portable on Windows; try both
        try:
            s = dt.strftime("%-I:%M %p")
        except Exception:
            s = dt.strftime("%I:%M %p").lstrip("0")
        return f"{s} ET"
    except Exception as e:
        log.warning("time parse failed: %s", e)
        return ""

def games_fetch_schedule(date_str):
    url = f"{STATS}/schedule"
    params = {"sportId": 1, "date": date_str}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    return data.get("dates", [{}])[0].get("games", [])

def games_fetch_live(game_pk):
    r = requests.get(f"{LIVE}/game/{game_pk}/feed/live", timeout=15)
    r.raise_for_status()
    return r.json()

def fetch_pitcher_stats(pid: int, season: int):
    """Fetch pitcher W-L and ERA for a season and cache results."""
    if not pid:
        return None
    key = (pid, season)
    cached = _PITCHER_CACHE.get(key)
    if cached:
        return cached
    url = f"{STATS}/people/{pid}"
    # hydrate season stats
    params = {"hydrate": f"stats(group=pitching,type=season,season={season})"}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json() or {}
        people = data.get("people") or []
        if not people:
            return None
        p = people[0]
        name = p.get("fullName") or ""
        stats_groups = p.get("stats") or []
        wins = losses = None
        era = None
        for grp in stats_groups:
            if (grp.get("group") or {}).get("displayName","").lower() == "pitching" or grp.get("group") == "pitching":
                splits = grp.get("splits") or []
                if splits:
                    stat = splits[0].get("stat") or {}
                    wins = stat.get("wins")
                    losses = stat.get("losses")
                    era = stat.get("era")
                    break
        result = {"name": name, "wins": wins, "losses": losses, "era": era}
        _PITCHER_CACHE[key] = result
        return result
    except Exception as e:
        log.warning("pitcher stats fetch failed for %s: %s", pid, e)
        return None

def _linescore_blob(ls):
    if not ls: return None
    return {
        "awayByInning": [i.get("away", "") for i in ls.get("innings", [])],
        "homeByInning": [i.get("home", "") for i in ls.get("innings", [])],
        "awayRuns": ls.get("teams", {}).get("away", {}).get("runs"),
        "homeRuns": ls.get("teams", {}).get("home", {}).get("runs"),
        "awayHits": ls.get("teams", {}).get("away", {}).get("hits"),
        "homeHits": ls.get("teams", {}).get("home", {}).get("hits"),
        "awayErrors": ls.get("teams", {}).get("away", {}).get("errors"),
        "homeErrors": ls.get("teams", {}).get("home", {}).get("errors"),
    }

def _team_blob(gameData, linescore, side):
    t = gameData.get("teams", {}).get(side, {}) or {}
    ls = (linescore or {}).get("teams", {}).get(side, {}) or {}
    return {
        "id": t.get("id"),
        "abbr": t.get("abbreviation") or t.get("clubName"),
        "name": t.get("name"),
        "score": ls.get("runs"),
        "hits": ls.get("hits"),
        "errors": ls.get("errors"),
    }

def games_live_to_view(live, schedule_piece=None):
    """Shape a single game's live feed into the UI view model, using schedule for start time."""
    gameData = live.get("gameData", {})
    liveData = live.get("liveData", {})
    status = gameData.get("status", {})
    ls = liveData.get("linescore", {}) or {}

    abstract = (status.get("abstractGameState") or "").upper()  # PREVIEW, LIVE, FINAL
    if abstract not in ("PREVIEW","LIVE","FINAL"):
        abstract = "PREVIEW"

    gamePk = gameData.get("game", {}).get("pk") or live.get("gamePk")
    # Use schedule.gameDate if available; else gameData.datetime.dateTime
    start_iso = (schedule_piece or {}).get("gameDate") or gameData.get("datetime", {}).get("dateTime")
    start_local = _to_et(start_iso)

    venue = (gameData.get("venue") or {}).get("name")

    # in-progress bits
    ip = None
    if abstract == "LIVE":
        ip = {
            "inning": ls.get("currentInning"),
            "half": "T" if ls.get("isTopInning") else "B",
            "outs": ls.get("outs"),
            "balls": ls.get("balls"),
            "strikes": ls.get("strikes"),
            "runners": {
                "first": bool(ls.get("offense", {}).get("first")),
                "second": bool(ls.get("offense", {}).get("second")),
                "third": bool(ls.get("offense", {}).get("third")),
            }
        }

    dec = liveData.get("decisions", {}) or {}
    fin = None
    if abstract == "FINAL":
        fin = {
            "winningPitcher": (dec.get("winner", {}) or {}).get("fullName"),
            "losingPitcher": (dec.get("loser", {}) or {}).get("fullName"),
            "savePitcher": (dec.get("save", {}) or {}).get("fullName"),
            "linescore": _linescore_blob(ls)
        }

    # Probables (with W-L, ERA)
    prob_away = (gameData.get("probablePitchers") or {}).get("away") or (gameData.get("teams", {}).get("away", {}) or {}).get("probablePitcher") or {}
    prob_home = (gameData.get("probablePitchers") or {}).get("home") or (gameData.get("teams", {}).get("home", {}) or {}).get("probablePitcher") or {}
    away_stats = fetch_pitcher_stats(prob_away.get("id"), SEASON) if prob_away else None
    home_stats = fetch_pitcher_stats(prob_home.get("id"), SEASON) if prob_home else None

    def fmt_prob(stats, fallback_name):
        if not stats and not fallback_name:
            return ""
        name = (stats or {}).get("name") or fallback_name or ""
        w = (stats or {}).get("wins")
        l = (stats or {}).get("losses")
        era = (stats or {}).get("era")
        parts = [name]
        wl = (f"{w}-{l}" if (w is not None and l is not None) else None)
        if wl: parts.append(wl)
        if era: parts.append(f"{era} ERA")
        return " • ".join(parts)

    probables_text = None
    if abstract == "PREVIEW":
        pa = fmt_prob(away_stats, prob_away.get("fullName") if prob_away else "")
        ph = fmt_prob(home_stats, prob_home.get("fullName") if prob_home else "")
        if pa or ph:
            probables_text = f"{pa or 'TBD'} vs {ph or 'TBD'}"

    # Build teams
    away_team = _team_blob(gameData, ls, "away")
    home_team = _team_blob(gameData, ls, "home")

    # suppress scores for scheduled
    if abstract == "PREVIEW":
        away_team["score"] = None
        home_team["score"] = None

    return {
        "gamePk": gamePk,
        "status": "in_progress" if abstract == "LIVE" else ("final" if abstract == "FINAL" else "scheduled"),
        "startTimeLocal": start_local,
        "venue": venue,
        "teams": {"away": away_team, "home": home_team},
        "inProgress": ip,
        "final": fin,
        "probablesText": probables_text
    }

def cache_get(key):
    x = _GAMES_CACHE.get(key)
    if not x: return None
    if time.time() - x["ts"] > x["ttl"]:
        return None
    return x["data"]

def cache_set(key, data, ttl):
    _GAMES_CACHE[key] = {"ts": time.time(), "ttl": ttl, "data": data}

# ===========================
# ROUTES
# ===========================

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/standings")
def standings_page():
    try:
        records = fetch_standings()
        data = simplify_standings(records)
        if "National League" not in data: data["National League"] = []
        if "American League" not in data: data["American League"] = []
        return render_template("standings.html", data=data, season=SEASON)
    except Exception as e:
        log.exception("Failed to fetch standings")
        safe = {"National League": [], "American League": []}
        return render_template("standings.html", data=safe, season=SEASON, error=str(e)), 200

@app.route("/debug/standings.json")
def debug_standings():
    try:
        return jsonify({"records": fetch_standings()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/todaysgames")
def todays_games_page():
    return render_template("todaysgames.html")

@app.route("/api/games")
def api_games():
    date_str = request.args.get("date")
    if not date_str:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
    cached = cache_get(date_str)
    if cached:
        return jsonify(cached)

    try:
        schedule = games_fetch_schedule(date_str)
    except Exception as e:
        data = {"date": date_str, "games": [], "error": f"schedule_error: {e}"}
        cache_set(date_str, data, 30)
        return jsonify(data), 502

    games = []
    any_live = False
    for g in schedule:
        pk = g.get("gamePk")
        if not pk: continue
        try:
            live = games_fetch_live(pk)
            view = games_live_to_view(live, schedule_piece=g)
            any_live = any_live or (view["status"] == "in_progress")
            games.append(view)
        except Exception as ex:
            log.warning("live fetch failed for %s: %s", pk, ex)
            continue

    payload = {"date": date_str, "games": games}
    cache_set(date_str, payload, 15 if any_live else 300)
    return jsonify(payload)

@app.route("/ping")
def ping():
    return "ok", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
