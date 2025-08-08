from flask import Flask, render_template, jsonify, request
import requests
import logging
import time
from datetime import datetime
try:
    import pytz
    PHOENIX = pytz.timezone("America/Phoenix")
except Exception:
    PHOENIX = None

from datetime import timezone

def _parse_iso_utc(iso_str):
    try:
        # gameDate is like '2025-08-08T23:05:00Z' -> ensure tz-aware UTC
        if iso_str.endswith('Z'):
            return datetime.fromisoformat(iso_str.replace('Z','+00:00'))
        dt = datetime.fromisoformat(iso_str)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

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
# TODAY'S GAMES (new)
# ===========================
STATS = "https://statsapi.mlb.com/api/v1"
LIVE  = "https://statsapi.mlb.com/api/v1.1"

# tiny cache for the games feed
_GAMES_CACHE = {}  # key: date_str -> {"ts": epoch, "ttl": sec, "data": {...}}

def _fmt_local_time(iso_z):
    try:
        dt = _parse_iso_utc(iso_z)
        if not dt:
            return ""
        if PHOENIX:
            dt = dt.astimezone(PHOENIX)
        else:
            dt = dt.astimezone()  # local system tz
        return dt.strftime("%-I:%M %p")
    except Exception:
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

def _record_str(team_obj):
    rec = team_obj.get("record", {}) if isinstance(team_obj, dict) else {}
    w = rec.get("wins"); l = rec.get("losses")
    return f"{w}-{l}" if (w is not None and l is not None) else ""

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

def games_live_to_view(live, schedule_start_iso=None):
    gameData = live.get("gameData", {})
    liveData = live.get("liveData", {})
    status = gameData.get("status", {})
    ls = liveData.get("linescore", {}) or {}

    abstract = (status.get("abstractGameState") or "").upper()  # PREVIEW, LIVE, FINAL
    if abstract not in ("PREVIEW","LIVE","FINAL"):
        abstract = "PREVIEW"

    gamePk = gameData.get("game", {}).get("pk") or live.get("gamePk")
    start_iso = gameData.get("datetime", {}).get("dateTime")
    # prefer schedule's UTC gameDate when present
    if schedule_start_iso:
        start_iso = schedule_start_iso
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

        # Build team blobs
    away_blob = _team_blob(gameData, ls, "away")
    home_blob = _team_blob(gameData, ls, "home")
    # attach records if available
    away_blob["record"] = _record_str(gameData.get("teams", {}).get("away", {}))
    home_blob["record"] = _record_str(gameData.get("teams", {}).get("home", {}))
    # If scheduled, suppress scores so UI won't show 0-0
    if abstract == "PREVIEW":
        away_blob["score"] = None
        home_blob["score"] = None
    return {
        "gamePk": gamePk,
        "status": "in_progress" if abstract == "LIVE" else ("final" if abstract == "FINAL" else "scheduled"),
        "startTimeUtc": start_iso,
        "startTimeLocal": _fmt_local_time(start_iso) if start_iso else "",
        "venue": venue,
        "teams": {"away": away_blob, "home": home_blob},
        "inProgress": ip,
        "final": fin
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
def standings():
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

# ---- Today's Games page (template) ----
@app.route("/todaysgames")
def todays_games_page():
    return render_template("todaysgames.html")

# ---- Today's Games API ----
@app.route("/api/games")
def api_games():
    date_str = request.args.get("date")
    if not date_str:
        if PHOENIX:
            date_str = datetime.now(PHOENIX).strftime("%Y-%m-%d")
        else:
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
        if not pk:
            continue
        try:
            live = games_fetch_live(pk)
            sched_start = g.get("gameDate")  # UTC 'Z'
            view = games_live_to_view(live, schedule_start_iso=sched_start)
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

# ---- Local dev entrypoint ----
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
