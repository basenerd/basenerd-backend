from flask import Flask, render_template, jsonify, request
import requests, logging, time
from datetime import datetime, timezone
try:
    import pytz
    ET_TZ = pytz.timezone("America/New_York")
except Exception:
    ET_TZ = None

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("basenerd")

# ===========================
# STANDINGS (kept as-is)
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
    if recs: return recs
    r2 = requests.get(BASE_URL, params=FALLBACK_PARAMS, headers=HEADERS, timeout=15)
    r2.raise_for_status()
    data2 = r2.json() or {}
    return data2.get("records") or []

def normalize_pct(pct_str):
    if pct_str in (None, ""): return 0.0
    try:
        s = str(pct_str).strip()
        return float("0"+s) if s.startswith(".") else float(s)
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
    if tid in TEAM_ABBR: return TEAM_ABBR[tid]
    name = (team.get("name") or "").replace(" ", "")
    return (name[:3] or "TBD").upper()

def simplify_standings(records):
    leagues = {"National League": [], "American League": []}
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
    return leagues

# ===========================
# TODAY'S GAMES (ET + probables with stats)
# ===========================
STATS = "https://statsapi.mlb.com/api/v1"
LIVE  = "https://statsapi.mlb.com/api/v1.1"

_GAMES_CACHE = {}          # date -> payload
_PITCHER_CACHE = {}        # (id, season) -> {name,w,l,era}

def _parse_iso_utc(iso_str: str):
    if not iso_str: return None
    try:
        if iso_str.endswith('Z'):
            dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        else:
            dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def _fmt_et(iso_utc):
    dt = _parse_iso_utc(iso_utc)
    if not dt: return ""
    if ET_TZ:
        dt = dt.astimezone(ET_TZ)
    return dt.strftime("%-I:%M %p ET")

def _fetch_schedule(date_str):
    r = requests.get(f"{STATS}/schedule", params={"sportId":1, "date":date_str}, timeout=15)
    r.raise_for_status()
    return r.json().get("dates", [{}])[0].get("games", [])

def _fetch_live(game_pk):
    r = requests.get(f"{LIVE}/game/{game_pk}/feed/live", timeout=15)
    r.raise_for_status()
    return r.json()

def _fetch_pitcher_stats(pid, season):
    key = (pid, season)
    cached = _PITCHER_CACHE.get(key)
    if cached and (time.time() - cached["ts"] < 6*3600):
        return cached["data"]
    try:
        url = f"{STATS}/people/{pid}"
        params = {"hydrate": f"stats(group=pitching,type=season,season={season})"}
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        ppl = r.json().get("people", [])
        name = ""
        w = l = None
        era = None
        if ppl:
            p = ppl[0]
            name = p.get("fullName") or p.get("lastFirstName") or p.get("firstLastName") or ""
            stats = (p.get("stats") or [])
            if stats and stats[0].get("splits"):
                s = stats[0]["splits"][0].get("stat", {})
                w = s.get("wins"); l = s.get("losses")
                era = s.get("era")
        data = {"name": name, "wins": w, "losses": l, "era": era}
        _PITCHER_CACHE[key] = {"ts": time.time(), "data": data}
        return data
    except Exception:
        return {"name":"", "wins":None, "losses":None, "era":None}

def _team_block(gameData, linescore, side, season):
    t = (gameData.get("teams") or {}).get(side, {}) or {}
    ls = (linescore or {}).get("teams", {}).get(side, {}) or {}
    abbr = t.get("abbreviation") or t.get("clubName")
    tid = t.get("id")
    # probable
    prob = (gameData.get("probablePitchers") or {}).get(side, {}) or {}
    if not prob:
        prob = ((gameData.get("teams") or {}).get(side, {}) or {}).get("probablePitcher") or {}
    probable = {"text":"", "name":"", "wins":None, "losses":None, "era":None}
    pid = prob.get("id")
    pname = prob.get("fullName") or prob.get("lastFirstName") or prob.get("firstLastName")
    if pid:
        ps = _fetch_pitcher_stats(pid, SEASON)
        probable.update(ps)
    elif pname:
        probable["name"] = pname
    # Build display text
    parts = []
    if probable["name"]: parts.append(probable["name"])
    wl = None
    if probable["wins"] is not None and probable["losses"] is not None:
        wl = f"{probable['wins']}-{probable['losses']}"
    era = probable.get("era")
    if wl and era: parts.append(f"{wl} • {era} ERA")
    elif wl: parts.append(wl)
    elif era: parts.append(f"{era} ERA")
    probable["text"] = " • ".join(parts)

    return {
        "id": tid,
        "abbr": abbr,
        "name": t.get("name"),
        "score": ls.get("runs"),
        "hits": ls.get("hits"),
        "errors": ls.get("errors"),
        "probable": probable,
    }

def _linescore_blob(ls):
    if not ls: return None
    return {
        "awayRuns": ls.get("teams", {}).get("away", {}).get("runs"),
        "homeRuns": ls.get("teams", {}).get("home", {}).get("runs"),
        "awayHits": ls.get("teams", {}).get("away", {}).get("hits"),
        "homeHits": ls.get("teams", {}).get("home", {}).get("hits"),
        "awayErrors": ls.get("teams", {}).get("away", {}).get("errors"),
        "homeErrors": ls.get("teams", {}).get("home", {}).get("errors"),
    }

def _shape_game(live, schedule_start_iso):
    gd = live.get("gameData", {}) or {}
    ld = live.get("liveData", {}) or {}
    status = gd.get("status", {}) or {}
    ls = ld.get("linescore", {}) or {}

    abstract = (status.get("abstractGameState") or "").upper()  # PREVIEW | LIVE | FINAL
    if abstract not in ("PREVIEW","LIVE","FINAL"): abstract = "PREVIEW"

    gamePk = gd.get("game", {}).get("pk") or live.get("gamePk")
    venue = (gd.get("venue") or {}).get("name")

    # start time strictly from SCHEDULE
    start_iso = schedule_start_iso

    # in-progress details for banner only
    ip = None
    if abstract == "LIVE":
        ip = {
            "inning": ls.get("currentInning"),
            "half": "T" if ls.get("isTopInning") else "B",
            "outs": ls.get("outs"),
            "balls": ls.get("balls"),
            "strikes": ls.get("strikes"),
        }

    # final details
    dec = ld.get("decisions", {}) or {}
    fin = None
    if abstract == "FINAL":
        fin = {
            "winningPitcher": (dec.get("winner", {}) or {}).get("fullName"),
            "losingPitcher": (dec.get("loser", {}) or {}).get("fullName"),
            "savePitcher": (dec.get("save", {}) or {}).get("fullName"),
            "linescore": _linescore_blob(ls)
        }

    away = _team_block(gd, ls, "away", SEASON)
    home = _team_block(gd, ls, "home", SEASON)

    # For scheduled games, hide 0-0 scores
    if abstract == "PREVIEW":
        away["score"] = None
        home["score"] = None

    return {
        "gamePk": gamePk,
        "status": "in_progress" if abstract == "LIVE" else ("final" if abstract == "FINAL" else "scheduled"),
        "startTimeLocal": _fmt_et(start_iso) if start_iso else "",
        "venue": venue,
        "teams": {"away": away, "home": home},
        "inProgress": ip,
        "final": fin
    }

def cache_get(key):
    x = _GAMES_CACHE.get(key)
    if not x: return None
    if time.time() - x["ts"] > x["ttl"]: return None
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

@app.route("/todaysgames")
def todays_games_page():
    return render_template("todaysgames.html")

@app.route("/api/games")
def api_games():
    date_str = request.args.get("date")
    if not date_str:
        if ET_TZ:
            date_str = datetime.now(ET_TZ).strftime("%Y-%m-%d")
        else:
            date_str = datetime.utcnow().strftime("%Y-%m-%d")

    cached = cache_get(date_str)
    if cached:
        return jsonify(cached)

    try:
        sched = _fetch_schedule(date_str)
    except Exception as e:
        data = {"date": date_str, "games": [], "error": f"schedule_error: {e}"}
        cache_set(date_str, data, 30)
        return jsonify(data), 502

    games = []
    any_live = False
    for s in sched:
        pk = s.get("gamePk")
        start_iso = s.get("gameDate")  # UTC
        if not pk: continue
        try:
            live = _fetch_live(pk)
            shaped = _shape_game(live, start_iso)
            any_live = any_live or (shaped["status"] == "in_progress")
            games.append(shaped)
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
