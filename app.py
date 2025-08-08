
from flask import Flask, render_template, jsonify, request
import requests, logging, time
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
# STANDINGS (existing baseline)
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
PRIMARY_PARAMS = {"leagueId":"103,104", "season":str(SEASON), "standingsTypes":"byDivision"}
FALLBACK_PARAMS = {"leagueId":"103,104", "season":str(SEASON)}

def fetch_standings():
    r = requests.get(BASE_URL, params=PRIMARY_PARAMS, headers=HEADERS, timeout=15)
    r.raise_for_status()
    data = r.json() or {}
    recs = data.get("records") or []
    if recs:
        return recs
    r2 = requests.get(BASE_URL, params=FALLBACK_PARAMS, headers=HEADERS, timeout=15)
    r2.raise_for_status()
    return (r2.json() or {}).get("records") or []

def normalize_pct(pct_str):
    if pct_str in (None, ""): return 0.0
    try:
        s = str(pct_str).strip()
        return float("0"+s) if s.startswith(".") else float(s)
    except Exception:
        return 0.0

def get_last10(tr):
    recs = (tr.get("records") or {}).get("splitRecords") or []
    for rec in recs:
        if rec.get("type") == "lastTen":
            return f"{rec.get('wins',0)}-{rec.get('losses',0)}"
    return ""

def hardcoded_abbr(team):
    tid = team.get("id")
    if tid in TEAM_ABBR: return TEAM_ABBR[tid]
    name = (team.get("name") or "").replace(" ","")
    return (name[:3] or "TBD").upper()

def simplify_standings(records):
    leagues = {"National League": [], "American League": []}
    for block in (records or []):
        league_obj = block.get("league") or {}
        division_obj = block.get("division") or {}
        league_id = league_obj.get("id")
        league_name = LEAGUE_NAME.get(league_id, league_obj.get("name") or "League")
        division_name = DIVISION_NAME.get(division_obj.get("id"), division_obj.get("name") or "Division")
        rows = []
        for tr in (block.get("teamRecords") or []):
            team = tr.get("team", {}) or {}
            rows.append({
                "team_name": team.get("name","Team"),
                "team_abbr": hardcoded_abbr(team),
                "team_id": team.get("id"),
                "w": tr.get("wins"),
                "l": tr.get("losses"),
                "pct": normalize_pct(tr.get("winningPercentage")),
                "gb": tr.get("gamesBack"),
                "streak": (tr.get("streak") or {}).get("streakCode",""),
                "last10": get_last10(tr),
                "runDiff": tr.get("runDifferential"),
            })
        if league_name in leagues:
            leagues[league_name].append({"division": division_name, "rows": rows})
        elif league_id == 103:
            leagues["American League"].append({"division": division_name, "rows": rows})
        else:
            leagues["National League"].append({"division": division_name, "rows": rows})
    return leagues

# ===========================
# TODAY'S GAMES (ET, probables, live batter/pitcher + box score)
# ===========================
STATS = "https://statsapi.mlb.com/api/v1"
LIVE  = "https://statsapi.mlb.com/api/v1.1"

_PITCHER_CACHE = {}   # id -> {"ts": epoch, "ttl": sec, "data": {name, wl, era}}
_GAMES_CACHE = {}     # date -> cached payload

def _fmt_et_from_iso(iso_z):
    try:
        dt = datetime.fromisoformat(iso_z.replace("Z","+00:00"))
        if ET_TZ: dt = dt.astimezone(ET_TZ)
        return dt.strftime("%-I:%M %p") + " ET"
    except Exception:
        return ""

def _fetch_schedule(date_str):
    r = requests.get(f"{STATS}/schedule", params={"sportId":1,"date":date_str}, timeout=15)
    r.raise_for_status()
    data = r.json()
    games = data.get("dates", [{}])[0].get("games", [])
    return { g.get("gamePk"): g.get("gameDate") for g in games }

def _fetch_live(game_pk):
    r = requests.get(f"{LIVE}/game/{game_pk}/feed/live", timeout=15)
    r.raise_for_status()
    return r.json()

def _fetch_pitcher_season_line(pid, season):
    now = time.time()
    c = _PITCHER_CACHE.get(pid)
    if c and now - c["ts"] < c["ttl"]:
        return c["data"]
    url = f"{STATS}/people/{pid}"
    params = {"hydrate": f"stats(group=pitching,type=season,season={season})"}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    ppl = (data.get("people") or [{}])[0]
    name = ppl.get("fullName") or ppl.get("lastFirstName") or ppl.get("firstLastName") or ""
    era = None; w = None; l = None
    for st in (ppl.get("stats") or []):
        if st.get("group","").get("displayName") == "pitching" or st.get("group") == "pitching":
            splits = st.get("splits") or []
            if splits:
                stat = splits[0].get("stat") or {}
                era = stat.get("era")
                w = stat.get("wins"); l = stat.get("losses")
                break
    line = {"name": name, "wl": (f"{w}-{l}" if w is not None and l is not None else None), "era": era}
    _PITCHER_CACHE[pid] = {"ts": now, "ttl": 21600, "data": line}
    return line

def _team_side_blob(gameData, linescore, boxscore, side):
    team = gameData.get("teams", {}).get(side, {}) or {}
    abbr = team.get("abbreviation") or team.get("clubName")
    tid = team.get("id")
    ls_team = (linescore or {}).get("teams", {}).get(side, {}) or {}
    score = ls_team.get("runs")
    return {"id": tid, "abbr": abbr, "name": team.get("name"), "score": score}

def _current_pitch_bat(live):
    ld = live.get("liveData", {}) or {}
    linescore = ld.get("linescore", {}) or {}
    current = ld.get("plays", {}).get("currentPlay", {}) or {}
    matchup = current.get("matchup", {}) or {}

    pit_id = (matchup.get("pitcher") or {}).get("id") or (linescore.get("defense") or {}).get("pitcher", {}).get("id")
    bat_id = (matchup.get("batter")  or {}).get("id") or (linescore.get("offense") or {}).get("batter", {}).get("id")

    offense = (linescore.get("offense") or {}).get("team", {}).get("id")
    defense = (linescore.get("defense") or {}).get("team", {}).get("id")

    box = ld.get("boxscore", {}) or {}
    home_players = (box.get("teams", {}).get("home", {}) or {}).get("players", {}) or {}
    away_players = (box.get("teams", {}).get("away", {}) or {}).get("players", {}) or {}

    def _pitch_line(pid):
        if not pid: return None
        key = f"ID{pid}"
        stat = (home_players.get(key) or away_players.get(key) or {}).get("stats", {}).get("pitching", {}) or {}
        ip = stat.get("inningsPitched")
        pitches = stat.get("numberOfPitches") or stat.get("pitchesThrown")
        er = stat.get("earnedRuns"); so = stat.get("strikeOuts"); bb = stat.get("baseOnBalls")
        name = (home_players.get(key) or away_players.get(key) or {}).get("person", {}).get("lastName")
        return {"id": pid, "name": name, "ip": ip, "pitches": pitches, "er": er, "k": so, "bb": bb}

    def _bat_line(pid):
        if not pid: return None
        key = f"ID{pid}"
        stat = (home_players.get(key) or away_players.get(key) or {}).get("stats", {}).get("batting", {}) or {}
        ab = stat.get("atBats") or 0
        hits = stat.get("hits") or 0
        name = (home_players.get(key) or away_players.get(key) or {}).get("person", {}).get("lastName")
        return {"id": pid, "name": name, "line": f"{hits}-{ab}"}

    return {
        "offense_team_id": offense,
        "defense_team_id": defense,
        "pitcher": _pitch_line(pit_id),
        "batter": _bat_line(bat_id)
    }

def _shape_view(live, gameDateUTC):
    gdata = live.get("gameData", {}) or {}
    ld = live.get("liveData", {}) or {}
    status = gdata.get("status", {}) or {}
    abstract = (status.get("abstractGameState") or "").upper()

    ls = ld.get("linescore", {}) or {}
    boxscore = ld.get("boxscore", {}) or {}
    venue = (gdata.get("venue") or {}).get("name")

    away = _team_side_blob(gdata, ls, boxscore, "away")
    home = _team_side_blob(gdata, ls, boxscore, "home")

    start_et = _fmt_et_from_iso(gameDateUTC) if gameDateUTC else ""

    probables = {}
    if abstract == "PREVIEW":
        probs = gdata.get("probablePitchers") or {}
        for side in ("away","home"):
            pid = (probs.get(side) or {}).get("id") or (gdata.get("teams",{}).get(side,{}).get("probablePitcher") or {}).get("id")
            if pid:
                p = _fetch_pitcher_season_line(pid, SEASON)
                probables[side] = {"id": pid, "text": " • ".join([x for x in [p.get("name"), p.get("wl"), (p.get("era") and f"{p.get('era')} ERA")] if x])}

    livepb = _current_pitch_bat(live) if abstract == "LIVE" else None

    away_inns = [inn.get("away","") for inn in (ls.get("innings") or [])]
    home_inns = [inn.get("home","") for inn in (ls.get("innings") or [])]
    totals = ls.get("teams", {}) or {}
    a_tot = totals.get("away", {}) or {}
    h_tot = totals.get("home", {}) or {}

    return {
        "gamePk": gdata.get("game", {}).get("pk") or live.get("gamePk"),
        "status": "scheduled" if abstract=="PREVIEW" else ("in_progress" if abstract=="LIVE" else "final"),
        "startTimeET": start_et,
        "venue": venue,
        "teams": {"away": away, "home": home},
        "probables": probables or None,
        "live": livepb,
        "linescore": {
            "awayByInning": away_inns,
            "homeByInning": home_inns,
            "away": {"R": a_tot.get("runs"), "H": a_tot.get("hits"), "E": a_tot.get("errors")},
            "home": {"R": h_tot.get("runs"), "H": h_tot.get("hits"), "E": h_tot.get("errors")},
        }
    }

def _cache_get(k):
    v = _GAMES_CACHE.get(k)
    if not v: return None
    if time.time() - v["ts"] > v["ttl"]: return None
    return v["data"]

def _cache_set(k, data, ttl):
    _GAMES_CACHE[k] = {"ts": time.time(), "ttl": ttl, "data": data}

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/standings")
def standings_page():
    try:
        data = simplify_standings(fetch_standings())
        if "National League" not in data: data["National League"] = []
        if "American League" not in data: data["American League"] = []
        return render_template("standings.html", data=data, season=SEASON)
    except Exception as e:
        log.exception("standings failed")
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

    cached = _cache_get(date_str)
    if cached:
        return jsonify(cached)

    try:
        sched_map = _fetch_schedule(date_str)
    except Exception as e:
        payload = {"date": date_str, "games": [], "error": f"schedule_error: {e}"}
        _cache_set(date_str, payload, 30)
        return jsonify(payload), 502

    games = []
    any_live = False
    for pk, gameDateUTC in sched_map.items():
        try:
            live = _fetch_live(pk)
            view = _shape_view(live, gameDateUTC)
            any_live = any_live or (view["status"] == "in_progress")
            games.append(view)
        except Exception as ex:
            log.warning("live fetch failed for %s: %s", pk, ex)

    payload = {"date": date_str, "games": games}
    _cache_set(date_str, payload, 15 if any_live else 300)
    return jsonify(payload)

@app.route("/ping")
def ping():
    return "ok", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
