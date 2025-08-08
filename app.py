
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

# ---------------- STANDINGS (unchanged from your app) ----------------
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
        else:
            leagues["National League"].append({"division": division_name, "rows": rows})
    return leagues

# ---------------- TODAY'S GAMES ----------------
STATS = "https://statsapi.mlb.com/api/v1"
LIVE  = "https://statsapi.mlb.com/api/v1.1"
_PEOPLE_CACHE = {}  # (pid, season) -> (ts, ttl, data)
_GAMES_CACHE = {}   # date -> (ts, ttl, data)

def _parse_utc(iso_z: str):
    if not iso_z: return None
    # robust parse of '2025-08-09T00:10:00Z'
    try:
        if iso_z.endswith('Z'):
            dt = datetime.fromisoformat(iso_z.replace('Z', '+00:00'))
        else:
            dt = datetime.fromisoformat(iso_z)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def _to_et(iso_z: str):
    dt_utc = _parse_utc(iso_z)
    if not dt_utc: return ""
    if ET_TZ:
        return dt_utc.astimezone(ET_TZ).strftime("%-I:%M %p ET")
    # Fallback to UTC label if pytz missing
    return dt_utc.strftime("%-I:%M %p UTC")

def _people_pitch_stats(pid: int, season: int):
    key = (pid, season)
    now = time.time()
    cached = _PEOPLE_CACHE.get(key)
    if cached and (now - cached[0] < cached[1]):
        return cached[2]
    url = f"{STATS}/people/{pid}"
    params = {"hydrate": f"stats(group=pitching,type=season,season={season})"}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json() or {}
    person = (data.get("people") or [{}])[0]
    splits = (((person.get("stats") or []) + [{}])[0].get("splits") or [])
    stat = (splits[0] if splits else {}).get("stat") or {}
    result = {
        "wins": stat.get("wins"),
        "losses": stat.get("losses"),
        "era": stat.get("era"),
        "firstLastName": person.get("firstLastName") or person.get("fullName") or person.get("lastName"),
        "throws": person.get("pitchHand", {}).get("code"),
    }
    _PEOPLE_CACHE[key] = (now, 6*3600, result)  # 6h TTL
    return result

def _probable_line(pid: int, season: int):
    if not pid: return ""
    try:
        st = _people_pitch_stats(pid, season)
        name = st.get("firstLastName") or ""
        wl = (f"{st.get('wins','?')}-{st.get('losses','?')}")
        era = st.get("era") or "—"
        return f"{name} • {wl} • {era} ERA"
    except Exception:
        return ""

def _fetch_schedule(date_str):
    url = f"{STATS}/schedule"
    params = {"sportId": 1, "date": date_str}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json().get("dates", [{}])[0].get("games", [])

def _fetch_live(game_pk):
    r = requests.get(f"{LIVE}/game/{game_pk}/feed/live", timeout=15)
    r.raise_for_status()
    return r.json()

def _cache_get(store, key):
    item = store.get(key)
    if not item: return None
    ts, ttl, data = item
    if time.time() - ts > ttl: return None
    return data

def _cache_set(store, key, data, ttl):
    store[key] = (time.time(), ttl, data)

def _team_basic(gameData, side, linescore):
    t = (gameData.get("teams", {}) or {}).get(side, {}) or {}
    ls = (linescore or {}).get("teams", {}).get(side, {}) or {}
    return {
        "id": t.get("id"),
        "abbr": t.get("abbreviation") or t.get("clubName") or "",
        "name": t.get("name") or "",
        "score": ls.get("runs"),  # may be None for scheduled
        "hits": ls.get("hits"),
        "errors": ls.get("errors"),
        "note": ""  # we'll fill later
    }

def _player_pitch_line(boxscore, side_key, player_id):
    try:
        players = (boxscore.get("teams", {}).get(side_key, {}).get("players") or {})
        pid_key = f"ID{player_id}"
        p = players.get(pid_key, {})
        st = (p.get("stats") or {}).get("pitching") or {}
        # inningsPitched is like "4.2"
        return {
            "name": (p.get("person") or {}).get("fullName", ""),
            "ip": st.get("inningsPitched"),
            "er": st.get("earnedRuns"),
            "so": st.get("strikeOuts"),
            "bb": st.get("baseOnBalls"),
            "pitches": st.get("numberOfPitches") or st.get("pitchesThrown") or st.get("pitchCount")
        }
    except Exception:
        return None

def _current_batter_name(boxscore, side_key, player_id):
    try:
        players = (boxscore.get("teams", {}).get(side_key, {}).get("players") or {})
        pid_key = f"ID{player_id}"
        p = players.get(pid_key, {})
        return (p.get("person") or {}).get("fullName") or ""
    except Exception:
        return ""

def _shape_game(live, sched_game):
    gd = live.get("gameData", {}) or {}
    ld = live.get("liveData", {}) or {}
    ls = ld.get("linescore", {}) or {}
    status = gd.get("status", {}) or {}
    abstract = (status.get("abstractGameState") or "").upper()
    if abstract not in ("PREVIEW","LIVE","FINAL"):
        abstract = "PREVIEW"

    home = _team_basic(gd, "home", ls)
    away = _team_basic(gd, "away", ls)
    home_id, away_id = home["id"], away["id"]

    # Use schedule-provided gameDate (UTC) for start time -> ET
    start_utc = (sched_game or {}).get("gameDate") or gd.get("datetime", {}).get("dateTime")
    start_time_local = _to_et(start_utc)

    # Default: suppress scores for scheduled
    if abstract == "PREVIEW":
        away["score"] = None
        home["score"] = None
        # Probables with W-L & ERA
        probs = gd.get("probablePitchers", {}) or {}
        a_pid = (probs.get("away") or {}).get("id")
        h_pid = (probs.get("home") or {}).get("id")
        away["note"] = _probable_line(a_pid, SEASON) if a_pid else ""
        home["note"] = _probable_line(h_pid, SEASON) if h_pid else ""

    elif abstract == "LIVE":
        # Current pitcher / batter (from currentPlay; fallback to linescore offense/defense)
        cur = (ld.get("plays", {}) or {}).get("currentPlay", {}) or {}
        matchup = cur.get("matchup") or {}
        pitcher_id = (matchup.get("pitcher") or {}).get("id") or (ls.get("defense") or {}).get("pitcher", {}).get("id")
        batter_id  = (matchup.get("batter") or {}).get("id")  or (ls.get("offense") or {}).get("batter", {}).get("id")
        # Determine which side is pitching/hitting
        defense_team_id = (ls.get("defense") or {}).get("team", {}).get("id")
        offense_team_id = (ls.get("offense") or {}).get("team", {}).get("id")
        box = ld.get("boxscore", {}) or {}

        if defense_team_id == home_id:
            # home pitching, away batting
            pline = _player_pitch_line(box, "home", pitcher_id) if pitcher_id else None
            bname = _current_batter_name(box, "away", batter_id) if batter_id else ""
            if pline:
                home["note"] = f"P: {pline['name']} • IP {pline['ip'] or '—'} • P {pline['pitches'] or '—'} • ER {pline['er'] or 0} • K {pline['so'] or 0} • BB {pline['bb'] or 0}"
            if bname:
                away["note"] = f"Bat: {bname}"
        elif defense_team_id == away_id:
            # away pitching
            pline = _player_pitch_line(box, "away", pitcher_id) if pitcher_id else None
            bname = _current_batter_name(box, "home", batter_id) if batter_id else ""
            if pline:
                away["note"] = f"P: {pline['name']} • IP {pline['ip'] or '—'} • P {pline['pitches'] or '—'} • ER {pline['er'] or 0} • K {pline['so'] or 0} • BB {pline['bb'] or 0}"
            if bname:
                home["note"] = f"Bat: {bname}"
        else:
            # Fallback: just list names if team ids aren't present
            if pitcher_id:
                # guess pitcher belongs to defense
                # try find in boxscore home first
                pl = _player_pitch_line(box, "home", pitcher_id) or _player_pitch_line(box, "away", pitcher_id)
                if pl:
                    # assign to whichever side has that player
                    home["note"] = home["note"] or f"P: {pl['name']} • IP {pl['ip'] or '—'} • P {pl['pitches'] or '—'} • ER {pl['er'] or 0} • K {pl['so'] or 0} • BB {pl['bb'] or 0}"
            if batter_id:
                # can't tell side; just put on away
                bname = _current_batter_name(box, "away", batter_id) or _current_batter_name(box, "home", batter_id)
                if bname:
                    away["note"] = away["note"] or f"Bat: {bname}"

    # Final: leave note blank or could show winning/losing pitcher later.
    return {
        "gamePk": live.get("gamePk") or (gd.get("game") or {}).get("pk"),
        "status": "in_progress" if abstract == "LIVE" else ("final" if abstract == "FINAL" else "scheduled"),
        "startTimeLocal": start_time_local,
        "venue": (gd.get("venue") or {}).get("name", ""),
        "teams": {"away": away, "home": home},
        "linescore": ls,  # keep for future
    }

@app.route("/todaysgames")
def todays_games_page():
    return render_template("todaysgames.html")

@app.route("/api/games")
def api_games():
    date_str = request.args.get("date")
    if not date_str:
        # Use ET today's date for schedule queries
        now = datetime.now(tz=ET_TZ) if ET_TZ else datetime.utcnow()
        date_str = now.strftime("%Y-%m-%d")

    cached = _cache_get(_GAMES_CACHE, date_str)
    if cached:
        return jsonify(cached)

    try:
        schedule = _fetch_schedule(date_str)
    except Exception as e:
        payload = {"date": date_str, "games": [], "error": f"schedule_error: {e}"}
        _cache_set(_GAMES_CACHE, date_str, payload, 30)
        return jsonify(payload), 502

    games_map = {g.get("gamePk"): g for g in schedule}
    games = []
    any_live = False

    for g in schedule:
        pk = g.get("gamePk")
        if not pk: continue
        try:
            live = _fetch_live(pk)
            shaped = _shape_game(live, games_map.get(pk))
            any_live = any_live or (shaped["status"] == "in_progress")
            games.append(shaped)
        except Exception as ex:
            log.warning("live fetch failed for %s: %s", pk, ex)
            continue

    payload = {"date": date_str, "games": games}
    _cache_set(_GAMES_CACHE, date_str, payload, 15 if any_live else 300)
    return jsonify(payload)

# ---------------- ROUTES YOU ALREADY HAVE ----------------
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
        safe = {"National League": [], "American League": []}
        return render_template("standings.html", data=safe, season=SEASON, error=str(e)), 200

@app.route("/ping")
def ping():
    return "ok", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
