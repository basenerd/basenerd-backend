from flask import Flask, render_template, jsonify, request
import requests, time, logging
from datetime import datetime, timedelta, timezone

app = Flask(__name__)

# ---- Logging ----
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("basenerd")

# ===========================
# Standings (AL/NL)  — supports Division & Wild Card
# ===========================
SEASON = 2025
STANDINGS_URL = "https://statsapi.mlb.com/api/v1/standings"
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

def _normalize_pct(pct_str):
    if pct_str in (None, ""):
        return 0.0
    try:
        s = str(pct_str).strip()
        return float("0" + s) if s.startswith(".") else float(s)
    except Exception:
        return 0.0

def _last10(tr: dict) -> str:
    recs = (tr.get("records") or {}).get("splitRecords") or []
    for rec in recs:
        if rec.get("type") == "lastTen":
            return f"{rec.get('wins', 0)}-{rec.get('losses', 0)}"
    return ""

def _abbr(team: dict) -> str:
    tid = team.get("id")
    if tid in TEAM_ABBR:
        return TEAM_ABBR[tid]
    name = (team.get("name") or "").replace(" ", "")
    return (name[:3] or "TBD").upper()

def fetch_standings_type(standings_type: str, season: int = SEASON):
    """standings_type e.g. 'byDivision' (division view) or 'wildCard' (wild card view)."""
    params = {
        "leagueId": "103,104",
        "season": str(season),
        "standingsTypes": standings_type,
    }
    r = requests.get(STANDINGS_URL, params=params, timeout=20)
    r.raise_for_status()
    return (r.json() or {}).get("records") or []

def simplify_standings(records, mode: str = "division"):
    """
    Convert MLB API records into:
    {
      "National League": [ { "division":"National League East", "rows":[...]} ],
      "American League": [ ... ]
    }
    If mode='wildcard', each league becomes a single 'Wild Card' block.
    """
    leagues = {"National League": [], "American League": []}
    for block in (records or []):
        league_obj = block.get("league") or {}
        division_obj = block.get("division") or {}

        league_id = league_obj.get("id")
        league_name = LEAGUE_NAME.get(league_id, league_obj.get("name") or "League")

        if mode == "wildcard":
            division_name = "Wild Card"
        else:
            division_id = division_obj.get("id")
            division_name = DIVISION_NAME.get(division_id, division_obj.get("name") or "Division")

        rows = []
        for tr in (block.get("teamRecords") or []):
            team = tr.get("team", {}) or {}
            rows.append({
                "team_name": team.get("name", "Team"),
                "team_abbr": _abbr(team),
                "team_id": team.get("id"),
                "w": tr.get("wins"),
                "l": tr.get("losses"),
                "pct": _normalize_pct(tr.get("winningPercentage")),
                "gb": tr.get("gamesBack"),
                "streak": (tr.get("streak") or {}).get("streakCode", ""),
                "last10": _last10(tr),
                "runDiff": tr.get("runDifferential"),
            })

        # Division mode preserves multiple divisions per league.
        # Wild Card mode: compress to a single block per league.
        if league_name in leagues:
            if mode == "wildcard":
                # Merge all rows for a league under one 'Wild Card' block
                if leagues[league_name] and leagues[league_name][-1]["division"] == "Wild Card":
                    leagues[league_name][-1]["rows"].extend(rows)
                else:
                    leagues[league_name].append({"division": "Wild Card", "rows": rows})
            else:
                leagues[league_name].append({"division": division_name, "rows": rows})
        else:
            # Fallback: assign to NL
            leagues["National League"].append({"division": division_name, "rows": rows})

    return leagues

@app.route("/standings")
def standings():
    try:
        # Division view
        rec_div = fetch_standings_type("byDivision", SEASON)
        data_div = simplify_standings(rec_div, mode="division")

        # Wild Card view
        rec_wc = fetch_standings_type("wildCard", SEASON)
        data_wc = simplify_standings(rec_wc, mode="wildcard")

        # Ensure keys
        for d in (data_div, data_wc):
            if "National League" not in d: d["National League"] = []
            if "American League" not in d: d["American League"] = []

        return render_template("standings.html",
                               season=SEASON,
                               data_division=data_div,
                               data_wc=data_wc)
    except Exception as e:
        log.exception("standings error")
        safe = {"National League": [], "American League": []}
        return render_template("standings.html",
                               season=SEASON,
                               data_division=safe,
                               data_wc=safe,
                               error=str(e)), 200

# ===========================
# Today’s Games (no external tz deps)
# ===========================
STATS = "https://statsapi.mlb.com/api/v1"
LIVE  = "https://statsapi.mlb.com/api/v1.1"

_PITCHER_CACHE = {}   # id -> cached person stats (for probables W-L/ERA)
_CACHE = {}           # date -> cached games payload

def to_et(iso_z: str) -> str:
    """
    Convert MLB ISO UTC (e.g. '2025-08-09T22:05:00Z') to Eastern Time string like '6:05 PM ET'
    using US daylight-saving rules without external libraries.

    US DST: starts 2:00 local on the second Sunday in March (07:00 UTC),
            ends   2:00 local on the first Sunday in November (06:00 UTC).
    """
    if not iso_z:
        return ""
    try:
        dt_utc = datetime.fromisoformat(iso_z.replace("Z", "+00:00")).astimezone(timezone.utc)
        y = dt_utc.year

        def first_sunday(year: int, month: int) -> datetime:
            d = datetime(year, month, 1, tzinfo=timezone.utc)
            days_to_sun = (6 - d.weekday()) % 7  # Mon=0 .. Sun=6
            return d + timedelta(days=days_to_sun)

        first_sun_mar = first_sunday(y, 3)
        second_sun_mar = first_sun_mar + timedelta(days=7)
        dst_start_utc = second_sun_mar.replace(hour=7, minute=0, second=0, microsecond=0)

        first_sun_nov = first_sunday(y, 11)
        dst_end_utc = first_sun_nov.replace(hour=6, minute=0, second=0, microsecond=0)

        offset_hours = -4 if dst_start_utc <= dt_utc < dst_end_utc else -5
        dt_et = dt_utc + timedelta(hours=offset_hours)

        # %-I not on Windows; fall back to %I and strip leading 0
        try:
            return dt_et.strftime("%-I:%M %p ET")
        except Exception:
            return dt_et.strftime("%I:%M %p ET").lstrip("0") + " ET"
    except Exception:
        return ""

def fetch_schedule(date_str):
    url = f"{STATS}/schedule"
    params = {"sportId": 1, "date": date_str}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json().get("dates", [{}])[0].get("games", [])

def fetch_live(game_pk):
    r = requests.get(f"{LIVE}/game/{game_pk}/feed/live", timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_pitcher_stats(pid, season):
    now = time.time()
    c = _PITCHER_CACHE.get(pid)
    if c and now - c["ts"] < 6*3600:
        return c["data"]
    url = f"{STATS}/people/{pid}"
    params = {"hydrate": f"stats(group=pitching,type=season,season={season})"}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json().get("people", [{}])[0]
    _PITCHER_CACHE[pid] = {"ts": now, "data": data}
    return data

def probable_line_from_person(person):
    name = person.get("fullName") or ""
    wl, era = "", ""
    for s in person.get("stats", []):
        if s.get("group", {}).get("displayName") == "pitching":
            splits = s.get("splits", [])
            if splits:
                st = splits[0].get("stat", {})
                w = st.get("wins"); l = st.get("losses"); era = st.get("era")
                if w is not None and l is not None:
                    wl = f"{w}-{l}"
                if era:
                    era = f"{era} ERA"
            break
    parts = [name]
    if wl: parts.append(wl)
    if era: parts.append(era)
    return " • ".join([p for p in parts if p])

def get_probables(gameData, season):
    out = {"away": "", "home": ""}
    pp = gameData.get("probablePitchers") or {}
    teams = gameData.get("teams") or {}
    for side in ("away", "home"):
        pid = (pp.get(side) or {}).get("id") or (teams.get(side, {}).get("probablePitcher") or {}).get("id")
        if not pid:
            out[side] = ""
            continue
        person = fetch_pitcher_stats(pid, season)
        out[side] = probable_line_from_person(person)
    return out

def linescore_blob(ls, force_n=None):
    if not ls:
        if force_n:
            return {"n": force_n, "away": ["" for _ in range(force_n)], "home": ["" for _ in range(force_n)],
                    "totals": {"away":{"R":None,"H":None,"E":None},"home":{"R":None,"H":None,"E":None}}}
        return None
    innings = ls.get("innings", [])
    away_by, home_by = [], []
    for inn in innings:
        a = inn.get("away", {})
        h = inn.get("home", {})
        away_by.append(a if isinstance(a, (int, str)) else a.get("runs", ""))
        home_by.append(h if isinstance(h, (int, str)) else h.get("runs", ""))
    n = len(away_by)
    if force_n and n < force_n:
        away_by += [""] * (force_n - n)
        home_by += [""] * (force_n - n)
        n = force_n
    totals = {
        "away": {
            "R": (ls.get("teams", {}).get("away", {}) or {}).get("runs"),
            "H": (ls.get("teams", {}).get("away", {}) or {}).get("hits"),
            "E": (ls.get("teams", {}).get("away", {}) or {}).get("errors"),
        },
        "home": {
            "R": (ls.get("teams", {}).get("home", {}) or {}).get("runs"),
            "H": (ls.get("teams", {}).get("home", {}) or {}).get("hits"),
            "E": (ls.get("teams", {}).get("home", {}) or {}).get("errors"),
        }
    }
    return {"n": max(n, force_n or 0), "away": away_by, "home": home_by, "totals": totals}

def extract_last_play(live):
    ld = live.get("liveData", {}) or {}
    plays = ld.get("plays", {}) or {}
    cp = plays.get("currentPlay") or {}
    def id_for(p):
        if not p: return ""
        pid = p.get("playId")
        if pid: return str(pid)
        about = p.get("about", {}) or {}
        return f"{about.get('atBatIndex','')}-{p.get('result',{}).get('eventType','')}"
    if cp:
        desc = (cp.get("result", {}) or {}).get("description")
        if desc:
            return {"id": id_for(cp), "text": desc}
    ap = plays.get("allPlays") or []
    if ap:
        last = ap[-1]
        desc = (last.get("result", {}) or {}).get("description")
        return {"id": id_for(last), "text": desc or ""}
    return {"id":"", "text":""}

def game_state_and_participants(live, include_records=None):
    gd = live.get("gameData", {}) or {}
    ld = live.get("liveData", {}) or {}
    status = gd.get("status", {}) or {}
    abstract = (status.get("abstractGameState") or "").upper()
    if abstract not in ("PREVIEW","LIVE","FINAL"):
        abstract = "PREVIEW"

    start_iso = gd.get("datetime", {}).get("dateTime") or gd.get("datetime", {}).get("startTimeUTC")
    start_et = to_et(start_iso)

    ls = ld.get("linescore", {}) or {}
    box = ld.get("boxscore", {}) or {}

    current = ld.get("plays", {}).get("currentPlay", {}) or {}
    matchup = current.get("matchup", {}) or {}
    count = current.get("count", {}) or {}
    batter_id = (matchup.get("batter") or {}).get("id")
    pitcher_id = (matchup.get("pitcher") or {}).get("id")

    is_top = ls.get("isTopInning")
    inning = ls.get("currentInning")
    balls = count.get("balls", ls.get("balls"))
    strikes = count.get("strikes", ls.get("strikes"))
    outs = count.get("outs", ls.get("outs"))

    bases = {
        "first": bool((ls.get("offense") or {}).get("first")),
        "second": bool((ls.get("offense") or {}).get("second")),
        "third": bool((ls.get("offense") or {}).get("third")),
    }

    home_players = (box.get("teams", {}).get("home", {}).get("players") or {})
    away_players = (box.get("teams", {}).get("away", {}).get("players") or {})

    def find_player(pid):
        if not pid:
            return None, None
        key = f"ID{pid}"
        if key in home_players:
            return "home", home_players[key]
        if key in away_players:
            return "away", away_players[key]
        return None, None

    # pitcher stats (this game)
    pitch_side, pitch_obj = find_player(pitcher_id)
    pitch_stats = {}
    if pitch_obj:
        st = (pitch_obj.get("stats") or {}).get("pitching") or {}
        pitch_stats = {
            "name": (pitch_obj.get("person") or {}).get("fullName"),
            "ip": st.get("inningsPitched"),
            "p": st.get("numberOfPitches") or st.get("pitchesThrown"),
            "er": st.get("earnedRuns"),
            "k": st.get("strikeOuts"),
            "bb": st.get("baseOnBalls"),
            "side": pitch_side
        }

    # batter line (H-AB)
    bat_side, bat_obj = find_player(batter_id)
    bat_line = {}
    if bat_obj:
        st = (bat_obj.get("stats") or {}).get("batting") or {}
        ab = st.get("atBats"); h = st.get("hits")
        bat_line = {"name": (bat_obj.get("person") or {}).get("fullName"), "line": (f"{h}-{ab}" if h is not None and ab is not None else ""), "side": bat_side}

    records = include_records or {"away":"", "home":""}

    return {
        "abstract": abstract,
        "startET": start_et,
        "inning": inning,
        "isTop": bool(is_top),
        "balls": balls, "strikes": strikes, "outs": outs,
        "bases": bases,
        "pitcher": pitch_stats,
        "batter": bat_line,
        "records": records,
        "lastPlay": extract_last_play(live),
        "linescore": ls
    }

def shape_game(live, season, records=None):
    gd = live.get("gameData", {}) or {}
    teams = gd.get("teams", {}) or {}
    home = teams.get("home", {}) or {}
    away = teams.get("away", {}) or {}

    state = game_state_and_participants(live, include_records=records)
    ls = state["linescore"]

    game = {
        "gamePk": gd.get("game", {}).get("pk") or live.get("gamePk"),
        "venue": (gd.get("venue") or {}).get("name"),
        "status": "scheduled" if state["abstract"] == "PREVIEW" else ("in_progress" if state["abstract"] == "LIVE" else "final"),
        "chip": state["startET"] if state["abstract"] == "PREVIEW" else (f"{'Top' if state['isTop'] else 'Bot'} {state['inning']} • {state['balls']}-{state['strikes']}, {state['outs']} out{'s' if state['outs']!=1 else ''}" if state["abstract"] == "LIVE" else "Final"),
        "bases": state["bases"],
        "lastPlay": state["lastPlay"].get("text",""),
        "lastPlayId": state["lastPlay"].get("id",""),
        "teams": {
            "home": {"id": home.get("id"), "abbr": home.get("abbreviation") or home.get("clubName"), "record": state["records"].get("home","")},
            "away": {"id": away.get("id"), "abbr": away.get("abbreviation") or away.get("clubName"), "record": state["records"].get("away","")},
        }
    }

    if game["status"] == "in_progress":
        game["linescore"] = linescore_blob(ls, force_n=9)
    elif game["status"] == "final":
        n_innings = max(9, len(ls.get("innings", []) if ls else []))
        game["linescore"] = linescore_blob(ls, force_n=n_innings)
    else:
        game["linescore"] = None

    if game["linescore"]:
        totals = game["linescore"]["totals"]
        game["teams"]["away"].update({"score": totals["away"]["R"], "hits": totals["away"]["H"], "errors": totals["away"]["E"]})
        game["teams"]["home"].update({"score": totals["home"]["R"], "hits": totals["home"]["H"], "errors": totals["home"]["E"]})
    else:
        game["teams"]["away"]["score"] = None
        game["teams"]["home"]["score"] = None

    if game["status"] == "scheduled":
        prob = get_probables(gd, season)
        for side in ("away","home"):
            game["teams"][side]["probable"] = prob.get(side, "")

    if game["status"] == "in_progress":
        pit = state["pitcher"]; bat = state["batter"]
        if pit and pit.get("side") in ("home","away"):
            game["teams"][pit["side"]]["currentPitcher"] = f"P: {pit['name']} • IP {pit.get('ip','-')} • P {pit.get('p','-')} • ER {pit.get('er',0)} • K {pit.get('k',0)} • BB {pit.get('bb',0)}"
        if bat and bat.get("side") in ("home","away"):
            game["teams"][bat["side"]]["currentBatter"] = f"B: {bat['name']} • {bat.get('line','')}"

    if game["status"] == "final":
        box = live.get("liveData", {}).get("boxscore", {})
        decisions = live.get("liveData", {}).get("decisions", {}) or {}
        win_id = (decisions.get("winner") or {}).get("id")
        lose_id = (decisions.get("loser") or {}).get("id")

        def pitcher_line_for(pid):
            if not pid: return "", ""
            for side in ("home","away"):
                players = (box.get("teams", {}).get(side, {}).get("players") or {})
                p = players.get(f"ID{pid}")
                if p:
                    st = (p.get("stats") or {}).get("pitching") or {}
                    name = (p.get("person") or {}).get("fullName") or ""
                    line = f"{name} • IP {st.get('inningsPitched','-')} • P {st.get('numberOfPitches') or st.get('pitchesThrown') or '-'} • ER {st.get('earnedRuns',0)} • K {st.get('strikeOuts',0)} • BB {st.get('baseOnBalls',0)}"
                    return side, line
            return "", ""
        win_side, win_line = pitcher_line_for(win_id)
        lose_side, lose_line = pitcher_line_for(lose_id)
        if win_side:
            game["teams"][win_side]["finalPitcher"] = "W: " + win_line
        if lose_side:
            game["teams"][lose_side]["finalPitcher"] = "L: " + lose_line

    return game

def cache_get(key):
    x = _CACHE.get(key)
    if not x: return None
    if time.time() - x["ts"] > x["ttl"]:
        return None
    return x["data"]

def cache_set(key, data, ttl):
    _CACHE[key] = {"ts": time.time(), "ttl": ttl, "data": data}

# ===========================
# Routes
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
        log.exception("standings error")
        safe = {"National League": [], "American League": []}
        return render_template("standings.html", data=safe, season=SEASON, error=str(e)), 200

@app.route("/todaysgames")
def todays_games_page():
    return render_template("todaysgames.html")

@app.route("/api/games")
def api_games():
    date_str = request.args.get("date")
    season = request.args.get("season")
    if not date_str:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
    if not season:
        season = datetime.utcnow().year

    cached = cache_get(date_str)
    if cached:
        return jsonify(cached)

    try:
        schedule = fetch_schedule(date_str)
    except Exception as e:
        data = {"date": date_str, "games": [], "error": f"schedule_error: {e}"}
        cache_set(date_str, data, 30)
        return jsonify(data), 502

    # records from schedule
    record_map = {}
    for g in schedule:
        pk = g.get("gamePk")
        if not pk: continue
        away_rec = g.get("teams", {}).get("away", {}).get("leagueRecord", {})
        home_rec = g.get("teams", {}).get("home", {}).get("leagueRecord", {})
        record_map[pk] = {
            "away": f"{away_rec.get('wins','')}-{away_rec.get('losses','')}",
            "home": f"{home_rec.get('wins','')}-{home_rec.get('losses','')}",
        }

    games = []
    any_live = False
    for g in schedule:
        pk = g.get("gamePk")
        if not pk: continue
        try:
            live = fetch_live(pk)
            shaped = shape_game(live, season, records=record_map.get(pk))
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

# ---- Local dev entrypoint ----
if __name__ == "__main__":
    # Render/Gunicorn uses this as a module; this is for local testing.
    app.run(host="0.0.0.0", port=5000, debug=True)
