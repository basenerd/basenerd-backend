
from flask import Flask, render_template, jsonify, request
import requests, time, logging
from datetime import datetime
try:
    import pytz
    ET_TZ = pytz.timezone("America/New_York")
except Exception:
    ET_TZ = None

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("basenerd")

STATS = "https://statsapi.mlb.com/api/v1"
LIVE  = "https://statsapi.mlb.com/api/v1.1"

_PITCHER_CACHE = {}   # id -> cached person stats (for probables W-L/ERA)
_CACHE = {}           # date -> cached games payload

def to_et(iso_z):
    if not iso_z:
        return ""
    try:
        dt = datetime.fromisoformat(iso_z.replace("Z","+00:00"))
        if ET_TZ:
            dt = dt.astimezone(ET_TZ)
        return dt.strftime("%-I:%M %p ET")
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
    away_by = []
    home_by = []
    for inn in innings:
        # inning entries can be dicts; get runs integer or "" if missing
        a = inn.get("away", {})
        h = inn.get("home", {})
        away_by.append(a if isinstance(a, (int, str)) else a.get("runs", ""))
        home_by.append(h if isinstance(h, (int, str)) else h.get("runs", ""))
    n = len(away_by)
    # pad to requested columns
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
    # Prefer currentPlay description
    desc = (plays.get("currentPlay", {}).get("result", {}) or {}).get("description")
    if desc: return desc
    # Fallback: last scoring play
    scoring = plays.get("scoringPlays") or []
    if scoring:
        last_id = scoring[-1]
        for p in plays.get("allPlays", []):
            if p.get("result", {}).get("eventType") and p.get("about", {}).get("atBatIndex") == last_id:
                d = p.get("result", {}).get("description")
                if d: return d
    # Fallback: last play in allPlays
    allp = plays.get("allPlays") or []
    if allp:
        d = allp[-1].get("result", {}).get("description")
        if d: return d
    return ""

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

    # Current matchup
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

    # Bases
    bases = {
        "first": bool((ls.get("offense") or {}).get("first")),
        "second": bool((ls.get("offense") or {}).get("second")),
        "third": bool((ls.get("offense") or {}).get("third")),
    }

    # Player lookups in boxscore
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

    # Pitcher game stats
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

    # Batter game line
    bat_side, bat_obj = find_player(batter_id)
    bat_line = {}
    if bat_obj:
        st = (bat_obj.get("stats") or {}).get("batting") or {}
        ab = st.get("atBats"); h = st.get("hits")
        bat_line = {"name": (bat_obj.get("person") or {}).get("fullName"), "line": (f"{h}-{ab}" if h is not None and ab is not None else ""), "side": bat_side}

    # Records from schedule (if provided)
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

    # Base structure
    game = {
        "gamePk": gd.get("game", {}).get("pk") or live.get("gamePk"),
        "venue": (gd.get("venue") or {}).get("name"),
        "status": "scheduled" if state["abstract"] == "PREVIEW" else ("in_progress" if state["abstract"] == "LIVE" else "final"),
        "chip": state["startET"] if state["abstract"] == "PREVIEW" else (f"{'Top' if state['isTop'] else 'Bot'} {state['inning']} • {state['balls']}-{state['strikes']}, {state['outs']} out{'s' if state['outs']!=1 else ''}" if state["abstract"] == "LIVE" else "Final"),
        "bases": state["bases"],
        "lastPlay": state["lastPlay"],
        "teams": {
            "home": {"id": home.get("id"), "abbr": home.get("abbreviation") or home.get("clubName"), "record": state["records"].get("home","")},
            "away": {"id": away.get("id"), "abbr": away.get("abbreviation") or away.get("clubName"), "record": state["records"].get("away","")},
        }
    }

    # Linescore & totals
    if game["status"] == "in_progress":
        game["linescore"] = linescore_blob(ls, force_n=9)
    elif game["status"] == "final":
        n_innings = max(9, len(ls.get("innings", []) if ls else []))
        game["linescore"] = linescore_blob(ls, force_n=n_innings)
    else:
        game["linescore"] = None

    # add team totals to scores
    if game["linescore"]:
        totals = game["linescore"]["totals"]
        game["teams"]["away"].update({"score": totals["away"]["R"], "hits": totals["away"]["H"], "errors": totals["away"]["E"]})
        game["teams"]["home"].update({"score": totals["home"]["R"], "hits": totals["home"]["H"], "errors": totals["home"]["E"]})
    else:
        game["teams"]["away"]["score"] = None
        game["teams"]["home"]["score"] = None

    # Probables for scheduled
    if game["status"] == "scheduled":
        prob = get_probables(gd, season)
        for side in ("away","home"):
            game["teams"][side]["probable"] = prob.get(side, "")

    # Live: current pitcher & batter lines
    if game["status"] == "in_progress":
        pit = state["pitcher"]; bat = state["batter"]
        if pit and pit.get("side") in ("home","away"):
            game["teams"][pit["side"]]["currentPitcher"] = f"P: {pit['name']} • IP {pit.get('ip','-')} • P {pit.get('p','-')} • ER {pit.get('er',0)} • K {pit.get('k',0)} • BB {pit.get('bb',0)}"
        if bat and bat.get("side") in ("home","away"):
            game["teams"][bat["side"]]["currentBatter"] = f"B: {bat['name']} • {bat.get('line','')}"

    # Final: winning/losing pitcher lines by team
    if game["status"] == "final":
        box = live.get("liveData", {}).get("boxscore", {})
        decisions = live.get("liveData", {}).get("decisions", {}) or {}
        # Extract winner/loser player IDs for mapping to team and stats
        win_id = (decisions.get("winner") or {}).get("id")
        lose_id = (decisions.get("loser") or {}).get("id")
        def pitcher_line_for(pid):
            if not pid: return ""
            for side in ("home","away"):
                players = (box.get("teams", {}).get(side, {}).get("players") or {})
                p = players.get(f"ID{pid}")
                if p:
                    st = (p.get("stats") or {}).get("pitching") or {}
                    name = (p.get("person") or {}).get("fullName") or ""
                    return side, f"{name} • IP {st.get('inningsPitched','-')} • P {st.get('numberOfPitches') or st.get('pitchesThrown') or '-'} • ER {st.get('earnedRuns',0)} • K {st.get('strikeOuts',0)} • BB {st.get('baseOnBalls',0)}"
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

    # Map records from schedule by gamePk
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
