
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

_PITCHER_CACHE = {}   # id -> {"ts": epoch, "data": {...}}
_CACHE = {}           # date -> {"ts": epoch, "ttl": sec, "data": {...}}

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
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json().get("dates", [{}])[0].get("games", [])

def fetch_live(game_pk):
    r = requests.get(f"{LIVE}/game/{game_pk}/feed/live", timeout=15)
    r.raise_for_status()
    return r.json()

def fetch_pitcher_stats(pid, season):
    now = time.time()
    cached = _PITCHER_CACHE.get(pid)
    if cached and (now - cached["ts"] < 6*3600):
        return cached["data"]
    url = f"{STATS}/people/{pid}"
    params = {"hydrate": f"stats(group=pitching,type=season,season={season})"}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json().get("people", [{}])[0]
    _PITCHER_CACHE[pid] = {"ts": now, "data": data}
    return data

def probable_line_from_person(person):
    name = person.get("fullName") or ""
    stats = ""
    for s in person.get("stats", []):
        if s.get("group", {}).get("displayName") == "pitching":
            splits = s.get("splits", [])
            if splits:
                st = splits[0].get("stat", {})
                w = st.get("wins"); l = st.get("losses"); era = st.get("era")
                parts = []
                if w is not None and l is not None:
                    parts.append(f"{w}-{l}")
                if era:
                    parts.append(f"{era} ERA")
                stats = " • ".join(parts)
                break
    return name, stats

def get_probables(gameData, season):
    out = {"away": None, "home": None}
    pp = gameData.get("probablePitchers") or {}
    teams = gameData.get("teams") or {}
    for side in ("away", "home"):
        pid = (pp.get(side) or {}).get("id") or (teams.get(side, {}).get("probablePitcher") or {}).get("id")
        if not pid:
            out[side] = None
            continue
        person = fetch_pitcher_stats(pid, season)
        name, statline = probable_line_from_person(person)
        out[side] = {"id": pid, "name": name, "statline": statline}
    return out

def linescore_blob(ls, force_9_live=False):
    if not ls:
        return None
    innings = ls.get("innings", []) or []
    # runs by inning may be integers or None; coerce to '' for missing
    away_by = []
    home_by = []
    for inn in innings:
        a = inn.get("away"); h = inn.get("home")
        # Some feeds provide dicts; prefer numeric 'runs' if dict
        if isinstance(a, dict):
            a = a.get("runs")
        if isinstance(h, dict):
            h = h.get("runs")
        away_by.append("" if a is None else a)
        home_by.append("" if h is None else h)

    n = len(away_by)
    if force_9_live:
        # ensure 9 columns for live; pad blanks
        if n < 9:
            away_by += [""] * (9 - n)
            home_by += [""] * (9 - n)
            n = 9
    else:
        # finals: at least 9, or extras
        if n < 9:
            away_by += [""] * (9 - n)
            home_by += [""] * (9 - n)
            n = 9

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
    return {"n": n, "away": away_by, "home": home_by, "totals": totals}

def get_last_play(ld):
    plays = ld.get("plays", {}) or {}
    cur = plays.get("currentPlay") or {}
    desc = (cur.get("result") or {}).get("description")
    if desc:
        return desc
    # fallback to last allPlays entry
    allp = plays.get("allPlays") or []
    if allp:
        return (allp[-1].get("result") or {}).get("description") or ""
    return ""

def pitcher_line_for_player_obj(player_obj, prefix=None):
    st = (player_obj.get("stats") or {}).get("pitching") or {}
    ip = st.get("inningsPitched")
    p = st.get("numberOfPitches") or st.get("pitchesThrown")
    er = st.get("earnedRuns"); k = st.get("strikeOuts"); bb = st.get("baseOnBalls")
    name = (player_obj.get("person") or {}).get("fullName")
    line = f"{name} • IP {ip or '-'} • P {p or '-'} • ER {er or 0} • K {k or 0} • BB {bb or 0}"
    return (f"{prefix}: " + line) if prefix else line

def final_pitcher_lines(ld, winner_id, loser_id):
    box = ld.get("boxscore", {}) or {}
    home = (box.get("teams", {}).get("home", {}).get("players") or {})
    away = (box.get("teams", {}).get("away", {}).get("players") or {})
    lines = {"home": "", "away": ""}
    def find(pid):
        if not pid: return None
        key = f"ID{pid}"
        return home.get(key) or away.get(key)
    w_obj = find(winner_id)
    l_obj = find(loser_id)
    if w_obj:
        lines["home"] = pitcher_line_for_player_obj(w_obj, "W")
        lines["away"] = lines["away"]  # unchanged
    if l_obj:
        l_line = pitcher_line_for_player_obj(l_obj, "L")
        # we won't know side here; return both and assign by team later
        lines["loser_line"] = l_line
    return lines

def game_state_and_participants(live):
    gd = live.get("gameData", {}) or {}
    ld = live.get("liveData", {}) or {}
    status = gd.get("status", {}) or {}
    abstract = (status.get("abstractGameState") or "").upper()  # PREVIEW, LIVE, FINAL
    if abstract not in ("PREVIEW", "LIVE", "FINAL"):
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

    # per-player game stats (from boxscore)
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

    pitch_side, pitch_obj = find_player(pitcher_id)
    pitch_stats = {}
    if pitch_obj:
        st = (pitch_obj.get("stats") or {}).get("pitching") or {}
        ip = st.get("inningsPitched")
        pitches = st.get("numberOfPitches") or st.get("pitchesThrown")
        er = st.get("earnedRuns")
        so = st.get("strikeOuts")
        bb = st.get("baseOnBalls")
        name = (pitch_obj.get("person") or {}).get("fullName")
        pitch_stats = {"name": name, "ip": ip, "p": pitches, "er": er, "k": so, "bb": bb, "side": pitch_side}

    bat_side, bat_obj = find_player(batter_id)
    bat_line = {}
    if bat_obj:
        st = (bat_obj.get("stats") or {}).get("batting") or {}
        ab = st.get("atBats"); h = st.get("hits")
        name = (bat_obj.get("person") or {}).get("fullName")
        line = f"{h}-{ab}" if (ab is not None and h is not None) else ""
        bat_line = {"name": name, "line": line, "side": bat_side}

    # decisions for finals
    decisions = (ld.get("decisions") or {})
    winner_id = (decisions.get("winner") or {}).get("id")
    loser_id = (decisions.get("loser") or {}).get("id")

    return {
        "abstract": abstract,
        "startET": start_et,
        "inning": inning,
        "isTop": bool(is_top),
        "balls": balls, "strikes": strikes, "outs": outs,
        "bases": bases,
        "pitcher": pitch_stats,
        "batter": bat_line,
        "linescore_live": linescore_blob(ls, force_9_live=True) if abstract == "LIVE" else None,
        "linescore_final": linescore_blob(ls, force_9_live=False) if abstract == "FINAL" else None,
        "winner_id": winner_id, "loser_id": loser_id,
        "lastPlay": get_last_play(ld)
    }

def shape_game(live, season):
    gd = live.get("gameData", {}) or {}
    ld = live.get("liveData", {}) or {}
    teams = gd.get("teams", {}) or {}
    home = teams.get("home", {}) or {}
    away = teams.get("away", {}) or {}

    game = {
        "gamePk": gd.get("game", {}).get("pk") or live.get("gamePk"),
        "venue": (gd.get("venue") or {}).get("name"),
        "teams": {
            "home": {"id": home.get("id"), "abbr": home.get("abbreviation") or home.get("clubName")},
            "away": {"id": away.get("id"), "abbr": away.get("abbreviation") or away.get("clubName")},
        },
        "lastPlay": ""
    }

    state = game_state_and_participants(live)
    game["lastPlay"] = state["lastPlay"]

    # status & chip
    if state["abstract"] == "PREVIEW":
        game["status"] = "scheduled"
        game["chip"] = state["startET"]
    elif state["abstract"] == "LIVE":
        game["status"] = "in_progress"
        game["chip"] = f"{'Top' if state['isTop'] else 'Bot'} {state['inning']} • {state['balls']}-{state['strikes']}, {state['outs']} out{'s' if state['outs']!=1 else ''}"
    else:
        game["status"] = "final"
        game["chip"] = "Final"

    # scores/linescore
    if state["abstract"] == "LIVE":
        ls = state["linescore_live"]
    elif state["abstract"] == "FINAL":
        ls = state["linescore_final"]
    else:
        ls = None
    game["linescore"] = ls
    if ls:
        totals = ls["totals"]
        game["teams"]["away"].update({"score": totals["away"]["R"], "hits": totals["away"]["H"], "errors": totals["away"]["E"]})
        game["teams"]["home"].update({"score": totals["home"]["R"], "hits": totals["home"]["H"], "errors": totals["home"]["E"]})
    else:
        game["teams"]["away"]["score"] = None
        game["teams"]["home"]["score"] = None

    # probables for scheduled
    if game["status"] == "scheduled":
        prob = get_probables(gd, season)
        for side in ("away","home"):
            p = prob.get(side)
            game["teams"][side]["probable"] = f"{p['name']} • {p['statline']}".strip(" •") if p else ""

    # live: current pitcher/batter lines
    if game["status"] == "in_progress":
        pit = state["pitcher"]; bat = state["batter"]
        if pit and pit.get("side") in ("home","away"):
            game["teams"][pit["side"]]["currentPitcher"] = f"P: {pit['name']} • IP {pit['ip'] or '-'} • P {pit['p'] or '-'} • ER {pit['er'] or 0} • K {pit['k'] or 0} • BB {pit['bb'] or 0}"
        if bat and bat.get("side") in ("home","away"):
            game["teams"][bat["side"]]["currentBatter"] = f"B: {bat['name']} • {bat['line'] or ''}"
        game["bases"] = state["bases"]
    else:
        game["bases"] = None

    # finals: winner/loser pitcher lines next to winning/losing team
    if game["status"] == "final":
        winner_id, loser_id = state["winner_id"], state["loser_id"]
        box = ld.get("boxscore", {}) or {}
        home_players = (box.get("teams", {}).get("home", {}).get("players") or {})
        away_players = (box.get("teams", {}).get("away", {}).get("players") or {})
        def find(pid):
            if not pid: return None, None
            key = f"ID{pid}"
            if key in home_players: return "home", home_players[key]
            if key in away_players: return "away", away_players[key]
            return None, None
        w_side, w_obj = find(winner_id)
        l_side, l_obj = find(loser_id)
        if w_obj and w_side in ("home","away"):
            game["teams"][w_side]["finalPitcher"] = pitcher_line_for_player_obj(w_obj, "W")
        if l_obj and l_side in ("home","away"):
            game["teams"][l_side]["finalPitcher"] = pitcher_line_for_player_obj(l_obj, "L")

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

    games = []
    any_live = False
    for g in schedule:
        pk = g.get("gamePk")
        if not pk: continue
        try:
            live = fetch_live(pk)
            shaped = shape_game(live, season)
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
