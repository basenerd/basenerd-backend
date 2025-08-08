
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
    # Cache 6 hours
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

def linescore_blob(ls, force_nine_live=False):
    if not ls:
        return None
    innings = ls.get("innings", [])
    away_by = []
    home_by = []
    for inn in innings:
        # inn has keys "away" and "home" which are integers (runs) or None
        away_by.append(inn.get("away", ""))
        home_by.append(inn.get("home", ""))
    n = max(len(away_by), len(home_by))
    # For live games, pad to 9
    if force_nine_live and n < 9:
        away_by += [""] * (9 - len(away_by))
        home_by += [""] * (9 - len(home_by))
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
    return {"n": n, "awayByInning": away_by, "homeByInning": home_by, "totals": totals}

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
        ab = st.get("atBats")
        h = st.get("hits")
        name = (bat_obj.get("person") or {}).get("fullName")
        line = f"{h}-{ab}" if (ab is not None and h is not None) else ""
        bat_line = {"name": name, "line": line, "side": bat_side}

    # Build linescore blob with forced 9 if live
    ls_blob = linescore_blob(ls, force_nine_live=(abstract == "LIVE"))

    return {
        "abstract": abstract,
        "startET": start_et,
        "inning": inning,
        "isTop": bool(is_top),
        "balls": balls, "strikes": strikes, "outs": outs,
        "bases": bases,
        "pitcher": pitch_stats,
        "batter": bat_line,
        "linescore": ls_blob
    }

def shape_game(live, season):
    gd = live.get("gameData", {}) or {}
    teams = gd.get("teams", {}) or {}
    home = teams.get("home", {}) or {}
    away = teams.get("away", {}) or {}

    game = {
        "gamePk": gd.get("game", {}).get("pk") or live.get("gamePk"),
        "venue": (gd.get("venue") or {}).get("name"),
        "teams": {
            "home": {"id": home.get("id"), "abbr": home.get("abbreviation") or home.get("clubName")},
            "away": {"id": away.get("id"), "abbr": away.get("abbreviation") or away.get("clubName")},
        }
    }

    state = game_state_and_participants(live)
    # Chip text
    if state["abstract"] == "PREVIEW":
        chip = state["startET"]
    elif state["abstract"] == "LIVE":
        outs_text = f"{state['outs']} out{'s' if state['outs']!=1 else ''}" if state['outs'] is not None else ""
        chip = f"{'Top' if state['isTop'] else 'Bot'} {state['inning']} • {state['balls']}-{state['strikes']}, {outs_text}".strip().strip(', ')
    else:
        chip = "Final"

    game.update({
        "status": "scheduled" if state["abstract"] == "PREVIEW" else ("in_progress" if state["abstract"] == "LIVE" else "final"),
        "chip": chip,
        "bases": state["bases"],
        "linescore": state["linescore"]
    })

    # scores for live/final
    if state["linescore"]:
        totals = state["linescore"]["totals"]
        game["teams"]["away"].update({"score": totals["away"]["R"], "hits": totals["away"]["H"], "errors": totals["away"]["E"]})
        game["teams"]["home"].update({"score": totals["home"]["R"], "hits": totals["home"]["H"], "errors": totals["home"]["E"]})
    else:
        game["teams"]["away"].update({"score": None})
        game["teams"]["home"].update({"score": None})

    # Probables for scheduled
    if game["status"] == "scheduled":
        season_year = season or datetime.utcnow().year
        prob = get_probables(gd, season_year)
        for side in ("away","home"):
            p = prob.get(side)
            game["teams"][side]["probable"] = (f"{p['name']} • {p['statline']}".strip(" •") if p else "")

    # Live participants: current pitcher and batter
    if game["status"] == "in_progress":
        pit = state["pitcher"]
        bat = state["batter"]
        # We don't know which is home/away from pitch_side/bat_side? We included 'side' above.
        if pit and pit.get("side") in ("home","away"):
            game["teams"][pit["side"]]["currentPitcher"] = f"P: {pit['name']} • IP {pit['ip'] or '-'} • P {pit['p'] or '-'} • ER {pit['er'] or 0} • K {pit['k'] or 0} • BB {pit['bb'] or 0}"
        if bat and bat.get("side") in ("home","away"):
            game["teams"][bat["side"]]["currentBatter"] = f"B: {bat['name']} • {bat['line'] or ''}"

    # Final pitchers next to teams (W/L with this-game stats if we can find them)
    if game["status"] == "final":
        box = live.get("liveData", {}).get("boxscore", {}) or {}
        decisions = live.get("liveData", {}).get("decisions", {}) or {}
        winner = decisions.get("winner", {}) or {}
        loser = decisions.get("loser", {}) or {}

        def find_pitch_line(pid):
            if not pid:
                return ""
            for side in ("home","away"):
                players = (box.get("teams", {}).get(side, {}).get("players") or {})
                key = f"ID{pid}"
                if key in players:
                    st = (players[key].get("stats") or {}).get("pitching") or {}
                    ip = st.get("inningsPitched"); p = st.get("numberOfPitches") or st.get("pitchesThrown")
                    er = st.get("earnedRuns"); k = st.get("strikeOuts"); bb = st.get("baseOnBalls")
                    name = (players[key].get("person") or {}).get("fullName") or ""
                    return f"{name} • IP {ip or '-'} • P {p or '-'} • ER {er or 0} • K {k or 0} • BB {bb or 0}"
            return ""

        w_pid = winner.get("id"); l_pid = loser.get("id")
        w_line = find_pitch_line(w_pid)
        l_line = find_pitch_line(l_pid)

        # Decide which team is winner/loser by totals
        totals = game["linescore"]["totals"] if game.get("linescore") else None
        if totals and (totals["home"]["R"] is not None and totals["away"]["R"] is not None):
            home_won = totals["home"]["R"] > totals["away"]["R"]
            if home_won:
                game["teams"]["home"]["winnerLine"] = f"W: {w_line}" if w_line else ""
                game["teams"]["away"]["loserLine"]  = f"L: {l_line}" if l_line else ""
            else:
                game["teams"]["away"]["winnerLine"] = f"W: {w_line}" if w_line else ""
                game["teams"]["home"]["loserLine"]  = f"L: {l_line}" if l_line else ""

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
