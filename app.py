
from flask import Flask, render_template, jsonify, request
import requests, time, logging
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
# STANDINGS (kept as-is placeholder; your existing routes should already be present elsewhere)
# If your current app already has /standings routes, keep those. This file focuses on Today's Games.
# ===========================

# ===========================
# TODAY'S GAMES
# ===========================
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
    # Expect person["stats"][0]["splits"][0]["stat"] includes wins, losses, era
    name = person.get("fullName") or ""
    stats = ""
    for s in person.get("stats", []):
        if s.get("group", {}).get("displayName") == "pitching":
            splits = s.get("splits", [])
            if splits:
                st = splits[0].get("stat", {})
                w = st.get("wins"); l = st.get("losses"); era = st.get("era")
                if w is not None and l is not None:
                    stats = f"{w}-{l}"
                if era:
                    stats = f"{stats} • {era} ERA" if stats else f"{era} ERA"
                break
    return name, stats

def get_probables(gameData, season):
    out = {"away": None, "home": None}
    pp = gameData.get("probablePitchers") or {}
    teams = gameData.get("teams") or {}
    for side in ("away", "home"):
        # get probable pitcher id
        pid = (pp.get(side) or {}).get("id") or (teams.get(side, {}).get("probablePitcher") or {}).get("id")
        if not pid:
            out[side] = None
            continue
        person = fetch_pitcher_stats(pid, season)
        name, statline = probable_line_from_person(person)
        out[side] = {"id": pid, "name": name, "statline": statline}
    return out

def linescore_blob(ls):
    if not ls:
        return None
    innings = ls.get("innings", [])
    away_by = [inn.get("away", "") for inn in innings]
    home_by = [inn.get("home", "") for inn in innings]
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
    return {"awayByInning": away_by, "homeByInning": home_by, "totals": totals}

def game_state_and_participants(live):
    gd = live.get("gameData", {}) or {}
    ld = live.get("liveData", {}) or {}
    status = gd.get("status", {}) or {}
    abstract = (status.get("abstractGameState") or "").upper()  # PREVIEW, LIVE, FINAL
    if abstract not in ("PREVIEW", "LIVE", "FINAL"):
        abstract = "PREVIEW"

    # schedule time for ET chip
    start_iso = gd.get("datetime", {}).get("dateTime") or gd.get("datetime", {}).get("startTimeUTC")
    start_et = to_et(start_iso)

    # Linescore & boxscore
    ls = ld.get("linescore", {}) or {}
    box = ld.get("boxscore", {}) or {}

    # Who's batting/defending and current matchup
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

    # offense/defense teams (ids)
    offense = (ls.get("offense") or {}).get("team", {}) or {}
    defense = (ls.get("defense") or {}).get("team", {}) or {}

    # current base occupancy
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

    # pitcher game stats
    pitch_side, pitch_obj = find_player(pitcher_id)
    pitch_stats = {}
    if pitch_obj:
        st = (pitch_obj.get("stats") or {}).get("pitching") or {}
        ip = st.get("inningsPitched")
        # pitches: StatsAPI sometimes has numberOfPitches or pitchesThrown
        pitches = st.get("numberOfPitches") or st.get("pitchesThrown")
        er = st.get("earnedRuns")
        so = st.get("strikeOuts")
        bb = st.get("baseOnBalls")
        name = (pitch_obj.get("person") or {}).get("fullName")
        pitch_stats = {"name": name, "ip": ip, "p": pitches, "er": er, "k": so, "bb": bb, "side": pitch_side}

    # batter game line (H-AB)
    bat_side, bat_obj = find_player(batter_id)
    bat_line = {}
    if bat_obj:
        st = (bat_obj.get("stats") or {}).get("batting") or {}
        ab = st.get("atBats")
        h = st.get("hits")
        name = (bat_obj.get("person") or {}).get("fullName")
        if ab is not None and h is not None:
            line = f"{h}-{ab}"
        else:
            line = ""
        bat_line = {"name": name, "line": line, "side": bat_side}

    # return summary
    return {
        "abstract": abstract,
        "startET": start_et,
        "inning": inning,
        "isTop": bool(is_top),
        "balls": balls, "strikes": strikes, "outs": outs,
        "bases": bases,
        "offenseSide": "home" if (offense.get("id") and offense.get("id") == (gd.get("teams", {}).get("home", {}).get("id"))) else "away",
        "pitcher": pitch_stats,
        "batter": bat_line,
        "linescore": linescore_blob(ls)
    }

def shape_game(live, season):
    gd = live.get("gameData", {}) or {}
    teams = gd.get("teams", {}) or {}
    home = teams.get("home", {}) or {}
    away = teams.get("away", {}) or {}

    # base team struct
    game = {
        "gamePk": gd.get("game", {}).get("pk") or live.get("gamePk"),
        "venue": (gd.get("venue") or {}).get("name"),
        "teams": {
            "home": {"id": home.get("id"), "abbr": home.get("abbreviation") or home.get("clubName")},
            "away": {"id": away.get("id"), "abbr": away.get("abbreviation") or away.get("clubName")},
        }
    }

    # enrich state & linescore and participants
    state = game_state_and_participants(live)
    game.update({
        "status": "scheduled" if state["abstract"] == "PREVIEW" else ("in_progress" if state["abstract"] == "LIVE" else "final"),
        "chip": state["startET"] if state["abstract"] == "PREVIEW" else (
            f"{'Top' if state['isTop'] else 'Bot'} {state['inning']} • {state['balls']}-{state['strikes']}, {state['outs']} out{'s' if state['outs']!=1 else ''}" if state["abstract"] == "LIVE" else "Final"
        ),
        "inningBadge": (f"{'T' if state['isTop'] else 'B'}{state['inning']}" if state["abstract"] == "LIVE" else (f"F/{state['linescore'].get('awayByInning') and len(state['linescore']['awayByInning'])}" if state["abstract"] == "FINAL" else "")),
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
        prob = get_probables(gd, season)
        for side in ("away","home"):
            p = prob.get(side)
            if p:
                game["teams"][side]["probable"] = f"{p['name']} • {p['statline']}".strip(" •")
            else:
                game["teams"][side]["probable"] = ""

    # Live participants: current pitcher and batter lines
    if game["status"] == "in_progress":
        pit = state["pitcher"]
        bat = state["batter"]
        # Determine which team is on defense/offense
        defense_side = pit.get("side") if pit else None
        offense_side = bat.get("side") if bat else None
        if pit and defense_side in ("home","away"):
            game["teams"][defense_side]["currentPitcher"] = f"P: {pit['name']} • IP {pit['ip'] or '-'} • P {pit['p'] or '-'} • ER {pit['er'] or 0} • K {pit['k'] or 0} • BB {pit['bb'] or 0}"
        if bat and offense_side in ("home","away"):
            game["teams"][offense_side]["currentBatter"] = f"B: {bat['name']} • {bat['line'] or ''}"

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
    # date=YYYY-MM-DD; season defaults to current year
    date_str = request.args.get("date")
    season = request.args.get("season")
    if not date_str:
        # ET today
        now = datetime.utcnow()
        date_str = now.strftime("%Y-%m-%d")
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
