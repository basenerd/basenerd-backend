
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

_PITCHER_CACHE = {}   # probable pitcher season stats
_CACHE = {}           # per-date games cache

def to_et_label(iso_z):
    if not iso_z: return ""
    try:
        dt = datetime.fromisoformat(iso_z.replace("Z","+00:00"))
        if ET_TZ: dt = dt.astimezone(ET_TZ)
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
    c = _PITCHER_CACHE.get(pid)
    if c and now - c["ts"] < 6*3600:
        return c["data"]
    url = f"{STATS}/people/{pid}"
    params = {"hydrate": f"stats(group=pitching,type=season,season={season})"}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json().get("people", [{}])[0]
    _PITCHER_CACHE[pid] = {"ts": now, "data": data}
    return data

def probable_line(person):
    name = person.get("fullName") or ""
    wl = era = None
    for s in person.get("stats", []):
        if s.get("group",{}).get("displayName") == "pitching":
            splits = s.get("splits", [])
            if splits:
                st = splits[0].get("stat", {})
                w = st.get("wins"); l = st.get("losses"); era = st.get("era")
                if w is not None and l is not None: wl = f"{w}-{l}"
                break
    parts = [name]
    if wl: parts.append(wl)
    if era: parts.append(f"{era} ERA")
    return " • ".join(parts)

def linescore_blob(ls, status):
    if not ls: 
        # For live with no ls, still produce 9 innings
        n = 9 if status == "LIVE" else 0
        return {"n": n, "away": [None]*n, "home":[None]*n, "totals":{"away":{"R":None,"H":None,"E":None},"home":{"R":None,"H":None,"E":None}}}
    innings = ls.get("innings", [])
    arr_away = []
    arr_home = []
    for inn in innings:
        a = inn.get("away")
        h = inn.get("home")
        a_runs = (a.get("runs") if isinstance(a, dict) else a) if a is not None else None
        h_runs = (h.get("runs") if isinstance(h, dict) else h) if h is not None else None
        arr_away.append(a_runs)
        arr_home.append(h_runs)
    n = max(9, len(arr_away), len(arr_home)) if status in ("LIVE","FINAL") else len(arr_away)
    # pad to n
    if len(arr_away) < n: arr_away.extend([None]*(n-len(arr_away)))
    if len(arr_home) < n: arr_home.extend([None]*(n-len(arr_home)))
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
    return {"n": n, "away": arr_away, "home": arr_home, "totals": totals}

def extract_live_context(live, start_iso_et_label):
    gd = live.get("gameData", {}) or {}
    ld = live.get("liveData", {}) or {}
    status = gd.get("status", {}) or {}
    abstract = (status.get("abstractGameState") or "").upper()  # PREVIEW/LIVE/FINAL
    if abstract not in ("PREVIEW","LIVE","FINAL"): abstract = "PREVIEW"
    ls = ld.get("linescore", {}) or {}
    box = ld.get("boxscore", {}) or {}

    # inning & count
    current = ld.get("plays", {}).get("currentPlay", {}) or {}
    matchup = current.get("matchup", {}) or {}
    count = current.get("count", {}) or {}
    inning = ls.get("currentInning")
    is_top = ls.get("isTopInning")
    balls = count.get("balls", ls.get("balls"))
    strikes = count.get("strikes", ls.get("strikes"))
    outs = count.get("outs", ls.get("outs"))

    # participants
    batter_id = (matchup.get("batter") or {}).get("id")
    pitcher_id = (matchup.get("pitcher") or {}).get("id")

    # bases
    bases = {
        "first": bool((ls.get("offense") or {}).get("first")),
        "second": bool((ls.get("offense") or {}).get("second")),
        "third": bool((ls.get("offense") or {}).get("third")),
    }

    # player find
    home_players = (box.get("teams", {}).get("home", {}).get("players") or {})
    away_players = (box.get("teams", {}).get("away", {}).get("players") or {})
    def find_player(pid):
        if not pid: return None, None
        key = f"ID{pid}"
        if key in home_players: return "home", home_players[key]
        if key in away_players: return "away", away_players[key]
        return None, None

    pitch_side, pitch_obj = find_player(pitcher_id)
    bat_side, bat_obj = find_player(batter_id)

    pitcher_line = None
    if pitch_obj:
        st = (pitch_obj.get("stats") or {}).get("pitching") or {}
        name = (pitch_obj.get("person") or {}).get("fullName")
        ip = st.get("inningsPitched")
        p = st.get("numberOfPitches") or st.get("pitchesThrown")
        er = st.get("earnedRuns"); k = st.get("strikeOuts"); bb = st.get("baseOnBalls")
        pitcher_line = f"P: {name} • IP {ip or '-'} • P {p or '-'} • ER {er or 0} • K {k or 0} • BB {bb or 0}"

    batter_line = None
    if bat_obj:
        st = (bat_obj.get("stats") or {}).get("batting") or {}
        name = (bat_obj.get("person") or {}).get("fullName")
        ab = st.get("atBats"); h = st.get("hits")
        batter_line = f"B: {name} • {h or 0}-{ab or 0}"

    # build linescore
    ls_blob = linescore_blob(ls, abstract)

    chip = start_iso_et_label if abstract == "PREVIEW" else ("Final" if abstract == "FINAL" else f"{'Top' if is_top else 'Bot'} {inning} • {balls}-{strikes}, {outs} out{'s' if outs!=1 else ''}")
    inning_badge = (f"{'T' if is_top else 'B'}{inning}" if abstract == "LIVE" else ("F" if abstract == "FINAL" else ""))

    return {
        "abstract": abstract,
        "chip": chip,
        "inning_badge": inning_badge,
        "bases": bases,
        "pitcher_line": (pitch_side, pitcher_line),
        "batter_line": (bat_side, batter_line),
        "linescore": ls_blob,
        "box": box
    }

def final_pitchers_with_stats(ctx):
    # From decisions and boxscore
    box = ctx["box"]
    ld_teams = box.get("teams", {}) if box else {}
    players = {}
    for side in ("home","away"):
        for pid, pdata in (ld_teams.get(side, {}).get("players") or {}).items():
            players[pdata.get("person",{}).get("id")] = (side, pdata)

    decisions = box.get("teams", {})  # decisions also available at liveData.decisions but boxscore has stats
    # actual decisions are in liveData.decisions
    return players

def shape_game(live, season, sched_gameDate_utc):
    gd = live.get("gameData", {}) or {}
    teams = gd.get("teams", {}) or {}
    home = teams.get("home", {}) or {}
    away = teams.get("away", {}) or {}

    start_et = to_et_label(sched_gameDate_utc)

    ctx = extract_live_context(live, start_et)

    game = {
        "gamePk": gd.get("game", {}).get("pk") or live.get("gamePk"),
        "venue": (gd.get("venue") or {}).get("name"),
        "chip": ctx["chip"],
        "inningBadge": ctx["inning_badge"],
        "bases": ctx["bases"],
        "status": "scheduled" if ctx["abstract"]=="PREVIEW" else ("in_progress" if ctx["abstract"]=="LIVE" else "final"),
        "linescore": ctx["linescore"],
        "teams": {
            "away": {"id": away.get("id"), "abbr": away.get("abbreviation") or away.get("clubName")},
            "home": {"id": home.get("id"), "abbr": home.get("abbreviation") or home.get("clubName")},
        }
    }

    # Set scores from totals if present
    totals = ctx["linescore"]["totals"]
    if totals:
        game["teams"]["away"].update({"score": totals["away"]["R"], "hits": totals["away"]["H"], "errors": totals["away"]["E"]})
        game["teams"]["home"].update({"score": totals["home"]["R"], "hits": totals["home"]["H"], "errors": totals["home"]["E"]})
    else:
        game["teams"]["away"]["score"] = None
        game["teams"]["home"]["score"] = None

    # Scheduled: probables with season W-L, ERA
    if game["status"] == "scheduled":
        pp = gd.get("probablePitchers") or {}
        for side in ("away","home"):
            pid = (pp.get(side) or {}).get("id")
            line = ""
            if pid:
                person = fetch_pitcher_stats(pid, season)
                line = probable_line(person)
            game["teams"][side]["probable"] = line

    # Live: attach pitcher and batter line to proper sides
    if game["status"] == "in_progress":
        side, pline = ctx["pitcher_line"]
        if side and pline:
            game["teams"][side]["currentPitcher"] = pline
        sideb, bline = ctx["batter_line"]
        if sideb and bline:
            game["teams"][sideb]["currentBatter"] = bline

    # Final: show winning and losing pitcher with their game stats next to team
    if game["status"] == "final":
        ld = live.get("liveData", {}) or {}
        decisions = ld.get("decisions", {}) or {}
        win = decisions.get("winner") or {}
        lose = decisions.get("loser") or {}
        win_id = win.get("id"); lose_id = lose.get("id")
        # lookup stats from boxscore
        box = ctx["box"]
        for pid, label in ((win_id, "W"), (lose_id, "L")):
            if not pid: continue
            for side in ("home","away"):
                pdata = (box.get("teams", {}).get(side, {}).get("players") or {}).get(f"ID{pid}")
                if pdata:
                    st = (pdata.get("stats") or {}).get("pitching") or {}
                    name = (pdata.get("person") or {}).get("fullName")
                    ip = st.get("inningsPitched"); p = st.get("numberOfPitches") or st.get("pitchesThrown")
                    er = st.get("earnedRuns"); k = st.get("strikeOuts"); bb = st.get("baseOnBalls")
                    line = f"{label}: {name} • IP {ip or '-'} • P {p or '-'} • ER {er or 0} • K {k or 0} • BB {bb or 0}"
                    game["teams"][side]["finalPitcher"] = line

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

    c = cache_get(date_str)
    if c:
        return jsonify(c)

    try:
        schedule = fetch_schedule(date_str)
    except Exception as e:
        data = {"date": date_str, "games": [], "error": f"schedule_error: {e}"}
        cache_set(date_str, data, 30)
        return jsonify(data), 502

    games = []
    any_live = False
    for sg in schedule:
        pk = sg.get("gamePk")
        if not pk: continue
        try:
            live = fetch_live(pk)
            shaped = shape_game(live, season, sg.get("gameDate"))
            any_live = any_live or (shaped["status"] == "in_progress")
            games.append(shaped)
        except Exception as ex:
            log.warning("live fetch failed for %s: %s", pk, ex)
            continue

    payload = {"date": date_str, "games": games}
    cache_set(date_str, payload, 15 if any_live else 300)
    return jsonify(payload)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
