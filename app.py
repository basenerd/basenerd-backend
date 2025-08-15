
from flask import Flask, render_template, jsonify, request
import requests, logging, time, os
from datetime import datetime, timezone, date
from typing import List, Optional, Dict

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("basenerd")

MLB_API = "https://statsapi.mlb.com/api/v1"
SPORT_ID = 1

TEAM_ABBR = {
    109:"ARI",144:"ATL",110:"BAL",111:"BOS",112:"CHC",145:"CHW",113:"CIN",114:"CLE",115:"COL",116:"DET",
    117:"HOU",118:"KC",108:"LAA",119:"LAD",146:"MIA",158:"MIL",142:"MIN",121:"NYM",147:"NYY",133:"OAK",
    134:"PIT",135:"SD",136:"SEA",137:"SF",138:"STL",139:"TB",140:"TEX",141:"TOR",143:"PHI",120:"WSH"
}

def _abbr(team_id: int, fallback: str = "") -> str:
    try:
        return TEAM_ABBR.get(int(team_id)) or (fallback or "")
    except Exception:
        return fallback or ""

def _get(url: str, params: dict | None = None, timeout: int = 12) -> dict:
    try:
        r = requests.get(url, params=params, timeout=timeout, headers={"User-Agent":"Basenerd/1.0", "Accept":"application/json"})
        r.raise_for_status()
        return r.json() or {}
    except Exception as e:
        log.info("GET failed %s params=%s: %s", url, params, e)
        return {}

try:
    import zoneinfo
    ET_TZ = zoneinfo.ZoneInfo("America/New_York")
except Exception:
    ET_TZ = None

def to_et_str(iso_z: str) -> str:
    if not iso_z: return ""
    try:
        dt = datetime.fromisoformat(iso_z.replace("Z","+00:00"))
        if ET_TZ: dt = dt.astimezone(ET_TZ)  # type: ignore
        return dt.strftime("%-I:%M %p ET")
    except Exception:
        return ""

def fetch_schedule_day(ymd: str) -> dict:
    season = (ymd or str(date.today())).split("-")[0]
    hydrate = f"team,linescore,probablePitcher,probablePitcher(stats(group=pitching,type=season,season={season}))"
    base = f"{MLB_API}/schedule"
    attempts = [
        {"sportId": SPORT_ID, "date": ymd, "hydrate": hydrate},
        {"sportId": SPORT_ID, "startDate": ymd, "endDate": ymd, "hydrate": hydrate},
        {"sportId": SPORT_ID, "date": ymd, "gameTypes": "R", "hydrate": hydrate},
        {"sportId": SPORT_ID, "startDate": ymd, "endDate": ymd, "gameTypes": "R", "hydrate": hydrate},
        {"sportId": SPORT_ID, "date": ymd},
    ]
    for params in attempts:
        js = _get(base, params)
        dates = js.get("dates") or []
        games = (dates[0].get("games") or []) if dates else []
        if games:
            return js
    return {"dates":[{"date": ymd, "games": []}]}

def fetch_live(game_pk: int) -> dict:
    return _get(f"{MLB_API}/game/{game_pk}/feed/live")

def fetch_linescore(game_pk: int) -> dict:
    return _get(f"{MLB_API}/game/{game_pk}/linescore")

def fetch_box(game_pk: int) -> dict:
    return _get(f"{MLB_API}/game/{game_pk}/boxscore")

def fetch_pbp(game_pk: int) -> dict:
    return _get(f"{MLB_API}/game/{game_pk}/playByPlay")

def _norm_status_from_sched(g: dict) -> str:
    st = ((g.get("status") or {}).get("detailedState") or "").lower()
    abs_s = ((g.get("status") or {}).get("abstractGameState") or "").lower()
    if "final" in st or "completed" in st or "game over" in st or abs_s == "final":
        return "final"
    if "in progress" in st or "warmup" in st or "delayed" in st or abs_s == "live":
        return "in_progress"
    return "scheduled"

def _ls_chip(ls: dict, status: str, game_date_iso: str) -> str:
    if status == "scheduled":
        return to_et_str(game_date_iso) or "Scheduled"
    if status == "in_progress":
        inning = ls.get("currentInning")
        sym = "▲" if ls.get("isTopInning") else ("▼" if ls.get("isTopInning") is False else "")
        if inning: return f"{sym}{inning}"
        return "Live"
    return "Final"

def latest_play_from_feed(live: dict) -> dict:
    plays = ((live.get("liveData") or {}).get("plays") or {})
    cur = plays.get("currentPlay") or {}
    if cur: return cur
    allp = plays.get("allPlays") or []
    return allp[-1] if allp else {}

def extract_statcast_line(live: dict) -> str:
    p = latest_play_from_feed(live) or {}
    hd = p.get("hitData") or {}
    if not hd:
        for ev in reversed(p.get("playEvents") or []):
            if ev.get("hitData"):
                hd = ev["hitData"]; break
    parts: List[str] = []
    try:
        ev = float(hd.get("launchSpeed")); parts.append(f"EV: {ev:.1f}")
    except Exception: pass
    try:
        la = float(hd.get("launchAngle")); parts.append(f"LA: {la:.1f}°")
    except Exception: pass
    try:
        dist = float(hd.get("totalDistance")); parts.append(f"Dist: {int(round(dist))} ft")
    except Exception: pass
    try:
        xba = hd.get("estimatedBAUsingSpeedAngle")
        if xba is not None: parts.append(f"xBA: {float(xba):.3f}".replace("0.", "."))
    except Exception: pass
    return " • ".join(parts)

def _shape_linescore(ls: dict) -> dict:
    if not ls: return {}
    innings = ls.get("innings") or []
    n = max((inn.get("num") or 0) for inn in innings) if innings else 9
    away_arr, home_arr = [], []
    for i in range(1, n+1):
        obj = next((x for x in innings if x.get("num")==i), {})
        a = (obj.get("away") or {}).get("runs")
        h = (obj.get("home") or {}).get("runs")
        away_arr.append(a if a is not None else "")
        home_arr.append(h if h is not None else "")
    teams = ls.get("teams") or {}
    a = teams.get("away") or {}
    h = teams.get("home") or {}
    return {
        "n": n,
        "away": away_arr,
        "home": home_arr,
        "totals": {
            "away": {"R": a.get("runs"), "H": a.get("hits"), "E": a.get("errors")},
            "home": {"R": h.get("runs"), "H": h.get("hits"), "E": h.get("errors")},
        }
    }

def _box_batting(box: dict, side: str) -> List[dict]:
    t = (box.get("teams") or {}).get(side) or {}
    players = (t.get("players") or {})
    order = t.get("battingOrder") or []
    ids: List[str] = []
    if isinstance(order, list) and order:
        ids = [s.replace("ID","") for s in order]
    else:
        tmp = []
        for pid,pobj in players.items():
            bo = str(pobj.get("battingOrder") or "")
            if bo:
                try: tmp.append((int(bo[:2]), pid.replace("ID","")))
                except Exception: pass
        tmp.sort()
        ids = [pid for _,pid in tmp] or [pid.replace("ID","") for pid in players.keys()]
    out = []
    for pid in ids:
        pobj = players.get(f"ID{pid}") or {}
        person = pobj.get("person") or {}
        pos = ((pobj.get("position") or {}).get("abbreviation")) or ""
        name = person.get("fullName") or person.get("boxscoreName") or person.get("lastInitName") or ""
        st = (pobj.get("stats") or {}).get("batting") or {}
        out.append({
            "pos": pos, "name": name,
            "ab": st.get("atBats"), "r": st.get("runs"), "h": st.get("hits"),
            "rbi": st.get("rbi"), "bb": st.get("baseOnBalls"), "k": st.get("strikeOuts"),
        })
    return out

def _box_pitching(box: dict, side: str) -> List[dict]:
    t = (box.get("teams") or {}).get(side) or {}
    players = (t.get("players") or {})
    out = []
    for _,pobj in players.items():
        st = (pobj.get("stats") or {}).get("pitching") or {}
        if not st: continue
        person = pobj.get("person") or {}
        name = person.get("fullName") or person.get("boxscoreName") or ""
        pos = ((pobj.get("position") or {}).get("abbreviation")) or "P"
        pitches = st.get("numberOfPitches") or st.get("pitchesThrown") or st.get("pitchCount")
        out.append({
            "pos": pos, "name": name, "ip": st.get("inningsPitched"), "h": st.get("hits"),
            "r": st.get("runs"), "er": st.get("earnedRuns"), "bb": st.get("baseOnBalls"),
            "k": st.get("strikeOuts"), "hr": st.get("homeRuns"), "p": pitches,
        })
    return out

def _lineup_from_box(box: dict, side: str) -> List[dict]:
    try:
        t = (box.get("teams") or {}).get(side) or {}
        players = (t.get("players") or {})
        order = t.get("battingOrder") or []
        ids = [str(pid).replace("ID","") for pid in order][:9]
        out = []
        for pid in ids:
            p = players.get(f"ID{pid}") or {}
            pos = ((p.get("position") or {}).get("abbreviation")) or ""
            nm = ((p.get("person") or {}).get("boxscoreName")
                  or (p.get("person") or {}).get("lastInitName")
                  or (p.get("person") or {}).get("fullName") or "")
            if nm:
                out.append({"pos": pos, "name": nm})
        return out
    except Exception:
        return []

def _count_from_live(live: dict) -> dict:
    try:
        bs = (((live.get("liveData") or {}).get("plays") or {}).get("currentPlay") or {}).get("count") or {}
        outs = (live.get("liveData") or {}).get("linescore", {}).get("outs", None)
        return {"balls": bs.get("balls"), "strikes": bs.get("strikes"), "outs": outs if outs is not None else bs.get("outs")}
    except Exception:
        return {}

def _bases_from_live(live: dict) -> dict:
    try:
        on = (((live.get("liveData") or {}).get("linescore") or {}).get("offense") or {})
        return {"first": bool(on.get("first")), "second": bool(on.get("second")), "third": bool(on.get("third"))}
    except Exception:
        return {}

def _due_up_from_live(live: dict) -> tuple[Optional[str], Optional[str]]:
    try:
        ld = (live.get("liveData") or {})
        du = (ld.get("linescore") or {}).get("offense", {})
        plays = (ld.get("plays") or {})
        next_up = (plays.get("currentPlay") or {}).get("about", {}).get("halfInning")
        half = str(next_up or "").lower()
        side = "away" if half == "top" else "home" if half == "bottom" else None
        due = []
        for key in ("batter","onDeck","inHole"):
            person = du.get(key) or {}
            name = person.get("lastInitName") or person.get("boxscoreName") or person.get("fullName")
            if name: due.append(name)
        return (side, ", ".join(due) if due else None)
    except Exception:
        return (None, None)

def _score_summary_from_pbp(pbp: dict) -> List[dict]:
    out = []
    plays = (pbp or {}).get("allPlays") or []
    a=h=0
    for p in plays:
        res = (p.get("result") or {})
        about = (p.get("about") or {})
        if not (res.get("rbi") or res.get("awayScore") or res.get("homeScore")):
            continue
        half = (about.get("halfInning") or "").lower()
        sym = "▲" if half=="top" else "▼" if half=="bottom" else ""
        inn = about.get("inning")
        a = about.get("awayScore", a)
        h = about.get("homeScore", h)
        out.append({
            "inning": f"{inn} {sym}".strip(),
            "play": res.get("description") or "",
            "away": a, "home": h
        })
    return out

def _decisions_text(live: dict) -> tuple[str,str,dict]:
    ids = {"winnerId": None, "loserId": None, "saveId": None}
    try:
        dec = (live.get("liveData") or {}).get("decisions") or {}
        w = (dec.get("winner") or {}); l = (dec.get("loser") or {}); s = (dec.get("save") or {})
        ids["winnerId"] = w.get("id"); ids["loserId"] = l.get("id"); ids["saveId"] = s.get("id")
        def nm(d): return d.get("fullName") or d.get("lastInitName") or d.get("boxscoreName") or ""
        parts = []
        if w: parts.append(f"W: {nm(w)}")
        if l: parts.append(f"L: {nm(l)}")
        if s: parts.append(f"SV: {nm(s)}")
        return " • ".join(parts), (f"SV: {nm(s)}" if s else ""), ids
    except Exception:
        return "", "", ids

# ---------------- PAGES ----------------
@app.route("/")
def home():
    return render_template("index.html")

@app.route("/todaysgames")
def todaysgames_page():
    return render_template("todaysgames.html")

@app.route("/game/<int:game_pk>")
def game_page(game_pk: int):
    return render_template("game.html", game_pk=game_pk, game=None)

@app.route("/ping")
def ping():
    return "ok", 200

# ---------------- API: GAMES GRID ----------------
@app.route("/api/games")
def api_games():
    d = request.args.get("date")
    if not d:
        try:
            dt = datetime.utcnow().replace(tzinfo=timezone.utc)
            if ET_TZ: dt = dt.astimezone(ET_TZ)  # type: ignore
            d = dt.strftime("%Y-%m-%d")
        except Exception:
            d = datetime.utcnow().strftime("%Y-%m-%d")

    if not hasattr(api_games, "_cache"):
        api_games._cache = {}
    cache = api_games._cache
    now = time.time()
    rec = cache.get(d)
    if rec and now - rec["t"] < 20:
        js = rec["v"]
    else:
        js = fetch_schedule_day(d)
        cache[d] = {"t": now, "v": js}

    dates = js.get("dates") or []
    games = (dates[0].get("games") or []) if dates else []

    out = []
    for g in games:
        try:
            game_pk = g.get("gamePk")
            status = _norm_status_from_sched(g)
            ls = g.get("linescore") or {}
            game_date_iso = g.get("gameDate") or ""

            teams = g.get("teams") or {}
            away = teams.get("away") or {}
            home = teams.get("home") or {}

            def team_obj(side_obj):
                t = side_obj or {}
                team = t.get("team") or {}
                rec = t.get("leagueRecord") or {}
                probable = (t.get("probablePitcher") or {})
                prob_line = None
                st = (probable.get("stats") or {})
                era=w=l=None
                if isinstance(st, list) and st:
                    try:
                        splits = st[0].get("splits") or []
                        if splits:
                            s = splits[0].get("stat") or {}
                            era = s.get("era"); w = s.get("wins"); l = s.get("losses")
                    except Exception:
                        pass
                if probable:
                    nm = probable.get("boxscoreName") or probable.get("lastInitName") or probable.get("fullName")
                    if nm and (era is not None) and (w is not None) and (l is not None):
                        prob_line = f"{nm} ({w}-{l}, {era} ERA)"
                    elif nm:
                        prob_line = nm
                record = ""
                if rec.get("wins") is not None and rec.get("losses") is not None:
                    record = f"{rec.get('wins')}-{rec.get('losses')}"
                return {
                    "id": team.get("id"),
                    "abbr": _abbr(team.get("id"), (team.get("abbreviation") or "")),
                    "record": record,
                    "probable": prob_line,
                }

            chip = _ls_chip(ls, status, game_date_iso)

            # Live enrich
            count = {}; bases = {}; last_play = ""; statcast = ""
            in_break = False; due_up_side = None; due_up = None
            current_pitcher = {"home": None, "away": None}
            current_batter  = {"home": None, "away": None}

            if status != "scheduled":
                live = fetch_live(game_pk)
                count = _count_from_live(live)
                bases = _bases_from_live(live)
                about = ((live.get("liveData") or {}).get("plays") or {}).get("currentPlay", {}).get("about", {})
                in_break = bool(((live.get("liveData") or {}).get("linescore") or {}).get("isInningBreak", False))
                due_up_side, due_up = _due_up_from_live(live)

                # current names (best-effort)
                boxlive = ((live.get("liveData") or {}).get("boxscore") or {})
                for side in ("home","away"):
                    curp = (boxlive.get("teams", {}).get(side, {}).get("pitchers") or [])
                    curb = (boxlive.get("teams", {}).get(side, {}).get("batters") or [])
                    players = (live.get("gameData") or {}).get("players") or {}
                    def name_for(pid):
                        p = players.get(f"ID{pid}") or {}
                        return p.get("lastInitName") or p.get("boxscoreName") or p.get("fullName")
                    current_pitcher[side] = name_for(curp[0]) if curp else None
                    current_batter[side]  = name_for(curb[0]) if curb else None

                lp = latest_play_from_feed(live) or {}
                last_play = (lp.get("result") or {}).get("description") or ""
                statcast = extract_statcast_line(live) or ""

            # scores
            away_score = (ls.get("teams") or {}).get("away", {}).get("runs")
            home_score = (ls.get("teams") or {}).get("home", {}).get("runs")

            shaped_ls = _shape_linescore(ls) if ls else _shape_linescore(fetch_linescore(game_pk))

            obj = {
                "gamePk": game_pk,
                "status": status,
                "chip": chip,
                "count": count,
                "bases": bases,
                "inBreak": in_break,
                "dueUpSide": due_up_side,
                "dueUp": due_up,
                "lastPlay": last_play,
                "statcast": statcast,
                "teams": {
                    "away": { **team_obj(away), "score": away_score, "currentPitcher": current_pitcher.get("away"), "currentBatter": current_batter.get("away"),
                              "finalPitcher": "", "savePitcher": "" },
                    "home": { **team_obj(home), "score": home_score, "currentPitcher": current_pitcher.get("home"), "currentBatter": current_batter.get("home"),
                              "finalPitcher": "", "savePitcher": "" },
                },
                "linescore": shaped_ls,
            }

            # Fetch boxscore for lineups always (even scheduled, if posted); add box tables when live/final
            try:
                box_any = fetch_box(game_pk) or {}
                obj["lineups"] = {
                    "away": _lineup_from_box(box_any, "away"),
                    "home": _lineup_from_box(box_any, "home"),
                }
                if status != "scheduled" and box_any:
                    obj["batting"]  = {"away": _box_batting(box_any, "away"), "home": _box_batting(box_any, "home")}
                    obj["pitching"] = {"away": _box_pitching(box_any, "away"), "home": _box_pitching(box_any, "home")}
                    # also add aliases some templates expect
                    obj["batters"]  = obj["batting"]
                    obj["pitchers"] = obj["pitching"]
                    # scoring summary
                    pbp = fetch_pbp(game_pk)
                    obj["scoreSummary"] = _score_summary_from_pbp(pbp)
                    obj["scoring"] = obj["scoreSummary"]
            except Exception:
                obj["lineups"] = {"away": [], "home": []}

            # final W/L/S text for final cards
            if status == "final":
                live = fetch_live(game_pk)
                dec_text, save_text, _ = _decisions_text(live)
                obj["teams"]["away"]["finalPitcher"] = dec_text or ""
                obj["teams"]["home"]["finalPitcher"] = ""
                obj["teams"]["away"]["savePitcher"]  = save_text or ""
                obj["teams"]["home"]["savePitcher"]  = ""

            out.append(obj)
        except Exception as e:
            log.info("build game failed %s", e)
            continue

    return jsonify({"date": d, "games": out})

# ---------------- API: SINGLE GAME ----------------
@app.route("/api/game/<int:game_pk>")
def api_game_detail(game_pk: int):
    sched = _get(f"{MLB_API}/schedule", {"gamePk": game_pk, "hydrate":"team,linescore"})
    games = (sched.get("dates", [{}])[0].get("games", []) if sched.get("dates") else [])
    shell = games[0] if games else {}
    status = _norm_status_from_sched(shell)
    ls = shell.get("linescore") or fetch_linescore(game_pk)

    live = fetch_live(game_pk)
    box = fetch_box(game_pk) if status != "scheduled" else {}
    pbp = fetch_pbp(game_pk)

    teams = (shell.get("teams") or {})
    away = teams.get("away") or {}; home = teams.get("home") or {}
    away_t = away.get("team") or {}; home_t = home.get("team") or {}

    def record(s):
        rec = s.get("leagueRecord") or {}
        if rec.get("wins") is not None and rec.get("losses") is not None:
            return f"{rec.get('wins')}-{rec.get('losses')}"
        return ""

    chip = _ls_chip(ls or {}, status, shell.get("gameDate") or "")
    shaped_ls = _shape_linescore(ls or {})
    dec_text, save_text, dec_ids = _decisions_text(live)
    last_play = (latest_play_from_feed(live).get("result") or {}).get("description") or ""
    statcast = extract_statcast_line(live) or ""

    out = {
        "gamePk": game_pk,
        "status": status,
        "chip": chip,
        "venue": (shell.get("venue") or {}).get("name"),
        "when": to_et_str(shell.get("gameDate") or ""),
        "count": _count_from_live(live),
        "bases": _bases_from_live(live),
        "lastPlay": last_play,
        "statcast": statcast,
        "teams": {
            "away": {"id": away_t.get("id"), "abbr": _abbr(away_t.get("id"), away_t.get("abbreviation") or ""), "record": record(away)},
            "home": {"id": home_t.get("id"), "abbr": _abbr(home_t.get("id"), home_t.get("abbreviation") or ""), "record": record(home)},
        },
        "linescore": shaped_ls,
        "batting": {"away": _box_batting(box, "away") if box else [], "home": _box_batting(box, "home") if box else []},
        "pitching": {"away": _box_pitching(box, "away") if box else [], "home": _box_pitching(box, "home") if box else []},
        "lineups": {"away": _lineup_from_box(box, "away") if box else [], "home": _lineup_from_box(box, "home") if box else []},
        "decisions": dec_ids,
        "decisionsText": dec_text,
        "saveText": save_text,
        "plays": [],
    }

    # shape pbp
    def _num(v, cast=float):
        try: return cast(v) if v is not None else None
        except Exception: return None

    plays_src = (pbp or {}).get("allPlays") or (((live or {}).get("liveData") or {}).get("plays") or {}).get("allPlays") or []
    shaped = []
    for p in plays_src:
        about = (p.get("about") or {})
        half  = (about.get("halfInning") or "").lower()
        sym   = "▲" if half == "top" else "▼" if half == "bottom" else ""
        inn   = about.get("inning")
        res = (p.get("result") or {})
        hd = p.get("hitData") or {}
        if not hd:
            for ev in reversed(p.get("playEvents") or []):
                if ev.get("hitData"):
                    hd = ev["hitData"]; break
        ev   = _num(hd.get("launchSpeed"))
        la   = _num(hd.get("launchAngle"))
        dist = _num(hd.get("totalDistance"), int)
        xba  = _num(hd.get("estimatedBAUsingSpeedAngle"))
        pitch_list = []
        for evn in (p.get("playEvents") or []):
            pd = evn.get("pitchData") or {}
            det = evn.get("details") or {}
            coords = (pd.get("coordinates") or {})
            if not pd and not coords and not det.get("isInPlay"):
                continue
            ptype = ((pd.get("pitchType") or {}).get("code")
                     or (pd.get("pitchType") or {}).get("description")
                     or (det.get("type") or {}).get("code")
                     or det.get("description") or "")
            pitch_list.append({
                "type": ptype,
                "velo": _num(pd.get("startSpeed")) or _num(pd.get("releaseSpeed")),
                "result": ((det.get("call") or {}).get("description") or det.get("description") or det.get("event") or ""),
                "px": _num(coords.get("pX")), "pz": _num(coords.get("pZ")),
                "code": ((det.get("call") or {}).get("code") or det.get("code")), "inPlay": bool(det.get("isInPlay")),
            })
        shaped.append({"inning": f"{inn} {sym}".strip(), "desc": res.get("description") or "", "ev": ev, "la": la, "dist": dist, "xba": xba, "pitches": pitch_list})
    out["plays"] = shaped

    return jsonify(out)

# ---------------- Standings passthrough (unchanged) ----------------
def fetch_standings(season: int) -> dict:
    base = f"{MLB_API}/standings"
    today = date.today().isoformat()
    common = {"season": str(season), "sportId":"1", "leagueId":"103,104", "hydrate":"team,league,division,record"}
    for p in [
        {**common, "standingsTypes":"byDivision", "date":today},
        {**common, "standingsType":"byDivision", "date":today},
        {**common, "standingsTypes":"regularSeason", "date":today},
        {**common, "standingsType":"regularSeason", "date":today},
        {**common, "standingsTypes":"byDivision"},
        {**common, "standingsType":"byDivision"},
        {**common, "standingsTypes":"regularSeason"},
        {**common, "standingsType":"regularSeason"},
    ]:
        js = _get(base, p, timeout=10)
        if js.get("records"): return js
    return {"records":[]}

@app.route("/standings")
def standings_page():
    try:
        season = date.today().year
        js = fetch_standings(season)
        return render_template("standings.html", data_division=js, data_wildcard={}, season=season, error=None)
    except Exception as e:
        return f"Standings error: {e}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)
