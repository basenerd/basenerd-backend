from flask import Flask, render_template, jsonify, request
import requests, logging
from datetime import datetime, timezone, date
from typing import List, Optional, Tuple

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

def _abbr(team_id: int, fallback: str="") -> str:
    try:
        return TEAM_ABBR.get(int(team_id)) or (fallback or "")
    except Exception:
        return fallback or ""

def _get(url: str, params: dict=None, timeout: int=15) -> dict:
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning("GET failed %s %s: %s", url, params, e)
        return {}

def fetch_schedule(ymd: str) -> dict:
    return _get(f"{MLB_API}/schedule", {"sportId": SPORT_ID, "date": ymd, "hydrate": "team,linescore,probablePitcher"})

def fetch_live(game_pk: int) -> dict:
    return _get(f"{MLB_API}/game/{game_pk}/feed/live")

def fetch_linescore(game_pk: int) -> dict:
    return _get(f"{MLB_API}/game/{game_pk}/linescore")

def fetch_box(game_pk: int) -> dict:
    return _get(f"{MLB_API}/game/{game_pk}/boxscore")

def fetch_standings(season: int) -> dict:
    return _get(f"{MLB_API}/standings", {"leagueId":"103,104","season":season, "hydrate":"team"})

# Time helpers
try:
    import zoneinfo
    ET_TZ = zoneinfo.ZoneInfo("America/New_York")
except Exception:
    ET_TZ = None

def to_et_str(iso_z: str) -> str:
    if not iso_z:
        return ""
    try:
        dt = datetime.fromisoformat(iso_z.replace("Z","+00:00"))
        if ET_TZ:
            dt = dt.astimezone(ET_TZ)  # type: ignore
            return dt.strftime("%-I:%M %p ET")
        return dt.strftime("%H:%M UTC")
    except Exception:
        return ""

# Shapers
def _norm_status_from_sched(g: dict) -> str:
    st_det = ((g.get("status") or {}).get("detailedState") or "").lower()
    st_abs = ((g.get("status") or {}).get("abstractGameState") or "").lower()
    if "final" in st_det or "completed" in st_det or "game over" in st_det or st_abs == "final":
        return "final"
    live_keys = ("in progress", "warmup", "manager challenge", "review", "resumed", "suspended")
    if any(k in st_det for k in live_keys) or st_abs == "live":
        return "in_progress"
    pre_keys = ("pre-game", "scheduled", "delayed start", "postponed", "makeup", "tbd", "preview")
    if any(k in st_det for k in pre_keys) or st_abs in ("preview","pre","scheduled"):
        return "scheduled"
    return "scheduled"

def latest_play_from_feed(live: dict) -> dict:
    ld = (live.get("liveData") or {})
    plays = (ld.get("plays") or {})
    cur = plays.get("currentPlay") or {}
    if cur:
        return cur
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
        if xba is not None: parts.append(f"xBA: {float(xba):.3f}".replace("0.","."))
    except Exception: pass
    try:
        xslg = hd.get("estimatedSLGUsingSpeedAngle")
        if xslg is not None: parts.append(f"xSLG: {float(xslg):.3f}")
    except Exception: pass
    return " • ".join(parts)

def _safe_pct(n: Optional[int], d: Optional[int]) -> Optional[str]:
    try:
        if d and d>0 and n is not None:
            return f"{n/d:.3f}".lstrip("0")
    except Exception:
        pass
    return None

def _shape_linescore(ls: dict) -> dict:
    if not ls: return {}
    innings = []
    for inn in ls.get("innings", []):
        innings.append({"num": inn.get("num"), "away": (inn.get("away") or {}).get("runs"), "home": (inn.get("home") or {}).get("runs")})
    totals = {
        "away": {"runs": (ls.get("teams") or {}).get("away", {}).get("runs"),
                 "hits": (ls.get("teams") or {}).get("away", {}).get("hits"),
                 "errors": (ls.get("teams") or {}).get("away", {}).get("errors")},
        "home": {"runs": (ls.get("teams") or {}).get("home", {}).get("runs"),
                 "hits": (ls.get("teams") or {}).get("home", {}).get("hits"),
                 "errors": (ls.get("teams") or {}).get("home", {}).get("errors")},
    }
    return {"innings": innings, "totals": totals}

def _box_batting(box: dict, side: str) -> List[dict]:
    t = (box.get("teams") or {}).get(side) or {}
    players = (t.get("players") or {})
    order = t.get("battingOrder") or []
    order_ids: List[str] = []
    if isinstance(order, list) and order:
        for pid in order:
            order_ids.append(str(pid).replace("ID",""))
    else:
        tmp = []
        for pid, pobj in players.items():
            bo = (pobj.get("battingOrder") or "")
            if bo:
                try:
                    slot = int(str(bo)[:2]); tmp.append((slot, pid.replace("ID","")))
                except Exception: pass
        tmp.sort(); order_ids = [pid for _,pid in tmp]
    if not order_ids:
        order_ids = [pid.replace("ID","") for pid in players.keys()]
    out: List[dict] = []
    for pid in order_ids:
        pobj = players.get(f"ID{pid}") or {}
        person = pobj.get("person") or {}
        stats = (pobj.get("stats") or {}).get("batting") or {}
        name = person.get("fullName") or person.get("lastInitName") or person.get("boxscoreName")
        ab = stats.get("atBats") or 0
        h = stats.get("hits") or 0
        rbi = stats.get("rbi")
        bb = stats.get("baseOnBalls")
        so = stats.get("strikeOuts")
        avg = _safe_pct(h, ab)
        out.append({"name": name, "AB": ab, "H": h, "BB": bb, "SO": so, "RBI": rbi, "AVG": avg})
    return out

def _box_pitching(box: dict, side: str) -> List[dict]:
    t = (box.get("teams") or {}).get(side) or {}
    players = (t.get("players") or {})
    out: List[dict] = []
    for pid, pobj in players.items():
        stats = (pobj.get("stats") or {}).get("pitching") or {}
        if not stats: 
            continue
        person = pobj.get("person") or {}
        name = person.get("fullName") or person.get("boxscoreName")
        ip = stats.get("inningsPitched")
        h = stats.get("hits")
        r = stats.get("runs")
        er = stats.get("earnedRuns")
        bb = stats.get("baseOnBalls")
        so = stats.get("strikeOuts")
        era = stats.get("era")
        out.append({"name": name, "IP": ip, "H": h, "R": r, "ER": er, "BB": bb, "SO": so, "ERA": era})
    return out

def _decisions(live: dict) -> Tuple[dict, str]:
    ids = {"winId": None, "lossId": None, "saveId": None}
    try:
        dec = (live.get("liveData") or {}).get("decisions") or {}
        w = (dec.get("winner") or {})
        l = (dec.get("loser") or {})
        s = (dec.get("save") or {})
        ids["winId"] = w.get("id"); ids["lossId"] = l.get("id"); ids["saveId"] = s.get("id")
        def nm(d): return d.get("fullName") or d.get("lastInitName") or d.get("boxscoreName") or ""
        parts = []
        if w: parts.append(f"W: {nm(w)}")
        if l: parts.append(f"L: {nm(l)}")
        if s: parts.append(f"SV: {nm(s)}")
        return ids, " • ".join(parts)
    except Exception:
        return ids, ""

# Pages
@app.route("/")
def home():
    return render_template("index.html")

@app.route("/todaysgames")
def todaysgames():
    return render_template("todaysgames.html")

@app.route("/standings")
def standings_page():
    try:
        season = date.today().year
        js = fetch_standings(season)
        data_division = {"American League": [], "National League": []}
        for rec in js.get("records", []):
            lg = (rec.get("league") or {}).get("name", "")
            data_division.setdefault(lg, []).append(rec)
        return render_template("standings.html", data_division=data_division)
    except Exception as e:
        log.exception("Could not render standings template: %s", e)
        return "Could not render standings template", 500

@app.route("/game/<int:game_pk>")
def game_page(game_pk: int):
    return render_template("game.html", game_pk=game_pk)

# APIs
@app.route("/api/games")
def api_games():
    # Determine date (ET by default)
    d = request.args.get("date")
    if not d:
        try:
            dt = datetime.utcnow().replace(tzinfo=timezone.utc)
            if ET_TZ:
                dt = dt.astimezone(ET_TZ)  # type: ignore
            d = dt.strftime("%Y-%m-%d")
        except Exception:
            d = datetime.utcnow().strftime("%Y-%m-%d")

    try:
        js = fetch_schedule(d)
    except Exception as e:
        log.exception("/api/games schedule fetch failed: %s", e)
        return jsonify({"date": d, "games": [], "error": "schedule_failed"}), 200

    out = []

    for date_obj in (js.get("dates") or []):
        for g in (date_obj.get("games") or []):
            try:
                game_pk = g.get("gamePk")
                status = _norm_status_from_sched(g)
                venue = ((g.get("venue") or {}).get("name")) or ""

                # Chip
                try:
                    if status == "scheduled":
                        chip = to_et_str(g.get("gameDate"))
                    else:
                        ls_sched = (g.get("linescore") or {})
                        inning = ls_sched.get("currentInning")
                        inning_state = (ls_sched.get("inningState") or "").title()
                        chip = (f"{inning_state} {inning}" if inning else inning_state).strip()
                except Exception:
                    chip = ""

                teams = (g.get("teams") or {})
                def _t(side: str) -> dict:
                    try:
                        tt = teams.get(side) or {}
                        tm = tt.get("team") or {}
                        rec = tt.get("leagueRecord") or {}
                        obj = {
                            "id": tm.get("id"),
                            "name": tm.get("name"),
                            "abbr": _abbr(tm.get("id"), tm.get("abbreviation","")),
                            "score": tt.get("score"),
                            "record": f"{rec.get('wins')}-{rec.get('losses')}" if rec else ""
                        }
                        prob = (tt.get("probablePitcher") or {})
                        if prob:
                            obj["probable"] = prob.get("fullName") or prob.get("name")
                        return obj
                    except Exception:
                        return {"id": None, "name": "", "abbr": "", "score": None, "record": ""}

                item = {
                    "gamePk": game_pk,
                    "status": status,
                    "chip": chip,
                    "venue": venue,
                    "teams": {"home": _t("home"), "away": _t("away")},
                }

                # Default shape keys so frontend never sees undefined
                item.setdefault("lastPlay", "")
                item.setdefault("statcast", "")
                item.setdefault("bases", {"first": False, "second": False, "third": False})
                item.setdefault("dueUpSide", None)
                item.setdefault("dueUp", None)
                item.setdefault("linescore", None)
                item.setdefault("batters", {"away": [], "home": []})
                item.setdefault("pitchers", {"away": [], "home": []})

                ls_sched = (g.get("linescore") or {})
                item["inning"] = ls_sched.get("currentInning")
                item["isTop"] = (ls_sched.get("isTopInning") is True)
                item["inBreak"] = ((ls_sched.get("inningState") or "").lower() in ("end","middle"))

                if status != "scheduled":
                    try:
                        live = fetch_live(game_pk) or {}
                    except Exception as e:
                        log.warning("live fetch failed for %s: %s", game_pk, e)
                        live = {}

                    # Last play + statcast
                    try:
                        play = latest_play_from_feed(live)
                        item["lastPlay"] = ((play.get("result") or {}).get("description")) or ""
                    except Exception:
                        item["lastPlay"] = ""
                    try:
                        item["statcast"] = extract_statcast_line(live) or ""
                    except Exception:
                        item["statcast"] = ""

                    # offense/defense context
                    try:
                        ld = (live.get("liveData") or {})
                        ls = ld.get("linescore") or {}
                        offense = ls.get("offense") or {}
                        defense = ls.get("defense") or {}

                        def _nm(pobj):
                            if not pobj: return None
                            return pobj.get("fullName") or pobj.get("name")

                        item["bases"] = {
                            "first":  bool(offense.get("first")  or offense.get("onFirst")),
                            "second": bool(offense.get("second") or offense.get("onSecond")),
                            "third":  bool(offense.get("third")  or offense.get("onThird")),
                        }

                        batter = _nm(offense.get("batter"))
                        pitcher = _nm(defense.get("pitcher"))

                        home_id = item["teams"]["home"]["id"]
                        away_id = item["teams"]["away"]["id"]
                        off_team_id = ((offense.get("team") or {}).get("id"))
                        item["dueUpSide"] = "home" if off_team_id == home_id else ("away" if off_team_id == away_id else None)

                        if item["dueUpSide"] == "home":
                            item["teams"]["home"]["currentBatter"] = batter
                            item["teams"]["away"]["currentPitcher"] = pitcher
                        elif item["dueUpSide"] == "away":
                            item["teams"]["away"]["currentBatter"] = batter
                            item["teams"]["home"]["currentPitcher"] = pitcher

                        on_deck = _nm(offense.get("onDeck"))
                        in_hole = _nm(offense.get("inHole"))
                        item["dueUp"] = ", ".join([n for n in (batter, on_deck, in_hole) if n]) or None
                    except Exception as e:
                        log.warning("context parse failed for %s: %s", game_pk, e)

                    # Linescore & box
                    try:
                        item["linescore"] = _shape_linescore(fetch_linescore(game_pk))
                    except Exception as e:
                        log.warning("linescore failed for %s: %s", game_pk, e)
                        item["linescore"] = None
                    try:
                        box = fetch_box(game_pk)
                        item["batters"]  = {"away": _box_batting(box, "away"), "home": _box_batting(box, "home")}
                        item["pitchers"] = {"away": _box_pitching(box, "away"), "home": _box_pitching(box, "home")}
                    except Exception as e:
                        log.warning("box failed for %s: %s", game_pk, e)
                        item["batters"] = item["pitchers"] = None

                    if status == "final":
                        try:
                            _, dec_text = _decisions(live)
                            item["teams"]["home"]["finalPitcher"] = dec_text
                            item["teams"]["away"]["finalPitcher"] = dec_text
                        except Exception:
                            pass

                out.append(item)

            except Exception as e:
                log.exception("Error shaping game %s: %s", (g.get("gamePk")), e)
                continue

    return jsonify({"date": d, "games": out}), 200

@app.route("/api/standings")
def api_standings():
    season = request.args.get("season", default=date.today().year, type=int)
    return jsonify(fetch_standings(season))

@app.route("/api/game/<int:game_pk>")
def api_game(game_pk: int):
    g = _get(f"{MLB_API}/schedule", {"gamePk": game_pk, "hydrate": "team,linescore"})
    games = (g.get("dates", [{}])[0].get("games", []) if g.get("dates") else [])
    if not games:
        return jsonify({"error":"not found"}), 404
    sched = games[0]
    status = _norm_status_from_sched(sched)
    date_iso = sched.get("gameDate")
    teams = (sched.get("teams") or {})
    home_t = (teams.get("home") or {}).get("team", {}) or {}
    away_t = (teams.get("away") or {}).get("team", {}) or {}

    resp = {
        "status": status,
        "chip": to_et_str(date_iso) if status=="scheduled" else "",
        "venue": (sched.get("venue") or {}).get("name", ""),
        "date": to_et_str(date_iso),
        "teams": {
            "home": {"id": home_t.get("id"), "name": home_t.get("name"), "abbr": _abbr(home_t.get("id"), home_t.get("abbreviation","")), "score": None},
            "away": {"id": away_t.get("id"), "name": away_t.get("name"), "abbr": _abbr(away_t.get("id"), away_t.get("abbreviation","")), "score": None},
        },
    }

    live = fetch_live(game_pk)
    resp["lastPlay"] = ((latest_play_from_feed(live).get("result") or {}).get("description")) if live else ""
    resp["statcast"] = extract_statcast_line(live) if live else ""
    ls_full = fetch_linescore(game_pk)
    resp["linescore"] = _shape_linescore(ls_full)
    box = fetch_box(game_pk)
    resp["batters"]  = {"away": _box_batting(box, "away"), "home": _box_batting(box, "home")}
    resp["pitchers"] = {"away": _box_pitching(box, "away"), "home": _box_pitching(box, "home")}
    try:
        _, dec_text = _decisions(live)
        resp["decisions"] = dec_text
    except Exception:
        resp["decisions"] = ""

    return jsonify(resp)

@app.route("/ping")
def ping():
    return "ok", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
