from flask import Flask, render_template, jsonify, request
import requests, time, logging
from datetime import datetime, timezone, date
from typing import Dict, Any, Tuple, List, Optional

# -------------------- App & logging --------------------
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("basenerd")

# -------------------- Constants --------------------
MLB_API = "https://statsapi.mlb.com/api/v1"
SPORT_ID = 1

# Some teams miss abbreviations in feed; harden with a map
TEAM_ABBR = {
    109:"ARI",144:"ATL",110:"BAL",111:"BOS",112:"CHC",145:"CHW",113:"CIN",114:"CLE",115:"COL",116:"DET",
    117:"HOU",118:"KCR",108:"LAA",119:"LAD",146:"MIA",158:"MIL",142:"MIN",121:"NYM",147:"NYY",133:"OAK",
    143:"PHI",134:"PIT",135:"SDP",136:"SEA",137:"SFG",138:"STL",139:"TBR",140:"TEX",141:"TOR",120:"WSH"
}

try:
    import pytz
    ET_TZ = pytz.timezone("America/New_York")
except Exception:
    ET_TZ = None

# -------------------- Tiny cache --------------------
_CACHE: Dict[str, Dict[str, Any]] = {}

def cache_get(key: str):
    ent = _CACHE.get(key)
    if not ent: return None
    if ent["ts"] + ent["ttl"] < time.time():
        _CACHE.pop(key, None)
        return None
    return ent["data"]

def cache_set(key: str, data: Any, ttl: int = 60):
    _CACHE[key] = {"ts": time.time(), "ttl": ttl, "data": data}

# -------------------- HTTP helpers --------------------
def http_json(url, params=None, timeout=20):
    r = requests.get(url, params=params or {}, timeout=timeout,
                     headers={"User-Agent":"basenerd/1.0"})
    r.raise_for_status()
    return r.json()

def to_et_str(iso_z: str) -> str:
    if not iso_z: return ""
    try:
        dt = datetime.fromisoformat(iso_z.replace("Z","+00:00"))
        if ET_TZ:
            dt = dt.astimezone(ET_TZ)
            return dt.strftime("%-I:%M %p ET")
        return dt.strftime("%H:%M UTC")
    except Exception:
        return ""

# -------------------- Baseball Savant helpers --------------------
def fetch_savant_gf(game_pk: int):
    """Baseball Savant 'gf' endpoint for xBA/xSLG by play."""
    key = f"savant_gf:{game_pk}"
    c = cache_get(key)
    if c is not None: return c
    data = http_json("https://baseballsavant.mlb.com/gf", {"game_pk": game_pk}, timeout=25)
    cache_set(key, data, ttl=20)
    return data

def shape_game(live: dict, season: int):
    """
    Minimal shaper for the pieces the templates expect:
      - chip (inningState + currentInning)
      - teams.home/away: id, name, abbr, score
      - gamePk (for lookups)
    """
    ld = (live.get("liveData") or {})
    gd = (live.get("gameData") or {})

    ls = ld.get("linescore") or {}
    teams_gd = gd.get("teams") or {}
    home = teams_gd.get("home") or {}
    away = teams_gd.get("away") or {}

    # scoreboard chip
    chip = ((ls.get("inningState") or "") + (" " + str(ls.get("currentInning")) if ls.get("currentInning") else "")).strip()

    # scores
    ls_teams = (ls.get("teams") or {})
    h_score = (ls_teams.get("home") or {}).get("runs")
    a_score = (ls_teams.get("away") or {}).get("runs")

    # abbr fallback
    def _abbr(team_obj):
        tid = team_obj.get("id")
        fallback = team_obj.get("abbreviation") or (team_obj.get("teamName") or "")[:3].upper()
        try:
            # you already have TEAM_ABBR in this file
            return TEAM_ABBR.get(tid, fallback)
        except Exception:
            return fallback

    shaped = {
        "gamePk": gd.get("gamePk") or ld.get("gamePk") or (gd.get("game") or {}).get("pk") or (ld.get("game") or {}).get("pk"),
        "chip": chip,
        "teams": {
            "home": {
                "id": home.get("id"),
                "name": home.get("name") or home.get("teamName"),
                "abbr": _abbr(home),
                "score": h_score,
            },
            "away": {
                "id": away.get("id"),
                "name": away.get("name") or away.get("teamName"),
                "abbr": _abbr(away),
                "score": a_score,
            },
        },
    }
    return shaped


def _savant_pick(d, keys):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None

def build_savant_x_lookup(game_pk: int):
    """
    Returns:
      x_by_playid: { play_id -> {"xba": float|None, "xslg": float|None} }
      x_by_trip:   { (inning, half('home'/'away'), ab_number) -> {...} }
    """
    js = fetch_savant_gf(game_pk) or {}
    x_by_playid, x_by_trip = {}, {}
    for side_key in ("home_pitchers", "away_pitchers"):
        groups = js.get(side_key) or {}
        for arr in groups.values():
            for row in arr:
                pid = row.get("play_id") or row.get("playId") or row.get("play_uuid")
                xba  = _savant_pick(row, ["xba", "estimated_ba_using_speedangle", "estimated_ba", "estimatedBA"])
                xslg = _savant_pick(row, ["xslg", "estimated_slg_using_speedangle", "estimated_slg", "estimatedSLG"])
                def _f(v):
                    try:
                        v = float(v)
                        return v/100.0 if v > 1.5 else v
                    except Exception:
                        return None
                xba_v, xslg_v = _f(xba), _f(xslg)
                if pid and (xba_v is not None or xslg_v is not None):
                    x_by_playid[pid] = {"xba": xba_v, "xslg": xslg_v}
                inn   = row.get("inning")
                half  = (row.get("team_batting") or "").lower()  # "home"/"away"
                ab_no = row.get("ab_number")
                if inn is not None and half in ("home","away") and ab_no is not None:
                    x_by_trip[(int(inn), half, int(ab_no))] = {"xba": xba_v, "xslg": xslg_v}
    return x_by_playid, x_by_trip

def _game_pk_from_live(live: dict):
    gd = (live.get("gameData") or {})
    ld = (live.get("liveData") or {})
    return (gd.get("gamePk") or ld.get("gamePk") or
            (gd.get("game") or {}).get("pk") or (ld.get("game") or {}).get("pk"))

def _find_hit_event_meta(play):
    """Return (hitData, ev_play_id, inning, half('top'/'bottom'), atBatIndex)."""
    about = (play.get("about") or {})
    inning = about.get("inning")
    half = (about.get("halfInning") or "").lower()
    ab_idx = about.get("atBatIndex")
    for ev in reversed(play.get("playEvents") or []):
        hd = ev.get("hitData")
        if not hd: continue
        ev_play_id = ev.get("playId") or (ev.get("details") or {}).get("playId") or ev.get("playGuid")
        return hd, ev_play_id, inning, half, ab_idx
    return None, None, inning, half, ab_idx

# -------------------- Extractors --------------------
def latest_play_from_feed(live: dict) -> dict:
    ld = (live.get("liveData") or {})
    plays = (ld.get("plays") or {}).get("allPlays") or []
    return plays[-1] if plays else {}

def extract_statcast_line(live: dict) -> str:
    play = latest_play_from_feed(live)
    hd, ev_play_id, *_ = _find_hit_event_meta(play)
    if not hd: return ""
    ev = hd.get("launchSpeed"); la = hd.get("launchAngle"); dist = hd.get("totalDistance")
    xba_s = xslg_s = None
    game_pk = _game_pk_from_live(live)
    if game_pk:
        try:
            x_by_playid, _ = build_savant_x_lookup(int(game_pk))
            if ev_play_id and ev_play_id in x_by_playid:
                xba_val  = x_by_playid[ev_play_id]["xba"]
                xslg_val = x_by_playid[ev_play_id]["xslg"]
                if xba_val is not None:  xba_s  = f"{float(xba_val):.3f}".replace("0.", ".")
                if xslg_val is not None: xslg_s = f"{float(xslg_val):.3f}"
        except Exception:
            pass
    parts = []
    try: parts.append(f"EV: {float(ev):.1f}")
    except: pass
    try: parts.append(f"LA: {float(la):.1f}°")
    except: pass
    try:
        dval = float(dist); parts.append(f"Dist: {int(round(dval))} ft")
    except: pass
    if xba_s:  parts.append(f"xBA: {xba_s}")
    if xslg_s: parts.append(f"xSLG: {xslg_s}")
    return " • ".join(parts)

def _fmt_one(v):  # 1 decimal
    if v in (None, "", "-"): return None
    try: return f"{float(v):.1f}"
    except Exception: return None

def _fmt_int(v):  # integer
    if v in (None, "", "-"): return None
    try: return f"{float(v):.0f}"
    except Exception: return None

def _fmt_prob(v):  # .3f with leading 0 dropped
    if v is None: return None
    try:
        s = f"{float(v):.3f}"
        return s.replace("0.", ".")
    except Exception:
        return None

def extract_play_by_play(live: dict, limit: int = 300) -> List[Dict[str, Any]]:
    """
    PBP with EV/LA/Dist, real xBA, pitch-by-pitch, and ball-in-play coordinates.
    """
    ld = (live.get("liveData") or {})
    allp = (ld.get("plays") or {}).get("allPlays") or []

    # Savant lookup once per game
    game_pk = _game_pk_from_live(live)
    x_by_playid, x_by_trip = ({}, {})
    if game_pk:
        try:
            x_by_playid, x_by_trip = build_savant_x_lookup(int(game_pk))
        except Exception:
            pass

    def _extract_pitch_sequence_local(play: dict):
        seq = []
        for ev in (play.get("playEvents") or []):
            det = ev.get("details") or {}
            if not ev.get("isPitch", False):
                if not det.get("type") and not det.get("call"):  # skip non-pitches
                    continue
            pdat = ev.get("pitchData") or {}
            coords = (pdat.get("coordinates") or ev.get("coordinates") or {}) or {}
            px = coords.get("pX") if "pX" in coords else coords.get("px")
            pz = coords.get("pZ") if "pZ" in coords else coords.get("pz")
            sz_top = pdat.get("strikeZoneTop"); sz_bot = pdat.get("strikeZoneBottom")
            ptype = (det.get("type") or {}).get("code")
            velo = pdat.get("startSpeed") or det.get("startSpeed")
            call = (det.get("call") or {}).get("description") or det.get("description") or ""
            call_code = (det.get("call") or {}).get("code")
            in_play = bool(det.get("isInPlay") or (call_code == "X"))
            seq.append({
                "type": ptype, "velo": velo, "result": call, "code": call_code,
                "inPlay": in_play, "px": px, "pz": pz, "sz_top": sz_top, "sz_bot": sz_bot
            })
        return seq

    out = []
    for p in allp[-limit:]:
        about = (p.get("about") or {})
        res   = (p.get("result") or {})
        count = (p.get("count") or {})
        half_word = (about.get("halfInning") or "").lower()
        half_symbol = "▲" if half_word == "top" else ("▼" if half_word == "bottom" else "")
        inning_num = about.get("inning")
        inning_label = f"{half_symbol} {inning_num}" if inning_num else ""

        # batted ball
        hd, ev_play_id, inn_meta, half_meta, ab_idx = _find_hit_event_meta(p)

        ev = hd.get("launchSpeed")      if hd else None
        la = hd.get("launchAngle")      if hd else None
        dist = hd.get("totalDistance")  if hd else None

        # Savant xBA exact play match, fallback to (inning,half,ab)
        xba_val = None
        if ev_play_id and ev_play_id in x_by_playid:
            xba_val = x_by_playid[ev_play_id].get("xba")
        else:
            half_key = "home" if half_meta == "bottom" else ("away" if half_meta == "top" else None)
            if half_key and inn_meta is not None and ab_idx is not None:
                x_obj = (x_by_trip.get((int(inn_meta), half_key, int(ab_idx))) or
                         x_by_trip.get((int(inn_meta), half_key, int(ab_idx) + 1)))
                if x_obj: xba_val = x_obj.get("xba")

        pitches = _extract_pitch_sequence_local(p)

        bip = {}
        if hd:
            hcoord = (hd.get("coordinates") or {})
            bip = {"x": hcoord.get("coordX") or hcoord.get("x"),
                   "y": hcoord.get("coordY") or hcoord.get("y")}

        out.append({
            "inningNum": inning_num,
            "half": half_symbol,
            "inning": inning_label,
            "desc": res.get("description") or "",
            "balls": count.get("balls"),
            "strikes": count.get("strikes"),
            "outs": count.get("outs"),
            "away": res.get("awayScore"),
            "home": res.get("homeScore"),
            "ev":   _fmt_one(ev),
            "la":   _fmt_one(la),
            "dist": _fmt_int(dist),
            "xba":  _fmt_prob(xba_val),
            "pitches": pitches,
            "bip": bip,
        })
    return out

def _shape_linescore(ls: dict) -> dict:
    if not ls: return {}
    innings = ls.get("innings") or []
    n = len(innings)
    out = {"n": n, "away": [], "home": [], "totals": {"away": {}, "home": {}}}
    for inn in innings:
        out["away"].append((inn.get("away") or {}).get("runs"))
        out["home"].append((inn.get("home") or {}).get("runs"))
    totals = ls.get("teams") or {}
    away = totals.get("away") or {}
    home = totals.get("home") or {}
    out["totals"]["away"] = {"R": away.get("runs"), "H": away.get("hits"), "E": away.get("errors")}
    out["totals"]["home"] = {"R": home.get("runs"), "H": home.get("hits"), "E": home.get("errors")}
    return out

def _abbr(team_id: int, fallback: str = "") -> str:
    return TEAM_ABBR.get(team_id, fallback or "")

def _team_record_from_live_or_sched(live, side) -> str:
    gd = (live.get("gameData") or {})
    teams = gd.get("teams") or {}
    rec = (teams.get(side) or {}).get("leagueRecord")
    if isinstance(rec, dict):
        w, l = rec.get("wins"), rec.get("losses")
        if w is not None and l is not None:
            return f"{w}-{l}"
    # fallback via schedule on the game date
    try:
        dt = (gd.get("datetime") or {}).get("dateTime")
        iso_date = dt.split("T")[0] if dt else None
        if iso_date:
            sched = fetch_schedule(iso_date)
            for d in sched.get("dates", []):
                for g in d.get("games", []):
                    if g.get("gamePk") == gd.get("gamePk"):
                        rec2 = ((g.get("teams") or {}).get(side) or {}).get("leagueRecord") or {}
                        w, l = rec2.get("wins"), rec2.get("losses")
                        if w is not None and l is not None:
                            return f"{w}-{l}"
    except Exception:
        pass
    return ""

def _box_batting(box: dict, side: str) -> List[dict]:
    t = (box.get("teams") or {}).get(side) or {}
    players = (t.get("players") or {})
    order = t.get("battingOrder") or []
    rows = []
    for pid in order:
        p = players.get(pid) or {}
        person = p.get("person") or {}
        pos = ((p.get("position") or {}).get("abbreviation")) or ""
        st = (p.get("stats") or {}).get("batting") or {}
        rows.append({
            "pos": pos, "name": person.get("fullName"),
            "ab": st.get("atBats"), "r": st.get("runs"), "h": st.get("hits"),
            "rbi": st.get("rbi"), "bb": st.get("baseOnBalls"), "k": st.get("strikeOuts")
        })
    return rows

def _box_pitching(box: dict, side: str) -> List[dict]:
    t = (box.get("teams") or {}).get(side) or {}
    players = (t.get("players") or {})
    pids = t.get("pitchers") or []
    rows = []
    for pid in pids:
        key = f"ID{pid}"
        p = players.get(key) or {}
        person = p.get("person") or {}
        st = (p.get("stats") or {}).get("pitching") or {}
        rows.append({
            "pid": pid, "pos": "P", "name": person.get("fullName"),
            "ip": st.get("inningsPitched"), "h": st.get("hits"),
            "r": st.get("runs"), "er": st.get("earnedRuns"),
            "bb": st.get("baseOnBalls"), "k": st.get("strikeOuts"),
            "hr": st.get("homeRuns"), "p": st.get("pitchesThrown"),
        })
    return rows

def _decisions(live: dict) -> Tuple[dict, str]:
    dec = (live.get("liveData") or {}).get("decisions") or {}
    players = (live.get("gameData") or {}).get("players") or {}
    def _name(pid):
        return (players.get(f"ID{pid}") or {}).get("fullName")
    w_id = dec.get("winner", {}).get("id")
    l_id = dec.get("loser", {}).get("id")
    s_id = dec.get("save", {}).get("id")
    parts = []
    if w_id: parts.append(f"W: {_name(w_id)}")
    if l_id: parts.append(f"L: {_name(l_id)}")
    if s_id: parts.append(f"SV: {_name(s_id)}")
    return {"winnerId": w_id, "loserId": l_id, "saveId": s_id}, " • ".join(parts)

def _shape_header(live: dict) -> dict:
    gd = (live.get("gameData") or {})
    ld = (live.get("liveData") or {})
    teams = (gd.get("teams") or {})
    home_t = teams.get("home") or {}
    away_t = teams.get("away") or {}

    ls = ld.get("linescore") or {}
    scores = {
        "home": (ls.get("teams") or {}).get("home", {}).get("runs"),
        "away": (ls.get("teams") or {}).get("away", {}).get("runs"),
    }
    home_id = home_t.get("id"); away_id = away_t.get("id")

    shaped = {
        "status": (gd.get("status") or {}).get("abstractGameState"),
        "chip": ((ls.get("inningState") or "") + (" " + str(ls.get("currentInning")) if ls.get("currentInning") else "")).strip(),
        "venue": (gd.get("venue") or {}).get("name", ""),
        "date": to_et_str((gd.get("datetime") or {}).get("dateTime")),
        "statcast": extract_statcast_line(live) or "",
        "teams": {
            "home": {"id": home_id, "name": home_t.get("name"), "abbr": _abbr(home_id, home_t.get("abbreviation","")), "score": scores["home"]},
            "away": {"id": away_id, "name": away_t.get("name"), "abbr": _abbr(away_id, away_t.get("abbreviation","")), "score": scores["away"]},
        },
    }
    # Back-compat top-level (your templates referenced game.home / game.away in header)
    shaped["home"] = {**shaped["teams"]["home"], "record": _team_record_from_live_or_sched(live, "home")}
    shaped["away"] = {**shaped["teams"]["away"], "record": _team_record_from_live_or_sched(live, "away")}
    return shaped

# -------------------- Fetchers --------------------
def fetch_schedule(iso_date: str) -> dict:
    key = f"sched:{iso_date}"
    c = cache_get(key)
    if c is not None: return c
    js = http_json(f"{MLB_API}/schedule", params={"sportId": SPORT_ID, "date": iso_date, "language":"en"})
    cache_set(key, js, ttl=60)
    return js

def fetch_standings(season: Optional[int]=None) -> dict:
    if season is None: season = date.today().year
    key = f"standings:{season}"
    c = cache_get(key)
    if c is not None: return c
    js = http_json(f"{MLB_API}/standings", params={"leagueId": "103,104","season": season, "standingsTypes":"regularSeason"})
    cache_set(key, js, ttl=300)
    return js

def fetch_live(game_pk: int) -> dict:
    key = f"live:{game_pk}"
    c = cache_get(key)
    if c is not None: return c
    js = http_json(f"{MLB_API}/game/{game_pk}/feed/live")
    cache_set(key, js, ttl=10)
    return js

def fetch_box(game_pk: int) -> dict:
    key = f"box:{game_pk}"
    c = cache_get(key)
    if c is not None: return c
    js = http_json(f"{MLB_API}/game/{game_pk}/boxscore")
    cache_set(key, js, ttl=20)
    return js

# -------------------- Pages --------------------
@app.route("/")
def home():
    return render_template("index.html")

@app.route("/todaysgames")
def todaysgames():
    return render_template("todaysgames.html")

@app.route("/standings")
def standings_page():
    return render_template("standings.html")

@app.route("/game/<int:game_pk>")
def game_page(game_pk: int):
    try:
        live = fetch_live(game_pk)
        shaped = _shape_header(live)
    except Exception:
        shaped = {"home": {"id":0,"name":"Home","score":"-","record":""}, "away":{"id":0,"name":"Away","score":"-","record":""}, "status":"", "venue":"", "date":""}
    return render_template("game.html", game=shaped, game_pk=game_pk)

# -------------------- APIs --------------------
@app.route("/api/games")
def api_games():
    d = request.args.get("date")
    if not d:
        # today ET
        dt = datetime.utcnow().replace(tzinfo=timezone.utc)
        if ET_TZ: dt = dt.astimezone(ET_TZ)
        d = dt.strftime("%Y-%m-%d")
    js = fetch_schedule(d)
    out = []
    for date_obj in js.get("dates", []):
        for g in date_obj.get("games", []):
            game_pk = g.get("gamePk")
            status = (g.get("status") or {}).get("detailedState")
            venue = (g.get("venue") or {}).get("name")
            if status == "Scheduled":
                chip = to_et_str(g.get("gameDate"))
            else:
                ls = (g.get("linescore") or {})
                chip = ((ls.get("inningState") or "") + (" " + str(ls.get("currentInning")) if ls.get("currentInning") else "")).strip()
            tms = g.get("teams") or {}
            h = tms.get("home") or {}
            a = tms.get("away") or {}
            out.append({
                "gamePk": game_pk,
                "status": status,
                "chip": chip,
                "venue": venue,
                "home": {
                    "id": (h.get("team") or {}).get("id"), "name": (h.get("team") or {}).get("name"),
                    "abbr": _abbr((h.get("team") or {}).get("id"), (h.get("team") or {}).get("abbreviation","")),
                    "score": h.get("score"),
                },
                "away": {
                    "id": (a.get("team") or {}).get("id"), "name": (a.get("team") or {}).get("name"),
                    "abbr": _abbr((a.get("team") or {}).get("id"), (a.get("team") or {}).get("abbreviation","")),
                    "score": a.get("score"),
                },
            })
    return jsonify({"date": d, "games": out})

@app.route("/api/standings")
def api_standings():
    season = request.args.get("season", type=int)
    js = fetch_standings(season)
    return jsonify(js)

@app.route("/api/game/<int:game_pk>")
def api_game(game_pk: int):
    """
    Returns JSON used by game.html:
      game: header info, linescore, batters/pitchers
      plays: PBP list with EV/LA/Dist/xBA/pitches/bip
      meta: decisions and text
    """
    try:
        live = fetch_live(game_pk)
        box = fetch_box(game_pk)
        shaped = _shape_header(live)
        shaped["linescore"] = _shape_linescore((live.get("liveData") or {}).get("linescore"))
        shaped["batters"] = {
            "away": _box_batting(box, "away"),
            "home": _box_batting(box, "home"),
        }
        shaped["pitchers"] = {
            "away": _box_pitching(box, "away"),
            "home": _box_pitching(box, "home"),
        }
        dec_ids, dec_text = _decisions(live)
        meta = {"decisions": dec_ids, "decisionsText": dec_text}
        return jsonify({
            "game": shaped,
            "plays": extract_play_by_play(live=live),
            "meta": meta
        })
    except Exception as e:
        log.exception("detail fetch failed for %s", game_pk)
        return jsonify({"error": f"detail_error: {e}"}), 200

# -------------------- Health --------------------
@app.route("/ping")
def ping():
    return "ok", 200

# -------------------- Main --------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
