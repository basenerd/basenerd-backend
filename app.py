
from flask import Flask, render_template, jsonify, request 
import requests, logging, time
from datetime import datetime, timezone, date, timedelta
from typing import List, Tuple

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("basenerd")

MLB_API = "https://statsapi.mlb.com/api/v1"
SPORT_ID = 1

# Minimal team id -> abbr map (used for schedule cards/standings)
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

def _get(url: str, params: dict = None, timeout: int = 15) -> dict:
    try:
        r = requests.get(url, params=params, timeout=timeout, headers={"User-Agent":"Basenerd/1.0"})
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning("GET failed %s %s: %s", url, params, e)
        return {}

# ---------- MLB fetchers ----------
def fetch_schedule(ymd: str) -> dict:
    """
    Robust schedule fetcher. Tries multiple param shapes that MLB StatsAPI
    sometimes requires. Returns the first non-empty result; otherwise an empty day.
    """
    season = (ymd or str(date.today())).split("-")[0]
    hydrate = f"team,linescore,probablePitcher,probablePitcher(stats(group=pitching,type=season,season={season}))"

    # Build a US-style date as an extra fallback (some hosts are picky)
    try:
        _us = datetime.strptime(ymd, "%Y-%m-%d").strftime("%m/%d/%Y")
    except Exception:
        _us = None

    base = f"{MLB_API}/schedule"
    attempts = [
        {"sportId": SPORT_ID, "date": ymd, "hydrate": hydrate},
        {"sportId": SPORT_ID, "startDate": ymd, "endDate": ymd, "hydrate": hydrate},
        {"sportId": SPORT_ID, "date": ymd, "gameTypes": "R", "hydrate": hydrate},
        {"sportId": SPORT_ID, "startDate": ymd, "endDate": ymd, "gameTypes": "R", "hydrate": hydrate},
        # simple no-hydrate try
        {"sportId": SPORT_ID, "date": ymd},
    ]
    # EXTRA fallbacks: US date format + leagueId/scheduleType that some gateways require
    if _us:
        attempts.extend([
            {"sportId": SPORT_ID, "date": _us, "leagueId": "103,104"},
            {"sportId": SPORT_ID, "startDate": _us, "endDate": _us, "leagueId": "103,104"},
            {"sportId": SPORT_ID, "date": _us, "scheduleType": "games"},
        ])

    for params in attempts:
        try:
            js = _get(base, params) or {}
            dates = js.get("dates") or []
            games = (dates[0].get("games") or []) if dates else []
            if games:
                return js
        except Exception:
            continue

    # Safe empty shape
    return {"dates": [{"date": ymd, "games": []}]}

def fetch_live(game_pk: int) -> dict:
    return _get(f"{MLB_API}/game/{game_pk}/feed/live")

def fetch_linescore(game_pk: int) -> dict:
    return _get(f"{MLB_API}/game/{game_pk}/linescore")

def fetch_box(game_pk: int) -> dict:
    return _get(f"{MLB_API}/game/{game_pk}/boxscore")

def fetch_pbp(game_pk: int) -> dict:
    # Dedicated, stable PBP endpoint
    return _get(f"{MLB_API}/game/{game_pk}/playByPlay")

def fetch_team_record(sched_team: dict) -> str:
    """Return 'W-L' from schedule's leagueRecord if present."""
    try:
        rec = (sched_team or {}).get("leagueRecord") or {}
        w, l = rec.get("wins"), rec.get("losses")
        if w is None or l is None:
            return ""
        return f"{w}-{l}"
    except Exception:
        return ""

# ---------- time helpers ----------
try:
    import zoneinfo
    ET_TZ = zoneinfo.ZoneInfo("America/New_York")
except Exception:
    ET_TZ = None

def to_et_str(iso_z: str) -> str:
    if not iso_z:
        return ""
    try:
        dt = datetime.fromisoformat(iso_z.replace("Z", "+00:00"))
        if ET_TZ:
            dt = dt.astimezone(ET_TZ)  # type: ignore
            return dt.strftime("%-I:%M %p ET")
        return dt.strftime("%H:%M UTC")
    except Exception:
        return ""

# ---------- shaping helpers ----------
def _norm_status_from_sched(g: dict) -> str:
    st_det = ((g.get("status") or {}).get("detailedState") or "").lower()
    st_abs = ((g.get("status") or {}).get("abstractGameState") or "").lower()
    if "final" in st_det or "completed" in st_det or "game over" in st_det or st_abs == "final":
        return "final"
    live_keys = ("in progress", "warmup", "manager challenge", "review", "resumed", "suspended", "delayed")
    if any(k in st_det for k in live_keys) or st_abs == "live":
        return "in_progress"
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
                hd = ev["hitData"]
                break
    parts: List[str] = []
    try:
        ev = float(hd.get("launchSpeed")); parts.append(f"EV: {ev:.1f}")
    except Exception:
        pass
    try:
        la = float(hd.get("launchAngle")); parts.append(f"LA: {la:.1f}°")
    except Exception:
        pass
    try:
        dist = float(hd.get("totalDistance")); parts.append(f"Dist: {int(round(dist))} ft")
    except Exception:
        pass
    try:
        xba = hd.get("estimatedBAUsingSpeedAngle")
        if xba is not None:
            parts.append(f"xBA: {float(xba):.3f}".replace("0.", "."))
    except Exception:
        pass
    try:
        xslg = hd.get("estimatedSLGUsingSpeedAngle")
        if xslg is not None:
            parts.append(f"xSLG: {float(xslg):.3f}")
    except Exception:
        pass
    return " • ".join(parts)

def _shape_linescore(ls: dict) -> dict:
    if not ls:
        return {}
    innings = ls.get("innings") or []
    n = max((inn.get("num") or 0) for inn in innings) if innings else 9

    away_arr, home_arr = [], []
    for inn in range(1, n + 1):
        obj = next((x for x in innings if x.get("num") == inn), {})
        a = (obj.get("away") or {}).get("runs")
        h = (obj.get("home") or {}).get("runs")
        away_arr.append(a if a is not None else "")
        home_arr.append(h if h is not None else "")

    teams_tot = ls.get("teams") or {}
    away_tot = teams_tot.get("away") or {}
    home_tot = teams_tot.get("home") or {}

    totals = {
        "away": {"R": away_tot.get("runs"), "H": away_tot.get("hits"), "E": away_tot.get("errors")},
        "home": {"R": home_tot.get("runs"), "H": home_tot.get("hits"), "E": home_tot.get("errors")},
    }
    return {"n": n, "away": away_arr, "home": home_arr, "totals": totals}

def _box_batting(box: dict, side: str) -> List[dict]:
    t = (box.get("teams") or {}).get(side) or {}
    players = (t.get("players") or {})
    order = t.get("battingOrder") or []

    # Build order of player ids (strings without 'ID' prefix)
    order_ids: List[str] = []
    if isinstance(order, list) and order:
        order_ids = [str(pid).replace("ID", "") for pid in order]
    else:
        tmp = []
        for pid, pobj in players.items():
            bo = str(pobj.get("battingOrder") or "")
            if bo:
                try:
                    slot = int(bo[:2]); tmp.append((slot, pid.replace("ID", "")))
                except Exception:
                    pass
        tmp.sort()
        order_ids = [pid for _, pid in tmp] or [pid.replace("ID", "") for pid in players.keys()]

    out: List[dict] = []
    for pid in order_ids:
        pobj = players.get(f"ID{pid}") or {}
        person = pobj.get("person") or {}
        pos = ((pobj.get("position") or {}).get("abbreviation")) or ""
        name = person.get("fullName") or person.get("boxscoreName") or person.get("lastInitName") or ""
        st = (pobj.get("stats") or {}).get("batting") or {}
        row = {
            "pos": pos,
            "name": name,
            "ab": st.get("atBats"),
            "r": st.get("runs"),
            "h": st.get("hits"),
            "rbi": st.get("rbi"),
            "bb": st.get("baseOnBalls"),
            "k": st.get("strikeOuts"),
        }
        out.append(row)

    return out

def _shape_pbp(live: dict | None, pbp: dict | None) -> list[dict]:
    plays_src = (pbp or {}).get("allPlays") or (((live or {}).get("liveData") or {}).get("plays") or {}).get("allPlays") or []
    out = []

    def _num(v, cast=float):
        try:
            return cast(v) if v is not None else None
        except Exception:
            return None

    for p in plays_src:
        about = (p.get("about") or {})
        half  = (about.get("halfInning") or "").lower()
        sym   = "▲" if half == "top" else "▼" if half == "bottom" else ""
        inn   = about.get("inning")
        inning_label = f"{inn} {sym}".strip() if inn else ""

        res = (p.get("result") or {})
        # Get Statcast contact data (root or last pitch with hitData)
        hd = p.get("hitData") or {}
        if not hd:
            for ev in reversed(p.get("playEvents") or []):
                if ev.get("hitData"):
                    hd = ev["hitData"]
                    break

        ev   = _num(hd.get("launchSpeed"))
        la   = _num(hd.get("launchAngle"))
        dist = _num(hd.get("totalDistance"), int)
        xba  = _num(hd.get("estimatedBAUsingSpeedAngle"))

        # Build pitch list
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
                "result": ((det.get("call") or {}).get("description")
                           or det.get("description")
                           or det.get("event")
                           or ""),
                "px": _num(coords.get("pX")),
                "pz": _num(coords.get("pZ")),
                "code": ((det.get("call") or {}).get("code") or det.get("code")),
                "inPlay": bool(det.get("isInPlay")),
            })

        out.append({
            "inning": inning_label,
            "desc": res.get("description") or "",
            "ev": ev,
            "la": la,
            "dist": dist,
            "xba": xba,
            "xBA": xba,
            "pitches": pitch_list,
        })

    return out

def _box_pitching(box: dict, side: str) -> List[dict]:
    t = (box.get("teams") or {}).get(side) or {}
    players = (t.get("players") or {})
    out: List[dict] = []

    for _, pobj in players.items():
        st = (pobj.get("stats") or {}).get("pitching") or {}
        if not st:
            continue
        person = pobj.get("person") or {}
        name = person.get("fullName") or person.get("boxscoreName") or ""
        pos = ((pobj.get("position") or {}).get("abbreviation")) or "P"
        pitches = st.get("numberOfPitches") or st.get("pitchesThrown") or st.get("pitchCount")

        row = {
            "pos": pos,
            "name": name,
            "ip": st.get("inningsPitched"),
            "h": st.get("hits"),
            "r": st.get("runs"),
            "er": st.get("earnedRuns"),
            "bb": st.get("baseOnBalls"),
            "k": st.get("strikeOuts"),
            "hr": st.get("homeRuns"),
            "p": pitches,
        }
        out.append(row)
    return out

def _decisions(live: dict) -> Tuple[dict, str, str]:
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
        dec_text = " • ".join(parts)
        save_text = f"SV: {nm(s)}" if s else ""
        return ids, dec_text, save_text
    except Exception:
        return ids, "", ""

def _lineup_from_box(box: dict, side: str) -> List[dict]:
    """Return [{pos,name}] for first 9 in batting order if present."""
    try:
        t = (box.get("teams") or {}).get(side) or {}
        players = (t.get("players") or {})
        order = t.get("battingOrder") or []
        ids = [str(pid).replace("ID", "") for pid in order][:9]
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

# ---------- pages ----------
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
        records = js.get("records") or []
        if not isinstance(records, list):
            records = []

        # ---------- helpers (kept INSIDE the route & try:) ----------
        def last10_from(tr) -> str:
            if not isinstance(tr, dict):
                return ""
            recs = tr.get("records") or []
            if isinstance(recs, list):
                for r in recs:
                    if isinstance(r, dict) and (r.get("type") or "").lower().replace(" ", "") in ("lastten", "last_10"):
                        w, l = r.get("wins"), r.get("losses")
                        if isinstance(w, int) and isinstance(l, int):
                            return f"{w}-{l}"
            if isinstance(tr.get("records"), dict):
                for key in ("splitRecords", "overallRecords", "records"):
                    arr = tr["records"].get(key) if isinstance(tr["records"].get(key), list) else []
                    for r in arr:
                        if isinstance(r, dict) and (r.get("type") or "").lower().replace(" ", "") in ("lastten", "last_10"):
                            w, l = r.get("wins"), r.get("losses")
                            if isinstance(w, int) and isinstance(l, int):
                                return f"{w}-{l}"
            w10, l10 = tr.get("lastTenWins"), tr.get("lastTenLosses")
            if isinstance(w10, int) and isinstance(l10, int):
                return f"{w10}-{l10}"
            return ""

        def streak_from(tr) -> str:
            if not isinstance(tr, dict):
                return ""
            s = tr.get("streak")
            if isinstance(s, dict) and s.get("streakCode"):
                return str(s.get("streakCode")).strip()
            sc = tr.get("streakCode")
            return str(sc).strip() if sc is not None else ""

        def gb_from(tr) -> str:
            if not isinstance(tr, dict):
                return "—"
            gb = tr.get("gamesBack")
            return "—" if gb in (None, "", "0.0", "0", 0, "-", "—") else str(gb)

        def pct_from(tr) -> float:
            if not isinstance(tr, dict):
                return 0.0
            try:
                val = tr.get("pct") or tr.get("winningPercentage") or 0.0
                return float(val)
            except Exception:
                return 0.0

        def run_diff_from(tr):
            if not isinstance(tr, dict):
                return ""
            if tr.get("runDifferential") is not None:
                return tr.get("runDifferential")
            try:
                rs = int(tr.get("runsScored") or 0)
                ra = int(tr.get("runsAllowed") or 0)
                return rs - ra
            except Exception:
                return ""

        def get_abbr(team_id, team_name):
            try:
                v = _abbr(team_id, "")
                if v:
                    return v
            except Exception:
                pass
            tail = (team_name or "").split()[-1].upper()
            return (tail[:3] if tail else "TBD")

        # ---------- build division tables ----------
        data_division = {"American League": [], "National League": []}

        for rec in records:
            if not isinstance(rec, dict):
                continue
            league_obj   = rec.get("league")   if isinstance(rec.get("league"), dict)   else {}
            division_obj = rec.get("division") if isinstance(rec.get("division"), dict) else {}
            league_name = (league_obj.get("name") or "").strip() or "League"
            div_name    = (division_obj.get("name") or "Division").strip()

            rows = []
            team_records = rec.get("teamRecords") or []
            if not isinstance(team_records, list):
                team_records = []

            for tr in team_records:
                if not isinstance(tr, dict):
                    continue
                team  = tr.get("team") if isinstance(tr.get("team"), dict) else {}
                tid   = team.get("id")
                tname = team.get("name") or ""
                rows.append({
                    "team_id":   tid,
                    "team_name": tname,
                    "team_abbr": get_abbr(tid, tname),
                    "w":   tr.get("wins"),
                    "l":   tr.get("losses"),
                    "pct": pct_from(tr),
                    "gb":  gb_from(tr),
                    "streak": streak_from(tr),
                    "last10": last10_from(tr),
                    "runDiff": run_diff_from(tr),
                })

            rows.sort(key=lambda r: (r.get("pct", 0.0), (r.get("runDiff") or 0)), reverse=True)
            data_division.setdefault(league_name, []).append({"division": div_name, "rows": rows})

        # ---------- build wild card tables ----------
        def wc_gb(leader_w, leader_l, w, l):
            try:
                return round(((leader_w - w) + (l - leader_l)) / 2.0, 1)
            except Exception:
                return ""

        def _div_tag(name: str) -> str:
            n = (name or "").lower()
            if "east" in n: return "E"
            if "central" in n: return "C"
            if "west" in n: return "W"
            return ""

        data_wildcard = {}

        for league in ("American League", "National League"):
            blocks = data_division.get(league) or []

            # 1) Division leaders for the "leaders" mini-card (E/C/W badge)
            leaders = []
            for block in blocks:
                rows = block.get("rows") or []
                if not rows:
                    continue
                leader = rows[0]  # first row is division leader (already sorted)
                leaders.append({
                    "division_tag": _div_tag(block.get("division")),
                    "team_id":   leader.get("team_id"),
                    "team_name": leader.get("team_name"),
                    "team_abbr": leader.get("team_abbr"),
                    "w": leader.get("w"),
                    "l": leader.get("l"),
                    "pct": leader.get("pct"),
                })

            # 2) Wild Card pool = all non-division winners
            leader_ids = {x.get("team_id") for x in leaders if x.get("team_id") is not None}
            pool = []
            for block in blocks:
                for r in (block.get("rows") or []):
                    if r.get("team_id") not in leader_ids:
                        pool.append(r)

            # Sort pool by win% desc, then runDiff desc
            pool.sort(key=lambda r: (r.get("pct") or 0.0, r.get("runDiff") or 0), reverse=True)

            # Cut line = WC3 if present, else best team in pool
            if len(pool) >= 3:
                cut = pool[2]
                cut_w = cut.get("w") or 0
                cut_l = cut.get("l") or 0
            elif pool:
                cut_w = pool[0].get("w") or 0
                cut_l = pool[0].get("l") or 0
            else:
                cut_w = cut_l = 0

            # 3) Build rows
            rows = []
            for i, r in enumerate(pool):
                badge = f"WC{i+1}" if i < 3 else ""
                rows.append({
                    "badge": badge,
                    "team_id":   r.get("team_id"),
                    "team_name": r.get("team_name"),
                    "team_abbr": r.get("team_abbr"),
                    "w":   r.get("w"),
                    "l":   r.get("l"),
                    "pct": r.get("pct"),
                    "wc_gb": wc_gb(cut_w, cut_l, r.get("w") or 0, r.get("l") or 0),
                    "streak": r.get("streak"),
                    "last10": r.get("last10"),
                    "runDiff": r.get("runDiff"),
                })

            data_wildcard[league] = {
                "leaders": leaders,
                "rows": rows,
            }

        return render_template(
            "standings.html",
            data_division=data_division,
            data_wildcard=data_wildcard,
            season=season,
            error=None
        )

    except Exception as e:
        return f"Standings error: {type(e).__name__}: {e}", 500

@app.route("/game/<int:game_pk>")
def game_page(game_pk: int):
    game_ctx = None
    try:
        g = _get(f"{MLB_API}/schedule", {"gamePk": game_pk, "hydrate": "team"})
        games = (g.get("dates", [{}])[0].get("games", []) if g.get("dates") else [])
        if games:
            sched = games[0]
            teams = sched.get("teams") or {}
            home_t = (teams.get("home") or {}).get("team", {}) or {}
            away_t = (teams.get("away") or {}).get("team", {}) or {}
            game_ctx = {
                "home": {"id": home_t.get("id"), "name": home_t.get("name"), "record": ""},
                "away": {"id": away_t.get("id"), "name": away_t.get("name"), "record": ""},
                "venue": (sched.get("venue") or {}).get("name", ""),
                "status": _norm_status_from_sched(sched),
                "date": to_et_str(sched.get("gameDate") or ""),
            }
    except Exception:
        game_ctx = None
    return render_template("game.html", game_pk=game_pk, game=game_ctx)

# ---------- APIs ----------
@app.route("/api/games")
def api_games():
    # --- pick date in ET (UI expects ET) ---
    d = request.args.get("date")
    if not d:
        try:
            dt = datetime.utcnow().replace(tzinfo=timezone.utc)
            if ET_TZ:
                dt = dt.astimezone(ET_TZ)  # type: ignore
            d = dt.strftime("%Y-%m-%d")
        except Exception:
            d = datetime.utcnow().strftime("%Y-%m-%d")

    # --- tiny in-proc cache for schedule (20s) ---
    if not hasattr(api_games, "_sched_cache"):
        api_games._sched_cache = {}
    _cache = api_games._sched_cache
    now = time.time()

    def cache_get(key):
        rec = _cache.get(key)
        if rec and (now - rec["t"] < 20):
            return rec["v"]
        return None

    def cache_put(key, val):
        _cache[key] = {"t": now, "v": val}

    def _sched_once(day: str, params: dict) -> dict:
        """Single schedule request with given params; 4s timeout."""
        base = f"{MLB_API}/schedule"
        try:
            r = requests.get(
                base,
                params=params,
                timeout=4,
                headers={"Accept": "application/json", "User-Agent": "Basenerd/1.0"},
            )
            r.raise_for_status()
            return r.json() or {}
        except Exception as e:
            try: log.info("schedule fetch failed %s params=%s: %s", day, params, e)
            except Exception: pass
            return {"dates": [{"date": day, "games": []}]}

    def _fetch_sched_with_fallback(day: str) -> tuple[str, list]:
        """
        Try multiple known-good param combos that some hosts require.
        Order: date, start/end, and each again with gameTypes=R.
        Uses the tiny cache to avoid hammering.
        """
        cached = cache_get(day)
        if cached is not None:
            dates = cached.get("dates") or []
            return day, ((dates[0].get("games") or []) if dates else [])

        season = (day or str(date.today())).split("-")[0]
        hydrate = (
            f"team,linescore,probablePitcher,"
            f"probablePitcher(stats(group=pitching,type=season,season={season}))"
        )

        attempts = [
            {"sportId": SPORT_ID, "date": day, "hydrate": hydrate},
            {"sportId": SPORT_ID, "startDate": day, "endDate": day, "hydrate": hydrate},
            {"sportId": SPORT_ID, "date": day, "gameTypes": "R", "hydrate": hydrate},
            {"sportId": SPORT_ID, "startDate": day, "endDate": day, "gameTypes": "R", "hydrate": hydrate},
        ]

        js = {}
        games = []
        for params in attempts:
            js = _sched_once(day, params)
            dates = js.get("dates") or []
            games = (dates[0].get("games") or []) if dates else []
            if games:
                break

        cache_put(day, js)
        return day, games

    def _norm_status(g: dict) -> str:
        s = ((g.get("status") or {}).get("detailedState") or "").lower()
        if "in progress" in s or "playing" in s or "delayed" in s:
            return "in_progress"
        if "final" in s or "game over" in s or "completed" in s:
            return "final"
        return "scheduled"

    # ---------- build response ----------
    out = []
    date_used, games = _fetch_sched_with_fallback(d)
    if not games:
        orig_date = d
        try:
            base_date = date.fromisoformat(d)
        except Exception:
            base_date = date.today()
        found = False
        for delta in (-1, 1):
            dd = (base_date + timedelta(days=delta)).isoformat()
            _du, _games = _fetch_sched_with_fallback(dd)
            if _games:
                date_used, games = _du, _games
                found = True
                break
        if not found:
            date_used = orig_date  # keep the date the client asked for

    if not games:
        return jsonify({"date": date_used, "games": []}), 200

    for g in games:
        try:
            game_pk = g.get("gamePk")
            status = _norm_status(g)
            ls_sched = (g.get("linescore") or {})
            inning = ls_sched.get("currentInning")
            inning_state = (ls_sched.get("inningState") or "").title()
            chip = to_et_str(g.get("gameDate")) if status == "scheduled" else (f"{inning_state} {inning}".strip() if inning_state else "")

            teams = (g.get("teams") or {})
            def _team(side: str) -> dict:
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
                    obj["probable"] = (lambda p: (p.get("fullName") or p.get("name") or ""))(prob)
                    nm = obj["probable"]
                    w = l = era = None
                    for stat in (prob.get("stats") or []):
                        for split in (stat.get("splits") or []):
                            s = split.get("stat") or {}
                            w = w or s.get("wins"); l = l or s.get("losses"); era = era or s.get("era")
                    if w is not None and l is not None and era:
                        obj["probable"] = f"{nm} ({w}-{l}, {era} ERA)"
                return obj

            item = {
                "gamePk": game_pk,
                "venue": ((g.get("venue") or {}).get("name")) or "",
                "status": status,
                "chip": chip,
                "teams": {"away": _team("away"), "home": _team("home")},
                "bases": {"first": False, "second": False, "third": False},
                "dueUpSide": None,
                "dueUp": None,
                "count": None,
                "inBreak": ((ls_sched.get("inningState") or "").lower() in ("end", "middle")),
                "inning": inning,
                "isTop": (ls_sched.get("isTopInning") is True),
                "lastPlay": "",
                "statcast": "",
                "linescore": None,
                "batters": {"away": [], "home": []},
                "pitchers": {"away": [], "home": []},
                "scoring": [],
                "lineups": {"away": [], "home": []},
            }

            # ---------- Live/Final enrichment (surgical block) ----------
            if status != "scheduled":
                live_full = fetch_live(game_pk) or {}
                try:
                    ld = (live_full.get("liveData") or {})
                    ls = (ld.get("linescore") or {})
                    plays = (ld.get("plays") or {})
                    cur = (plays.get("currentPlay") or {})
                    res = (cur.get("result") or {})

                    # last play & statcast
                    item["lastPlay"] = res.get("description") or ""
                    item["statcast"] = extract_statcast_line(live_full) or ""

                    # live count & outs (prefer linescore, fallback to currentPlay)
                    balls = ls.get("balls"); strikes = ls.get("strikes"); outs = ls.get("outs")
                    if balls is None or strikes is None:
                        cnt = cur.get("count") or {}
                        balls = balls if balls is not None else cnt.get("balls")
                        strikes = strikes if strikes is not None else cnt.get("strikes")
                    if outs is None:
                        outs = (cur.get("about") or {}).get("outs")
                    if (balls is not None) or (strikes is not None) or (outs is not None):
                        item["count"] = {"balls": balls, "strikes": strikes, "outs": outs}

                    # inning chip + flags
                    state = (ls.get("inningState") or "").title()
                    inn = ls.get("currentInning")
                    item["chip"] = (f"{state} {inn}".strip() if state else item["chip"]) or item["chip"]
                    item["inBreak"] = ((ls.get("inningState") or "").lower() in ("end", "middle"))
                    item["inning"] = inn or item["inning"]
                    item["isTop"] = bool(ls.get("isTopInning"))

                    # bases / due up
                    offense = (ls.get("offense") or {})
                    defense = (ls.get("defense") or {})
                    item["bases"] = {
                        "first":  bool(offense.get("first") or offense.get("onFirst")),
                        "second": bool(offense.get("second") or offense.get("onSecond")),
                        "third":  bool(offense.get("third")  or offense.get("onThird")),
                    }
                    def _nm(p):
                        return (p or {}).get("lastInitName") or (p or {}).get("boxscoreName") or (p or {}).get("fullName") or (p or {}).get("name")
                    batter = offense.get("batter") or {}
                    on_deck = offense.get("onDeck") or {}
                    in_hole = offense.get("inHole") or {}
                    item["dueUp"] = ", ".join([x for x in (_nm(batter), _nm(on_deck), _nm(in_hole)) if x]) or None

                    home_id = item["teams"]["home"]["id"]
                    away_id = item["teams"]["away"]["id"]
                    off_team_id = (offense.get("team") or {}).get("id")
                    item["dueUpSide"] = "home" if off_team_id == home_id else ("away" if off_team_id == away_id else None)

                    # current pitcher & batter names
                    def name_of(person):
                        return (person or {}).get("boxscoreName") or (person or {}).get("lastInitName") or (person or {}).get("fullName") or ""
                    cur_pitcher_name = name_of(defense.get("pitcher"))
                    cur_batter_name  = name_of(offense.get("batter"))

                    # seed names
                    if off_team_id == home_id:
                        item["teams"]["home"]["currentBatter"] = cur_batter_name
                        item["teams"]["away"]["currentPitcher"] = cur_pitcher_name
                        if item["inBreak"]:
                            item["teams"]["away"]["breakPitcher"] = cur_pitcher_name
                    elif off_team_id == away_id:
                        item["teams"]["away"]["currentBatter"] = cur_batter_name
                        item["teams"]["home"]["currentPitcher"] = cur_pitcher_name
                        if item["inBreak"]:
                            item["teams"]["home"]["breakPitcher"] = cur_pitcher_name

                    # linescore table
                    def _shape_linescore_from_live(linescore: dict) -> dict | None:
                        try:
                            innings = linescore.get("innings") or []
                            n = max(9, len(innings))
                            away = ["" for _ in range(n)]; home = ["" for _ in range(n)]
                            for i, innobj in enumerate(innings[:n]):
                                a = (innobj.get("away") or {}).get("runs")
                                h = (innobj.get("home") or {}).get("runs")
                                away[i] = a if a is not None else ""
                                home[i] = h if h is not None else ""
                            tots = {
                                "away": {
                                    "R": (linescore.get("teams") or {}).get("away", {}).get("runs"),
                                    "H": (linescore.get("teams") or {}).get("away", {}).get("hits"),
                                    "E": (linescore.get("teams") or {}).get("away", {}).get("errors"),
                                },
                                "home": {
                                    "R": (linescore.get("teams") or {}).get("home", {}).get("runs"),
                                    "H": (linescore.get("teams") or {}).get("home", {}).get("hits"),
                                    "E": (linescore.get("teams") or {}).get("home", {}).get("errors"),
                                },
                            }
                            return {"n": n, "away": away, "home": home, "totals": tots}
                        except Exception:
                            return None
                    item["linescore"] = _shape_linescore_from_live(ls)

                    # scoring summary
                    try:
                        plays = (ld.get("plays") or {})
                        idxs = plays.get("scoringPlays") or []
                        allp = plays.get("allPlays") or []
                        play_by_id = {p.get("playId"): p for p in allp if p.get("playId")}
                        norm = []
                        for ref in idxs:
                            if isinstance(ref, int) and 0 <= ref < len(allp):
                                p = allp[ref]
                            elif isinstance(ref, str):
                                p = play_by_id.get(ref, {})
                            else:
                                p = {}
                            about = p.get("about") or {}
                            resd = p.get("result") or {}
                            norm.append({
                                "inning": about.get("inning"),
                                "play": resd.get("description") or "",
                                "away": about.get("awayScore"),
                                "home": about.get("homeScore"),
                            })
                        item["scoring"] = norm
                    except Exception:
                        item["scoring"] = []
                except Exception as e:
                    log.info("live enrich failed %s: %s", game_pk, e)

                # live box scores + current labels
                try:
                    box = fetch_box(game_pk) or {}

                    def _box_batting_rows(side: str):
                        t = (box.get("teams") or {}).get(side) or {}
                        players = (t.get("players") or {})
                        order = t.get("battingOrder") or []

                        order_ids = [str(pid).replace("ID", "") for pid in order] if order else []
                        if not order_ids:
                            tmp = []
                            for pid, pobj in players.items():
                                bo = str(pobj.get("battingOrder") or "")
                                if bo:
                                    try:
                                        slot = int(bo[:2]); tmp.append((slot, pid.replace("ID", "")))
                                    except Exception:
                                        pass
                            tmp.sort()
                            order_ids = [pid for _, pid in tmp] or [pid.replace("ID", "") for pid in players.keys()]

                        out_rows = []
                        for pid in order_ids:
                            pobj = players.get(f"ID{pid}") or {}
                            person = pobj.get("person") or {}
                            pos = ((pobj.get("position") or {}).get("abbreviation")) or ""
                            name = person.get("fullName") or person.get("boxscoreName") or person.get("lastInitName") or ""
                            st = (pobj.get("stats") or {}).get("batting") or {}
                            out_rows.append({
                                "pos": pos, "name": name,
                                "ab": st.get("atBats"), "r": st.get("runs"), "h": st.get("hits"),
                                "rbi": st.get("rbi"), "bb": st.get("baseOnBalls"), "k": st.get("strikeOuts"),
                            })
                        return out_rows

                    def _box_pitching_rows(side: str):
                        t = (box.get("teams") or {}).get(side) or {}
                        players = (t.get("players") or {})
                        rows = []
                        for _, pobj in players.items():
                            st = (pobj.get("stats") or {}).get("pitching") or {}
                            if not st: 
                                continue
                            person = pobj.get("person") or {}
                            name = person.get("fullName") or person.get("boxscoreName") or ""
                            pos = ((pobj.get("position") or {}).get("abbreviation")) or "P"
                            pitches = st.get("numberOfPitches") or st.get("pitchesThrown") or st.get("pitchCount")
                            rows.append({
                                "pos": pos, "name": name, "ip": st.get("inningsPitched"),
                                "h": st.get("hits"), "r": st.get("runs"), "er": st.get("earnedRuns"),
                                "bb": st.get("baseOnBalls"), "k": st.get("strikeOuts"),
                                "hr": st.get("homeRuns"), "p": pitches,
                            })
                        return rows

                    # bat/pit tables
                    item["batters"]  = {"away": _box_batting_rows("away"), "home": _box_batting_rows("home")}
                    item["pitchers"] = {"away": _box_pitching_rows("away"), "home": _box_pitching_rows("home")}

                    # lineups for todaysgames scheduled dropdowns if available
                    try:
                        item["lineups"]["away"] = _lineup_from_box(box, "away")
                        item["lineups"]["home"] = _lineup_from_box(box, "home")
                    except Exception:
                        pass

                    # add concise stat lines to current pitcher/batter labels
                    def _pitch_line(rows):
                        if not rows: return ""
                        r = rows[0]
                        parts = []
                        if r.get("ip"): parts.append(f"IP {r['ip']}")
                        if r.get("h") is not None: parts.append(f"H {r['h']}")
                        if r.get("er") is not None: parts.append(f"ER {r['er']}")
                        if r.get("k") is not None: parts.append(f"K {r['k']}")
                        if r.get("bb") is not None: parts.append(f"BB {r['bb']}")
                        return ", ".join(parts)

                    def _bat_line(rows):
                        if not rows: return ""
                        r = rows[0]
                        hits = r.get("h"); abs_ = r.get("ab"); rbi = r.get("rbi"); hr = r.get("hr")
                        parts = []
                        if hits is not None and abs_ is not None:
                            parts.append(f"{hits}-{abs_}")
                        if rbi: parts.append(f"RBI {rbi}")
                        if hr: parts.append(f"HR {hr}")
                        return ", ".join(parts)

                    for side in ("home", "away"):
                        if item["teams"][side].get("currentPitcher"):
                            line = _pitch_line(item["pitchers"]["home" if side=="home" else "away"])
                            if line:
                                item["teams"][side]["currentPitcher"] += f" ({line})"
                        if item["teams"][side].get("currentBatter"):
                            line = _bat_line(item["batters"]["home" if side=="home" else "away"])
                            if line:
                                item["teams"][side]["currentBatter"] += f" ({line})"

                except Exception as e:
                    log.info("boxscore enrich failed %s: %s", game_pk, e)

                # decisions -> W/L/S
                try:
                    live_full = fetch_live(game_pk) or {}
                    dec = (live_full.get("liveData") or {}).get("decisions") or {}
                    w = dec.get("winner") or {}; l = dec.get("loser") or {}; s = dec.get("save") or {}
                    w_name = (w.get("fullName") or w.get("boxscoreName") or w.get("lastInitName") or "")
                    l_name = (l.get("fullName") or l.get("boxscoreName") or l.get("lastInitName") or "")
                    s_name = (s.get("fullName") or s.get("boxscoreName") or s.get("lastInitName") or "")
                    if status == "final":
                        try:
                            home_runs = int((item["linescore"] or {}).get("totals", {}).get("home", {}).get("R") or (item["teams"]["home"]["score"] or 0))
                            away_runs = int((item["linescore"] or {}).get("totals", {}).get("away", {}).get("R") or (item["teams"]["away"]["score"] or 0))
                        except Exception:
                            home_runs = item["teams"]["home"]["score"] or 0
                            away_runs = item["teams"]["away"]["score"] or 0
                        winner_side = "home" if home_runs >= away_runs else "away"
                        loser_side  = "away" if winner_side == "home" else "home"
                        if w_name: item["teams"][winner_side]["finalPitcher"] = f"W: {w_name}"
                        if l_name: item["teams"][loser_side]["finalPitcher"] = f"L: {l_name}"
                        if s_name: item["teams"][winner_side]["savePitcher"]  = f"SV: {s_name}"
                except Exception as e:
                    log.info("decisions enrich failed %s: %s", game_pk, e)

            else:
                # Scheduled game: try to prefill lineups if box has battingOrder posted
                try:
                    box = fetch_box(game_pk) or {}
                    item["lineups"]["away"] = _lineup_from_box(box, "away")
                    item["lineups"]["home"] = _lineup_from_box(box, "home")
                except Exception:
                    pass

            out.append(item)

        except Exception as e:
            try: log.exception("Error shaping game %s: %s", (g.get("gamePk")), e)
            except Exception: pass
            continue

    return jsonify({"date": date_used, "games": out}), 200

@app.route("/api/standings")
def api_standings():
    season = request.args.get("season", default=date.today().year, type=int)
    return jsonify(fetch_standings(season))

@app.route("/api/game/<int:game_pk>")
def api_game(game_pk: int):
    # Schedule (teams, venue, linescore)
    g = _get(f"{MLB_API}/schedule", {"gamePk": game_pk, "hydrate": "team,linescore"})
    games = (g.get("dates", [{}])[0].get("games", []) if g.get("dates") else [])
    if not games:
        return jsonify({"error": "not found"}), 404

    sched = games[0]
    status = _norm_status_from_sched(sched)
    date_iso = sched.get("gameDate")

    teams = (sched.get("teams") or {})
    home_sched = teams.get("home") or {}
    away_sched = teams.get("away") or {}
    home_t = home_sched.get("team", {}) or {}
    away_t = away_sched.get("team", {}) or {}

    game_payload = {
        "status": status,
        "chip": to_et_str(date_iso) if status == "scheduled" else "",
        "venue": (sched.get("venue") or {}).get("name", ""),
        "date": to_et_str(date_iso),
        "teams": {
            "home": {
                "id": home_t.get("id"),
                "name": home_t.get("name"),
                "abbr": _abbr(home_t.get("id"), home_t.get("abbreviation", "")),
                "record": fetch_team_record(home_sched),
                "score": None
            },
            "away": {
                "id": away_t.get("id"),
                "name": away_t.get("name"),
                "abbr": _abbr(away_t.get("id"), away_t.get("abbreviation", "")),
                "record": fetch_team_record(away_sched),
                "score": None
            },
        },
        "count": None,
        "lineups": {"away": [], "home": []},
        "scoring": []
    }

    # Linescore + scores
    ls_full = fetch_linescore(game_pk)
    game_payload["linescore"] = _shape_linescore(ls_full)
    try:
        game_payload["teams"]["away"]["score"] = int((ls_full.get("teams") or {}).get("away", {}).get("runs"))
        game_payload["teams"]["home"]["score"] = int((ls_full.get("teams") or {}).get("home", {}).get("runs"))
    except Exception:
        pass

    # Live, box, decisions, statcast summaries
    live = fetch_live(game_pk)
    try:
        pbp = fetch_pbp(game_pk)
    except Exception:
        pbp = None

    # last play/statcast in single-game view
    if live:
        try:
            res = (latest_play_from_feed(live).get("result") or {})
            game_payload["lastPlay"] = res.get("description") or ""
        except Exception:
            game_payload["lastPlay"] = ""
        try:
            game_payload["statcast"] = extract_statcast_line(live) or ""
        except Exception:
            game_payload["statcast"] = ""

        # count/outs from live
        try:
            ld = (live.get("liveData") or {})
            ls = (ld.get("linescore") or {})
            plays = (ld.get("plays") or {})
            cur = (plays.get("currentPlay") or {})
            balls = ls.get("balls"); strikes = ls.get("strikes"); outs = ls.get("outs")
            if balls is None or strikes is None:
                cnt = cur.get("count") or {}
                balls = balls if balls is not None else cnt.get("balls")
                strikes = strikes if strikes is not None else cnt.get("strikes")
            if outs is None:
                outs = (cur.get("about") or {}).get("outs")
            if (balls is not None) or (strikes is not None) or (outs is not None):
                game_payload["count"] = {"balls": balls, "strikes": strikes, "outs": outs}
        except Exception:
            pass
    else:
        game_payload["lastPlay"] = ""
        game_payload["statcast"] = ""

    box = fetch_box(game_pk)
    game_payload["batters"]  = {"away": _box_batting(box, "away"), "home": _box_batting(box, "home")}
    game_payload["pitchers"] = {"away": _box_pitching(box, "away"), "home": _box_pitching(box, "home")}

    # Lineups (scheduled/live/final if available)
    try:
        game_payload["lineups"]["away"] = _lineup_from_box(box, "away")
        game_payload["lineups"]["home"] = _lineup_from_box(box, "home")
    except Exception:
        pass

    # Scoring summary
    try:
        ld = (live or {}).get("liveData") or {}
        plays_live = ld.get("plays") or {}
        idxs = (plays_live.get("scoringPlays") or [])
        allp = (plays_live.get("allPlays") or [])
        play_by_id = {p.get("playId"): p for p in allp if p.get("playId")}
        scoring = []
        for ref in idxs:
            if isinstance(ref, int) and 0 <= ref < len(allp):
                p = allp[ref]
            elif isinstance(ref, str):
                p = play_by_id.get(ref, {})
            else:
                p = {}
            about = p.get("about") or {}
            resd = p.get("result") or {}
            scoring.append({
                "inning": about.get("inning"),
                "play": resd.get("description") or "",
                "away": about.get("awayScore"),
                "home": about.get("homeScore"),
            })
        game_payload["scoring"] = scoring
    except Exception:
        game_payload["scoring"] = []

    # Play-by-play
    plays = _shape_pbp(live, pbp)

    # Decisions text (for W/L/S strip)
    meta = {}
    try:
        _, dec_text, _ = _decisions(live or {})
        if dec_text:
            meta["decisionsText"] = dec_text
    except Exception:
        pass

    return jsonify({
        "game": game_payload,
        "plays": plays,
        "meta": meta
    }), 200

@app.route("/ping")
def ping():
    return "ok", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
