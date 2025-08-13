from flask import Flask, render_template, jsonify, request 
import requests, logging
from datetime import datetime, timezone, date
from typing import List, Optional, Tuple

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
        r = requests.get(url, params=params, timeout=timeout)
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

    base = f"{MLB_API}/schedule"
    attempts = [
        # Standard
        {"sportId": SPORT_ID, "date": ymd, "hydrate": hydrate},
        # Some envs only return when using start/end
        {"sportId": SPORT_ID, "startDate": ymd, "endDate": ymd, "hydrate": hydrate},
        # Some envs filter to Regular Season implicitly; be explicit
        {"sportId": SPORT_ID, "date": ymd, "gameTypes": "R", "hydrate": hydrate},
        {"sportId": SPORT_ID, "startDate": ymd, "endDate": ymd, "gameTypes": "R", "hydrate": hydrate},
    ]

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

import requests
from datetime import date

def fetch_standings(season: int):
    """
    Robust MLB standings fetcher.
    Tries several known-good StatsAPI parameter combos (with/without `date` and hydrates).
    Returns a dict with 'records' (possibly empty) and logs what happened.
    """
    base = "https://statsapi.mlb.com/api/v1/standings"
    today = date.today().isoformat()

    # Some hosts require `date` for current-season snapshots.
    # Also try both standingsType(s) spellings and a hydrate to ensure league/division present.
    common = {
        "season": str(season),
        "sportId": "1",             # MLB
        "leagueId": "103,104",      # 103 = AL, 104 = NL
        "hydrate": "team,league,division,record",
    }

    attempts = [
        {**common, "standingsTypes": "byDivision", "date": today},
        {**common, "standingsType":  "byDivision", "date": today},
        {**common, "standingsTypes": "regularSeason", "date": today},
        {**common, "standingsType":  "regularSeason", "date": today},

        # no date fallback
        {**common, "standingsTypes": "byDivision"},
        {**common, "standingsType":  "byDivision"},
        {**common, "standingsTypes": "regularSeason"},
        {**common, "standingsType":  "regularSeason"},

        # wildcard (not used for division tables, but sometimes proves connectivity)
        {**common, "standingsTypes": "wildCard", "date": today},
        {**common, "standingsType":  "wildCard", "date": today},
    ]

    headers = {
        "Accept": "application/json",
        "User-Agent": "ParlayPressStandings/1.0"
    }

    for i, params in enumerate(attempts, 1):
        try:
            r = requests.get(base, params=params, headers=headers, timeout=12)
            r.raise_for_status()
            js = r.json()
            recs = js.get("records") if isinstance(js, dict) else None
            if isinstance(recs, list) and recs:
                # success
                try:
                    app.logger.info("MLB standings success on attempt %s: records=%s", i, len(recs))
                except Exception:
                    pass
                return js
            else:
                try:
                    app.logger.info("MLB standings attempt %s returned empty records", i)
                except Exception:
                    pass
        except Exception as e:
            try:
                app.logger.info("MLB standings attempt %s failed: %s", i, e)
            except Exception:
                pass

    # Final fallback that won't crash caller
    return {"records": []}

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

# ---------- shaping helpers (MATCH what todaysgames.html expects) ----------
def _norm_status_from_sched(g: dict) -> str:
    st_det = ((g.get("status") or {}).get("detailedState") or "").lower()
    st_abs = ((g.get("status") or {}).get("abstractGameState") or "").lower()
    if "final" in st_det or "completed" in st_det or "game over" in st_det or st_abs == "final":
        return "final"
    live_keys = ("in progress", "warmup", "manager challenge", "review", "resumed", "suspended")
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
    """
    Shape to what todaysgames.html expects:
    {
      n: <int>,
      away: [per-inning runs],
      home: [per-inning runs],
      totals: { away: {R,H,E}, home: {R,H,E} }
    }
    """
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
    """
    Lowercase keys + POS, to match todaysgames.html:
    [{pos,name,ab,r,h,rbi,bb,k,indent?}]
    """
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
    """
    Normalizes play-by-play into the structure game.html expects, including xBA.
    """
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

        # Build pitch list (used by strike zone + mini table)
        pitch_list = []
        for evn in (p.get("playEvents") or []):
            pd = evn.get("pitchData") or {}
            det = evn.get("details") or {}
            coords = (pd.get("coordinates") or {})

            # Skip non-pitches unless it marked ball-in-play
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
            "xba": xba,   # <- lowercase
            "xBA": xba,   # <- alias in case the UI uses camel case
            "pitches": pitch_list,
        })

    return out


def _box_pitching(box: dict, side: str) -> List[dict]:
    """
    Lowercase keys your page expects:
    [{pos,name,ip,h,r,er,bb,k,hr,p}]
    """
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
    """
    Returns (ids, decisions_text, save_text)
    decisions_text like 'W: Name • L: Name • SV: Name'
    save_text like 'SV: Name' or ''
    """
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

def _shape_scoring_from_live(live: dict) -> List[dict]:
    """
    Return list of {inning, play, away, home} for scoring summary dropdown.
    """
    out: List[dict] = []
    plays = ((live.get("liveData") or {}).get("plays") or {})
    scoring = plays.get("scoringPlays") or []
    all_plays = plays.get("allPlays") or []
    idx = {p.get("playId"): p for p in all_plays if p.get("playId")}
    for sp in scoring:
        p = sp if isinstance(sp, dict) else idx.get(sp)
        if not p:
            continue
        res = (p.get("result") or {})
        about = (p.get("about") or {})
        inn = about.get("inning")
        away = about.get("awayScore")
        home = about.get("homeScore")
        desc = res.get("description") or ""
        out.append({"inning": inn, "play": desc, "away": away, "home": home})
    return out

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
            # 1) teamRecords[].records[] with type 'lastTen'
            recs = tr.get("records") or []
            if isinstance(recs, list):
                for r in recs:
                    if isinstance(r, dict) and (r.get("type") or "").lower().replace(" ", "") in ("lastten", "last_10"):
                        w, l = r.get("wins"), r.get("losses")
                        if isinstance(w, int) and isinstance(l, int):
                            return f"{w}-{l}"
            # 2) nested shapes
            if isinstance(tr.get("records"), dict):
                for key in ("splitRecords", "overallRecords", "records"):
                    arr = tr["records"].get(key) if isinstance(tr["records"].get(key), list) else []
                    for r in arr:
                        if isinstance(r, dict) and (r.get("type") or "").lower().replace(" ", "") in ("lastten", "last_10"):
                            w, l = r.get("wins"), r.get("losses")
                            if isinstance(w, int) and isinstance(l, int):
                                return f"{w}-{l}"
            # 3) flat fields
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
                if "_abbr" in globals():
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
            # ---------- build wild card tables (full) ----------
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

            # 3) Build rows (top three get WC1–WC3 badge; everyone else blank)
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
                "leaders": leaders,  # division leaders with E/C/W tags
                "rows": rows,        # all WC contenders, top 3 badged WC1–WC3
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



# app.py
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
    # Date in ET (template expects ET)
    d = request.args.get("date")
    if not d:
        try:
            dt = datetime.utcnow().replace(tzinfo=timezone.utc)
            if ET_TZ:
                dt = dt.astimezone(ET_TZ)  # type: ignore
            d = dt.strftime("%Y-%m-%d")
        except Exception:
            d = datetime.utcnow().strftime("%Y-%m-%d")

    out = []
    try:
        js = fetch_schedule(d) or {}
        dates = js.get("dates") or []
        games = (dates[0].get("games") or []) if dates else []
    # Fallback: if no games, try explicit start/end and a 1-day ET fudge
if not games:
    try:
        # Attempt #1: explicit start/end (some proxies require this)
        js2 = fetch_schedule(d) or {}
        dates2 = js2.get("dates") or []
        games2 = (dates2[0].get("games") or []) if dates2 else []
        if games2:
            games = games2
        else:
            # Attempt #2: +/- 1 day in ET in case of timezone boundaries
            et = ET_TZ.localize(datetime.strptime(d, "%Y-%m-%d")).date()
            for delta in (-1, 1):
                dd = (et + timedelta(days=delta)).isoformat()
                js3 = fetch_schedule(dd) or {}
                dates3 = js3.get("dates") or []
                games3 = (dates3[0].get("games") or []) if dates3 else []
                if games3:
                    d = dd  # use the date that actually has games
                    games = games3
                    break
    except Exception:
        pass

    except Exception as e:
        log.exception("/api/games schedule fetch failed: %s", e)
        return jsonify({"date": d, "games": []}), 200

    def _abbr(team_id, fallback=""):
        try:
            return TEAM_ABBR.get(int(team_id)) or (fallback or "")
        except Exception:
            return fallback or ""

    def _fmt_probable(prob: dict) -> str:
        """Name + season line if available"""
        if not isinstance(prob, dict): return ""
        nm = prob.get("fullName") or prob.get("name") or ""
        w = l = era = None
        for stat in (prob.get("stats") or []):
            for split in (stat.get("splits") or []):
                s = split.get("stat") or {}
                w = w or s.get("wins")
                l = l or s.get("losses")
                era = era or s.get("era")
        if w is not None and l is not None and era:
            return f"{nm} ({w}-{l}, {era} ERA)"
        return nm

    for g in games:
        try:
            game_pk = g.get("gamePk")
            status = _norm_status_from_sched(g)
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
                    obj["probable"] = _fmt_probable(prob)
                return obj

            item = {
                "gamePk": game_pk,
                "venue": ((g.get("venue") or {}).get("name")) or "",
                "status": status,
                "chip": chip,
                "teams": {
                    "away": _team("away"),
                    "home": _team("home"),
                },
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
                "lineups": {"away": [], "home": []},  # placeholder; fine if empty
            }

            if status != "scheduled":
                # ---- Live feed
                try:
                    live = fetch_live(game_pk) or {}
                except Exception as e:
                    log.warning("live fetch failed for %s: %s", game_pk, e)
                    live = {}

                # Last play + Statcast
                try:
                    cur = latest_play_from_feed(live)
                    item["lastPlay"] = ((cur.get("result") or {}).get("description")) or ""
                except Exception:
                    item["lastPlay"] = ""
                try:
                    item["statcast"] = extract_statcast_line(live) or ""
                except Exception:
                    item["statcast"] = ""

                ld = (live.get("liveData") or {})
                ls_live = (ld.get("linescore") or {})

                # Bases / offense / defense
                offense = ls_live.get("offense") or {}
                defense = ls_live.get("defense") or {}

                item["bases"] = {
                    "first":  bool(offense.get("first")  or offense.get("onFirst")),
                    "second": bool(offense.get("second") or offense.get("onSecond")),
                    "third":  bool(offense.get("third")  or offense.get("onThird")),
                }

                # Due-up and which side is batting
                batter_obj = offense.get("batter") or {}
                pitcher_obj = defense.get("pitcher") or {}
                batter_name = batter_obj.get("fullName") or batter_obj.get("name")
                pitcher_name = pitcher_obj.get("fullName") or pitcher_obj.get("name")

                home_id = item["teams"]["home"]["id"]
                away_id = item["teams"]["away"]["id"]
                off_team_id = ((offense.get("team") or {}).get("id"))
                item["dueUpSide"] = "home" if off_team_id == home_id else ("away" if off_team_id == away_id else None)

                on_deck = (offense.get("onDeck") or {}).get("fullName") or (offense.get("onDeck") or {}).get("name")
                in_hole = (offense.get("inHole") or {}).get("fullName") or (offense.get("inHole") or {}).get("name")
                item["dueUp"] = ", ".join([n for n in (batter_name, on_deck, in_hole) if n]) or None

                # Live count (balls, strikes, outs) with fallbacks
                balls = ls_live.get("balls")
                strikes = ls_live.get("strikes")
                outs = ls_live.get("outs")
                try:
                    cur_play = ((ld.get("plays") or {}).get("currentPlay") or {})
                    if balls is None or strikes is None:
                        cnt = cur_play.get("count") or {}
                        balls = balls if balls is not None else cnt.get("balls")
                        strikes = strikes if strikes is not None else cnt.get("strikes")
                    if outs is None:
                        outs = (cur_play.get("about") or {}).get("outs")
                except Exception:
                    pass
                if balls is not None or strikes is not None or outs is not None:
                    item["count"] = {"balls": balls, "strikes": strikes, "outs": outs}

                # Linescore (inning-by-inning table)
                try:
                    item["linescore"] = _shape_linescore(fetch_linescore(game_pk))
                except Exception as e:
                    log.warning("linescore failed for %s: %s", game_pk, e)
                    item["linescore"] = None

                # Boxscore for inline current batter/pitcher lines + dropdowns
                try:
                    box = fetch_box(game_pk) or {}
                    item["batters"]  = {"away": _box_batting(box, "away"), "home": _box_batting(box, "home")}
                    item["pitchers"] = {"away": _box_pitching(box, "away"), "home": _box_pitching(box, "home")}

                    # Inline “current” strings
                    def _pl(team_side: str, pid: int | None):
                        if not pid:
                            return None, None
                        team = (box.get("teams") or {}).get(team_side) or {}
                        players = team.get("players") or {}
                        pobj = players.get(f"ID{pid}") or {}
                        person = pobj.get("person") or {}
                        name = person.get("lastInitName") or person.get("boxscoreName") or person.get("fullName") or ""
                        bat = (pobj.get("stats") or {}).get("batting") or {}
                        pit = (pobj.get("stats") or {}).get("pitching") or {}
                        # batter line
                        bat_parts = []
                        if bat:
                            h, ab = bat.get("hits"), bat.get("atBats")
                            if h is not None and ab is not None: bat_parts.append(f"{h}-{ab}")
                            hr = bat.get("homeRuns"); rbi = bat.get("rbi")
                            if hr: bat_parts.append(f"{hr} HR")
                            if rbi: bat_parts.append(f"{rbi} RBI")
                        batter_line = f"{name} — {', '.join(bat_parts)}" if bat_parts else (name or None)
                        # pitcher line
                        pit_parts = []
                        if pit:
                            pc = pit.get("pitchesThrown") or pit.get("numberOfPitches") or pit.get("pitchCount")
                            ip = pit.get("inningsPitched")
                            so = pit.get("strikeOuts") or pit.get("strikeouts")
                            if pc is not None: pit_parts.append(f"{pc} P")
                            if ip: pit_parts.append(f"{ip} IP")
                            if so: pit_parts.append(f"{so} K")
                        pitcher_line = f"{name} — {' • '.join(pit_parts)}" if pit_parts else (name or None)
                        return batter_line, pitcher_line

                    batter_id = batter_obj.get("id")
                    pitcher_id = pitcher_obj.get("id")

                    # attach to the correct team blocks
                    if item["dueUpSide"] == "home":
                        b_line, _ = _pl("home", batter_id)
                        _, p_line = _pl("away", pitcher_id)
                        if b_line: item["teams"]["home"]["currentBatter"] = b_line
                        if p_line: item["teams"]["away"]["currentPitcher"] = p_line
                    elif item["dueUpSide"] == "away":
                        b_line, _ = _pl("away", batter_id)
                        _, p_line = _pl("home", pitcher_id)
                        if b_line: item["teams"]["away"]["currentBatter"] = b_line
                        if p_line: item["teams"]["home"]["currentPitcher"] = p_line

                except Exception as e:
                    log.warning("box failed for %s: %s", game_pk, e)
                    item["batters"] = item["pitchers"] = None

                # Scoring summary
                try:
                    item["scoring"] = _shape_scoring_from_live(live)
                except Exception:
                    item["scoring"] = []

            out.append(item)

        except Exception as e:
            log.exception("Error shaping game %s: %s", (g.get("gamePk")), e)
            continue

    return jsonify({"date": d, "games": out}), 200

@app.route("/api/standings")
def api_standings():
    season = request.args.get("season", default=date.today().year, type=int)
    return jsonify(fetch_standings(season))

# app.py
# app.py
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
    home_t = (teams.get("home") or {}).get("team", {}) or {}
    away_t = (teams.get("away") or {}).get("team", {}) or {}

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
                "score": None
            },
            "away": {
                "id": away_t.get("id"),
                "name": away_t.get("name"),
                "abbr": _abbr(away_t.get("id"), away_t.get("abbreviation", "")),
                "score": None
            },
        },
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

    game_payload["lastPlay"] = ((latest_play_from_feed(live).get("result") or {}).get("description")) if live else ""
    game_payload["statcast"] = extract_statcast_line(live) if live else ""

    box = fetch_box(game_pk)
    game_payload["batters"]  = {"away": _box_batting(box, "away"), "home": _box_batting(box, "home")}
    game_payload["pitchers"] = {"away": _box_pitching(box, "away"), "home": _box_pitching(box, "home")}

    # Decisions for W/L/S highlighting
    ids, dec_text, _ = _decisions(live) if live else ({}, "", "")

    # Play-by-play (uses dedicated endpoint, falls back to live)
    plays = _shape_pbp(live, pbp)

    return jsonify({
        "game": game_payload,
        "meta": {"decisions": ids, "decisionsText": dec_text},
        "plays": plays
    }), 200
@app.route("/ping")
def ping():
    return "ok", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
