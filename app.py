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
    # include linescore+probablePitcher for card info
    return _get(f"{MLB_API}/schedule", {
        "sportId": SPORT_ID,
        "date": ymd,
        "hydrate": "team,linescore,probablePitcher"
    })

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


        # ---- helpers robust to odd shapes ----
        # --- replace your last10_from with this robust version ---
def last10_from(tr) -> str:
    """
    Try multiple known shapes for the 'last 10' record.
    Returns 'W-L' or ''.
    """
    if not isinstance(tr, dict):
        return ""

    # 1) teamRecords[].records list with type 'lastTen'
    recs = tr.get("records") or []
    if isinstance(recs, list):
        for r in recs:
            if isinstance(r, dict) and (r.get("type") or "").lower().replace(" ", "") in ("lastten", "last_10"):
                w, l = r.get("wins"), r.get("losses")
                if isinstance(w, int) and isinstance(l, int):
                    return f"{w}-{l}"

    # 2) nested splitRecords/overallRecords (sometimes under 'records' dict)
    if isinstance(tr.get("records"), dict):
        for key in ("splitRecords", "overallRecords", "records"):
            arr = tr["records"].get(key) if isinstance(tr["records"].get(key), list) else []
            for r in arr:
                if isinstance(r, dict) and (r.get("type") or "").lower().replace(" ", "") in ("lastten", "last_10"):
                    w, l = r.get("wins"), r.get("losses")
                    if isinstance(w, int) and isinstance(l, int):
                        return f"{w}-{l}"

    # 3) flat fields sometimes appear
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

        # ---- build payload safely ----
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

        def wc_gb(leader_w, leader_l, w, l):
    """Games behind helper = ((leader_w - w) + (l - leader_l)) / 2"""
    try:
        return round(((leader_w - w) + (l - leader_l)) / 2.0, 1)
    except Exception:
        return ""

def flatten_league_rows(league_name):
    """Flatten all division rows for a league into one list with division info."""
    flat = []
    for block in (data_division.get(league_name) or []):
        for row in (block.get("rows") or []):
            flat.append({
                **row,
                "division": block.get("division") or "",
            })
    return flat

data_wildcard = {}
for league in ("American League", "National League"):
    flat = flatten_league_rows(league)

    # Identify each division leader (first row in each division block)
    division_leaders = set()
    for block in (data_division.get(league) or []):
        first = (block.get("rows") or [])
        if first:
            division_leaders.add(first[0].get("team_id"))

    # Pool = all non-division-winners
    pool = [r for r in flat if r.get("team_id") not in division_leaders]

    # Sort pool by win% then runDiff (desc)
    pool.sort(key=lambda r: (r.get("pct") or 0.0, r.get("runDiff") or 0), reverse=True)

    # Top three = WC1..WC3
    top3 = pool[:3]

    # Compute WCGB relative to the 3rd WC team (the cut line)
    if len(top3) >= 3:
        cut = top3[2]
        cw, cl = cut.get("w") or 0, cut.get("l") or 0
    else:
        # If <3 teams, just use the best one as reference to avoid blanks
        ref = top3[0] if top3 else {"w": 0, "l": 0}
        cw, cl = ref.get("w") or 0, ref.get("l") or 0

    rows = []
    labels = ["WC1", "WC2", "WC3"]
    for i, r in enumerate(top3):
        rows.append({
            "seed": labels[i],
            "team_id": r.get("team_id"),
            "team_name": r.get("team_name"),
            "team_abbr": r.get("team_abbr"),
            "w": r.get("w"),
            "l": r.get("l"),
            "pct": r.get("pct"),
            "gb": r.get("gb"),
            "streak": r.get("streak"),
            "last10": r.get("last10"),
            "runDiff": r.get("runDiff"),
            "wcgb": wc_gb(cw, cl, r.get("w") or 0, r.get("l") or 0),
        })

    # 'leaders' can show the three WC seeds with badges; same schema as rows works fine
    data_wildcard[league] = {
        "leaders": rows[:],  # shallow copy
        "rows": rows[:],     # if your template uses both, keep them in sync
    }

        # Tiny debug signal rendered in your page footer so we know what's happening
        debug = {
            "season": season,
            "records_count": len(records),
            "leagues_built": {k: len(v) for k, v in data_division.items()},
            "first_div_rows": (len(data_division.get("American League", [])[0]["rows"])
                               if data_division.get("American League") else 0),
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
    # Date in ET by default to match UI
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

                # Badge text
                try:
                    if status == "scheduled":
                        chip = to_et_str(g.get("gameDate"))  # '1:05 PM ET'
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

                # Stable fields used by todaysgames.html
                item.setdefault("lastPlay", "")
                item.setdefault("statcast", "")
                item.setdefault("bases", {"first": False, "second": False, "third": False})
                item.setdefault("dueUpSide", None)
                item.setdefault("dueUp", None)
                item.setdefault("linescore", None)
                item.setdefault("batters", {"away": [], "home": []})
                item.setdefault("pitchers", {"away": [], "home": []})
                item.setdefault("lineups", {"away": [], "home": []})  # scheduled-only; UI will show "not yet available"

                # inning context from schedule (used for live badge + inBreak)
                ls_sched = (g.get("linescore") or {})
                item["inning"] = ls_sched.get("currentInning")
                item["isTop"] = (ls_sched.get("isTopInning") is True)
                item["inBreak"] = ((ls_sched.get("inningState") or "").lower() in ("end", "middle"))

                if status != "scheduled":
                    # Live/final enrichment
                    try:
                        live = fetch_live(game_pk) or {}
                    except Exception as e:
                        log.warning("live fetch failed for %s: %s", game_pk, e)
                        live = {}

                    # Last play + Statcast
                    try:
                        play = latest_play_from_feed(live)
                        item["lastPlay"] = ((play.get("result") or {}).get("description")) or ""
                    except Exception:
                        item["lastPlay"] = ""
                    try:
                        item["statcast"] = extract_statcast_line(live) or ""
                    except Exception:
                        item["statcast"] = ""

                    # Bases + current batter/pitcher + due-up + break pitcher
                    try:
                        ld = (live.get("liveData") or {})
                        ls_live = ld.get("linescore") or {}
                        offense = ls_live.get("offense") or {}
                        defense = ls_live.get("defense") or {}

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

                        # For UI: set current batter/pitcher onto each team block
                        if item["dueUpSide"] == "home":
                            item["teams"]["home"]["currentBatter"] = batter
                            item["teams"]["away"]["currentPitcher"] = pitcher
                        elif item["dueUpSide"] == "away":
                            item["teams"]["away"]["currentBatter"] = batter
                            item["teams"]["home"]["currentPitcher"] = pitcher

                        on_deck = _nm(offense.get("onDeck"))
                        in_hole = _nm(offense.get("inHole"))
                        item["dueUp"] = ", ".join([n for n in (batter, on_deck, in_hole) if n]) or None

                        # When in break, surface the last pitcher as 'breakPitcher' so your UI can show it
                        if item["inBreak"] and pitcher:
                            # show break pitcher for both teams (UI picks whichever makes sense)
                            item["teams"]["home"]["breakPitcher"] = pitcher
                            item["teams"]["away"]["breakPitcher"] = pitcher

                    except Exception as e:
                        log.warning("context parse failed for %s: %s", game_pk, e)

                    # Linescore & box tables (shaped to your keys)
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

                    # Scoring summary
                    try:
                        item["scoring"] = _shape_scoring_from_live(live)
                    except Exception:
                        item["scoring"] = []

                    # Decisions text (final)
                    if status == "final":
                        try:
                            _, dec_text, save_text = _decisions(live)
                            item["teams"]["home"]["finalPitcher"] = dec_text
                            item["teams"]["away"]["finalPitcher"] = dec_text
                            if save_text:
                                item["teams"]["home"]["savePitcher"] = save_text
                                item["teams"]["away"]["savePitcher"] = save_text
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
