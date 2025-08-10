from flask import Flask, render_template, jsonify, request
import requests, time, logging, re
from datetime import datetime, timezone

# --------- Setup ---------
try:
    import pytz
    ET_TZ = pytz.timezone("America/New_York")
except Exception:
    ET_TZ = None

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("basenerd")

# --------- Constants / API ---------
STATS = "https://statsapi.mlb.com/api/v1"
LIVE  = "https://statsapi.mlb.com/api/v1.1"
SEASON = datetime.now(timezone.utc).year

LEAGUE_NAME = {103: "American League", 104: "National League"}
DIVISION_NAME = {
    200: "American League West",
    201: "American League East",
    202: "American League Central",
    203: "National League West",
    204: "National League East",
    205: "National League Central",
}
DIV_ORDER = {
    "American League": ["American League East","American League Central","American League West"],
    "National League": ["National League East","National League Central","National League West"],
}
TEAM_ABBR = {
    109:"ARI",144:"ATL",110:"BAL",111:"BOS",112:"CHC",145:"CHW",113:"CIN",114:"CLE",115:"COL",116:"DET",
    117:"HOU",118:"KCR",108:"LAA",119:"LAD",146:"MIA",158:"MIL",142:"MIN",121:"NYM",147:"NYY",133:"OAK",
    143:"PHI",134:"PIT",135:"SDP",136:"SEA",137:"SFG",138:"STL",139:"TBR",140:"TEX",141:"TOR",120:"WSH"
}

# simple caches
_PITCHER_CACHE = {}   # personId -> {ts, data}
_CACHE = {}           # key -> {ts, ttl, data}

# --------- HTTP / Time helpers ---------
def http_json(url, params=None, timeout=20):
    r = requests.get(url, params=params or {}, timeout=timeout,
                     headers={"User-Agent":"basenerd/1.0"})
    r.raise_for_status()
    return r.json()

def to_et(iso_z):
    if not iso_z:
        return ""
    try:
        dt = datetime.fromisoformat(iso_z.replace("Z","+00:00"))
        if ET_TZ:
            dt = dt.astimezone(ET_TZ)
        try:
            return dt.strftime("%-I:%M %p ET")
        except Exception:
            return dt.strftime("%I:%M %p ET").lstrip("0") + " ET"
    except Exception:
        return ""

def cache_get(key):
    x = _CACHE.get(key)
    if not x: return None
    if time.time() - x["ts"] > x["ttl"]:
        return None
    return x["data"]

def cache_set(key, data, ttl):
    _CACHE[key] = {"ts": time.time(), "ttl": ttl, "data": data}

# --------- Standings helpers ---------
def normalize_pct(pct_str):
    if pct_str in (None, ""): return 0.0
    try:
        s = str(pct_str).strip()
        return float("0"+s) if s.startswith(".") else float(s)
    except Exception:
        return 0.0

def get_last10(tr: dict) -> str:
    try:
        recs = (tr.get("records") or {}).get("splitRecords") or []
        for rec in recs:
            if rec.get("type") == "lastTen":
                return f"{rec.get('wins', 0)}-{rec.get('losses', 0)}"
    except Exception:
        pass
    return ""

def hardcoded_abbr(team: dict) -> str:
    tid = (team or {}).get("id")
    if tid in TEAM_ABBR: return TEAM_ABBR[tid]
    name = (team or {}).get("name") or ""
    name = name.replace(" ", "")
    return (name[:3] or "TBD").upper()

def fetch_standings_safe():
    try:
        data = http_json(f"{STATS}/standings", {
            "leagueId":"103,104","season":str(SEASON),"standingsTypes":"byDivision"})
        recs = data.get("records") or []
        if recs:
            return recs, None
        data2 = http_json(f"{STATS}/standings", {"leagueId":"103,104","season":str(SEASON)})
        return (data2.get("records") or []), None
    except Exception as e:
        log.exception("standings fetch failed")
        return [], f"standings_error: {e}"

def simplify_standings(records):
    leagues = {"National League": [], "American League": []}
    for block in (records or []):
        try:
            league_obj = block.get("league") or {}
            division_obj = block.get("division") or {}
            league_id = league_obj.get("id")
            division_id = division_obj.get("id")
            league_name = LEAGUE_NAME.get(league_id, league_obj.get("name") or "League")
            division_name = DIVISION_NAME.get(division_id, division_obj.get("name") or "Division")

            rows = []
            for tr in (block.get("teamRecords") or []):
                team = tr.get("team", {}) or {}
                rows.append({
                    "team_name": team.get("name", "Team"),
                    "team_abbr": hardcoded_abbr(team),
                    "team_id": team.get("id"),
                    "division": division_name,
                    "w": tr.get("wins", 0),
                    "l": tr.get("losses", 0),
                    "pct": normalize_pct(tr.get("winningPercentage")),
                    "gb": tr.get("gamesBack"),
                    "streak": (tr.get("streak") or {}).get("streakCode", "") or "",
                    "last10": get_last10(tr),
                    "runDiff": tr.get("runDifferential", 0),
                })

            target = league_name if league_name in leagues else ("American League" if league_id == 103 else "National League")
            leagues[target].append({"division": division_name, "rows": rows})
        except Exception as ie:
            log.warning("simplify_standings block skipped: %s", ie)
            continue

    for lg in leagues:
        div_order = DIV_ORDER.get(lg)
        if div_order:
            leagues[lg].sort(key=lambda d: div_order.index(d["division"]) if d["division"] in div_order else 99)
    return leagues

def wildcard_from_division(div_data):
    out = {"American League": {"leaders": [], "rows": []},
           "National League": {"leaders": [], "rows": []}}

    tag_map = {"East":"E", "Central":"C", "West":"W"}

    for league in ("American League","National League"):
        blocks = div_data.get(league, [])
        if not blocks:
            continue

        leaders, pool = [], []
        for block in blocks:
            div_name = block["division"]
            div_rows = list(block["rows"])
            div_rows.sort(key=lambda r: (r["pct"], r["runDiff"]), reverse=True)
            if div_rows:
                top = div_rows[0].copy()
                short = next((v for k,v in tag_map.items() if k in div_name), "")
                top["division_tag"] = short
                leaders.append(top)
                for r in div_rows[1:]:
                    pool.append(r.copy())

        leaders.sort(key=lambda r: (r["pct"], r["runDiff"]), reverse=True)
        pool.sort(key=lambda r: (r["pct"], r["runDiff"]), reverse=True)

        wc_rows = []
        wc3_ref = None
        for i, r in enumerate(pool):
            row = r.copy()
            row["wc_rank"] = i + 1
            row["badge"] = f"WC{i+1}" if i < 3 else ""
            wc_rows.append(row)
        if len(wc_rows) >= 3:
            wc3_ref = wc_rows[2]

        def wc_gb_str(row, ref):
            if not ref:
                return "—" if row.get("wc_rank", 99) <= 3 else ""
            if row.get("wc_rank", 99) <= 3:
                return "—"
            try:
                w, l = int(row["w"]), int(row["l"])
                rw, rl = int(ref["w"]), int(ref["l"])
                gb = ((rw - w) + (l - rl)) / 2.0
                s = f"{gb:.1f}"
                return s[:-2] if s.endswith(".0") else s
            except Exception:
                return ""

        for row in wc_rows:
            row["wc_gb"] = wc_gb_str(row, wc3_ref)

        out[league] = {"leaders": leaders, "rows": wc_rows}
    return out

# --------- Today’s Games helpers ----------
def fetch_schedule(date_str):
    js = http_json(f"{STATS}/schedule", {"sportId": 1, "date": date_str})
    dates = js.get("dates") or []
    return dates[0].get("games", []) if dates else []

def fetch_live(game_pk):
    return http_json(f"{LIVE}/game/{game_pk}/feed/live")

def fetch_pitcher_stats(pid, season):
    now = time.time()
    c = _PITCHER_CACHE.get(pid)
    if c and now - c["ts"] < 6*3600:
        return c["data"]
    data = http_json(f"{STATS}/people/{pid}", {"hydrate": f"stats(group=pitching,type=season,season={season})"}).get("people", [{}])[0]
    _PITCHER_CACHE[pid] = {"ts": now, "data": data}
    return data

def probable_line_from_person(person):
    name = person.get("fullName") or ""
    hand = (((person.get("pitchHand") or {}).get("code") or "") or "").upper()
    arm = "RHP" if hand == "R" else ("LHP" if hand == "L" else "").strip()
    wl, era = "", ""
    for s in person.get("stats", []):
        if s.get("group", {}).get("displayName") == "pitching":
            splits = s.get("splits", [])
            if splits:
                st = splits[0].get("stat", {})
                w = st.get("wins"); l = st.get("losses"); eraval = st.get("era")
                if w is not None and l is not None: wl = f"{w}-{l}"
                if eraval: era = f"{eraval} ERA"
            break
    parts = [name + (f" ({arm})" if arm else "")]
    if wl: parts.append(wl)
    if era: parts.append(era)
    return " • ".join([p for p in parts if p])

def get_probables(gameData, season):
    out = {"away": "", "home": ""}
    pp = gameData.get("probablePitchers") or {}
    teams = gameData.get("teams") or {}
    for side in ("away", "home"):
        pid = (pp.get(side) or {}).get("id") or (teams.get(side, {}).get("probablePitcher") or {}).get("id")
        if not pid:
            out[side] = ""
            continue
        person = fetch_pitcher_stats(pid, season)
        out[side] = probable_line_from_person(person)
    return out

def linescore_blob(ls, force_n=None):
    if not ls:
        if force_n:
            return {"n": force_n, "away": ["" for _ in range(force_n)], "home": ["" for _ in range(force_n)],
                    "totals": {"away":{"R":None,"H":None,"E":None},"home":{"R":None,"H":None,"E":None}}}
        return None
    innings = ls.get("innings", []) or []
    away_by, home_by = [], []
    for inn in innings:
        a = inn.get("away", {}); h = inn.get("home", {})
        away_by.append(a if isinstance(a, (int, str)) else a.get("runs", ""))
        home_by.append(h if isinstance(h, (int, str)) else h.get("runs", ""))
    n = len(away_by)
    if force_n and n < force_n:
        away_by += ["" ] * (force_n - n)
        home_by += ["" ] * (force_n - n)
        n = force_n
    totals = {
        "away": {"R": (ls.get("teams", {}).get("away", {}) or {}).get("runs"),
                 "H": (ls.get("teams", {}).get("away", {}) or {}).get("hits"),
                 "E": (ls.get("teams", {}).get("away", {}) or {}).get("errors")},
        "home": {"R": (ls.get("teams", {}).get("home", {}) or {}).get("runs"),
                 "H": (ls.get("teams", {}).get("home", {}) or {}).get("hits"),
                 "E": (ls.get("teams", {}).get("home", {}) or {}).get("errors")}
    }
    return {"n": max(n, force_n or 0), "away": away_by, "home": home_by, "totals": totals}

# (All other helpers unchanged...)

# --------- Routes ---------
@app.route("/")
def home_page():
    return render_template("index.html")

@app.route("/standings")
def standings_page():
    recs, err = fetch_standings_safe()
    data_division = simplify_standings(recs)
    data_wildcard = wildcard_from_division(data_division)
    # IMPORTANT: pass both names so old/new templates work
    return render_template(
        "standings.html",
        data_division=data_division,
        data_wildcard=data_wildcard,
        data_wc=data_wildcard,  # alias to prevent Jinja 'undefined' errors
        season=SEASON,
        error=err
    )

@app.route("/debug/standings.json")
def debug_standings():
    recs, err = fetch_standings_safe()
    return jsonify({"error": err, "records_count": len(recs), "records": recs})

@app.route("/todaysgames")
def todays_games_page():
    return render_template("todaysgames.html")

@app.route("/api/games")
def api_games():
    date_str = request.args.get("date") or datetime.utcnow().strftime("%Y-%m-%d")
    season = request.args.get("season") or SEASON

    cached = cache_get(date_str)
    if cached:
        return jsonify(cached)

    try:
        schedule = fetch_schedule(date_str)
    except Exception as e:
        msg = f"schedule_error: {e}"
        log.exception("Schedule fetch failed")
        data = {"date": date_str, "games": [], "error": msg}
        cache_set(date_str, data, 30)
        return jsonify(data), 200

    record_map = {}
    for g in schedule:
        pk = g.get("gamePk")
        if not pk: continue
        away_rec = g.get("teams", {}).get("away", {}).get("leagueRecord", {}) or {}
        home_rec = g.get("teams", {}).get("home", {}).get("leagueRecord", {}) or {}
        record_map[pk] = {
            "away": f"{away_rec.get('wins','')}-{away_rec.get('losses','')}",
            "home": f"{home_rec.get('wins','')}-{home_rec.get('losses','')}",
        }

    games = []
    any_live = False
    for g in schedule:
        pk = g.get("gamePk")
        if not pk: continue
        try:
            live = fetch_live(pk)
            shaped = shape_game(live, season, records=record_map.get(pk))
            any_live = any_live or (shaped["status"] == "in_progress")
            games.append(shaped)
        except Exception as ex:
            log.warning("live fetch failed for %s: %s", pk, ex)
            games.append({"gamePk": pk, "status": "error", "error": f"live_fetch_failed: {ex}"})

    payload = {"date": date_str, "games": games}
    cache_set(date_str, payload, 15 if any_live else 300)
    return jsonify(payload)

@app.route("/ping")
def ping():
    return "ok", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
