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
    """
    Division view dict:
      {"League":[{"division": "...", "rows":[...]}], ...}
    rows: team_name, team_abbr, team_id, division, w, l, pct, gb, streak, last10, runDiff
    """
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

    # sort divisions in logical order
    for lg in leagues:
        div_order = DIV_ORDER.get(lg)
        if div_order:
            leagues[lg].sort(key=lambda d: div_order.index(d["division"]) if d["division"] in div_order else 99)
    return leagues

def wildcard_from_division(div_data):
    """
    Wild Card view per league:
    {
      "American League": {
        "leaders":[{... with division_tag E/C/W} x3],
        "rows":[{... , wc_rank, wc_gb, badge 'WC1/2/3' or ''}]
      },
      "National League": { ... }
    }
    """
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
        away_by += [""] * (force_n - n)
        home_by += [""] * (force_n - n)
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

# ---- latest play + Statcast (sync to true last play) ----
def latest_play_from_feed(live):
    ld = live.get("liveData", {}) or {}
    plays = ld.get("plays", {}) or {}
    allp = plays.get("allPlays") or []
    for p in reversed(allp):
        desc = (p.get("result") or {}).get("description")
        if desc:
            return p
    cur = plays.get("currentPlay") or {}
    if (cur.get("result") or {}).get("description"):
        return cur
    scoring = plays.get("scoringPlays") or []
    if scoring and allp:
        ids = {p.get("about", {}).get("atBatIndex"): p for p in allp}
        sp = ids.get(scoring[-1])
        if sp:
            return sp
    return {}

def extract_last_play(live):
    p = latest_play_from_feed(live)
    return (p.get("result") or {}).get("description") or ""

def _find_latest_hitdata_from_play(play):
    if not play:
        return None
    events = play.get("playEvents") or []
    for ev in reversed(events):
        hd = ev.get("hitData")
        if hd:
            return hd
    return play.get("hitData") or None

def extract_statcast_line(live):
    play = latest_play_from_feed(live)
    hd = _find_latest_hitdata_from_play(play)
    if not hd:
        return ""
    ev = hd.get("launchSpeed"); la = hd.get("launchAngle"); dist = hd.get("totalDistance")
    xba = (hd.get("estimatedBA") or hd.get("estimatedBa") or hd.get("estimatedBattingAverage")
           or hd.get("xba") or hd.get("expectedBattingAverage"))
    parts = []
    if ev is not None:
        try: parts.append(f"EV: {float(ev):.1f} MPH")
        except: pass
    if la is not None:
        try: parts.append(f"LA: {float(la):.1f}°")
        except: pass
    if dist is not None:
        try:
            dval = float(dist)
            parts.append(f"Dist: {int(dval) if abs(dval-int(dval))<1e-9 else round(dval):d}ft")
        except: pass
    if xba is not None:
        try:
            x = float(xba)
            if x > 1.0: x = x / 100.0
            parts.append(f"xBA: {x:.3f}".replace("0.", "."))
        except:
            s = str(xba).strip()
            if s: parts.append(f"xBA: {s}")
    return " • ".join(parts)

# ---- Plate appearance tokens for current batter line ----
PAREN_CODE_RE = re.compile(r"\(([1-9](?:-[1-9]){0,3})\)")
def extract_scoring_summary(live):
    """Return scoring plays with inning label, description, and score after play."""
    try:
        ld = live.get("liveData") or {}
        plays = ld.get("plays") or {}
        allp = plays.get("allPlays") or []
        scoring_idxs = plays.get("scoringPlays") or []
        # Map atBatIndex to play
        by_idx = {}
        for p in allp:
            abidx = (p.get("about") or {}).get("atBatIndex")
            if abidx is not None:
                by_idx[abidx] = p
        out = []
        for idx in scoring_idxs:
            p = by_idx.get(idx) or {}
            about = (p.get("about") or {})
            res = (p.get("result") or {})
            inn = about.get("inning")
            half = about.get("halfInning")
            inning_label = f"{('Top' if str(half).lower().startswith('t') else 'Bot') if half else ''} {inn}".strip()
            desc = res.get("description") or ""
            away = res.get("awayScore")
            home = res.get("homeScore")
            # Fallback: if scores missing, try to derive from linescore totals up to this play (skipped for speed)
            out.append({
                "inning": inning_label or inn,
                "play": desc,
                "away": away,
                "home": home
            })
        return out
    except Exception:
        return []

def out_air_prefix(event_type):
    et = (event_type or "").lower()
    if "pop" in et: return "P"
    if "line" in et: return "L"
    return "F"
ABBREV_TO_NUM = {"P":1,"C":2,"1B":3,"2B":4,"3B":5,"SS":6,"LF":7,"CF":8,"RF":9}
WORD_TO_NUM = {"pitcher":1,"catcher":2,"first baseman":3,"first base":3,"second baseman":4,"second base":4,
               "third baseman":5,"third base":5,"shortstop":6,"left fielder":7,"left field":7,
               "center fielder":8,"center field":8,"right fielder":9,"right field":9}

def fielder_chain(play):
    credits = play.get("credits") or []
    assists, putouts = [], []
    for c in credits:
        credit = (c.get("credit") or "").lower()
        pos = None
        posobj = c.get("position") or {}
        code = posobj.get("code"); abbr = (posobj.get("abbrev") or "").upper()
        if code and str(code).isdigit():
            pos = int(code)
        elif abbr in ABBREV_TO_NUM:
            pos = ABBREV_TO_NUM[abbr]
        if pos is None: continue
        if "assist" in credit: assists.append(pos)
        elif "putout" in credit: putouts.append(pos)
    if assists or putouts:
        chain = assists + (putouts[-1:] if putouts else [])
        if chain:
            return "-".join(str(n) for n in chain)
    desc = ((play.get("result") or {}).get("description") or "")
    m = PAREN_CODE_RE.search(desc)
    if m: return m.group(1)
    m2 = re.search(r"\bto ([a-z ]+?)(?:$|,|\.|\s)", desc.lower())
    if m2 and m2.group(1).strip() in WORD_TO_NUM:
        return str(WORD_TO_NUM[m2.group(1).strip()])
    return ""

def play_to_token(play):
    res = (play.get("result") or {})
    et = (res.get("eventType") or "").lower()
    desc = (res.get("description") or "")
    if et == "single": return "1B"
    if et == "double": return "2B"
    if et == "triple": return "3B"
    if et == "home_run": return "HR"
    if "walk" in et: return "BB"
    if et == "hit_by_pitch": return "HBP"
    if et in ("intent_walk", "intentional_walk"): return "BB"
    if et.startswith("strikeout"):
        return "K" if et != "strikeout_double_play" else "KDP"
    if et in ("sac_fly", "sac_fly_double_play"):
        pos = fielder_chain(play); return f"SF{pos}" if pos else "SF"
    if et in ("sac_bunt", "sac_bunt_double_play"):
        pos = fielder_chain(play); return f"SH{pos}" if pos else "SH"
    if "error" in et:
        chain = fielder_chain(play); return f"E{chain}" if chain else "E"
    if "fielders_choice" in et:
        chain = fielder_chain(play); return f"FC{chain}" if chain else "FC"
    if et in ("groundout","force_out","double_play","triple_play","grounded_into_double_play"):
        chain = fielder_chain(play); return chain or "GO"
    if any(k in et for k in ("flyout","lineout","pop_out","foul_popout")):
        prefix = out_air_prefix(et)
        chain = fielder_chain(play)
        if chain: return f"{prefix}{chain}"
        m2 = re.search(r"\bto ([a-z ]+?)(?:$|,|\.|\s)", desc.lower())
        if m2 and m2.group(1).strip() in WORD_TO_NUM:
            return f"{prefix}{WORD_TO_NUM[m2.group(1).strip()]}"
        return prefix
    m = PAREN_CODE_RE.search(desc)
    if m: return m.group(1)
    evshort = (res.get("event") or "").upper().replace(" ", "_")
    return evshort[:6] if evshort else ""

def batter_outcomes(live, batter_id):
    if not batter_id: return ""
    allp = (live.get("liveData", {}) or {}).get("plays", {}).get("allPlays", []) or []
    tokens = []
    for p in allp:
        m = p.get("matchup") or {}
        b = (m.get("batter") or {}).get("id")
        if b != batter_id: continue
        et = ((p.get("result") or {}).get("eventType") or "").lower()
        if not et: continue
        token = play_to_token(p)
        if token: tokens.append(token)
    return ", ".join(tokens)

# ---- Box score extraction ----
def _name(person):
    return (person or {}).get("fullName") or ""

def extract_batting_box_grouped(box, side):
    """
    Return batting rows grouped by official battingOrder so subs appear under the starter.
    Each row: pos, name, ab, r, h, rbi, bb, k, orderCode, indent
    """
    t = (box.get("teams", {}) or {}).get(side, {}) or {}
    players = t.get("players") or {}
    entries = []

    for key, pl in players.items():
        bo = pl.get("battingOrder")
        if not bo:
            continue
        try:
            order_code = int(bo)  # e.g., 101, 202, 903 (order*100 + slot)
        except Exception:
            continue
        pos = ((pl.get("position") or {}).get("abbreviation") or "").upper()
        bat = (pl.get("stats") or {}).get("batting") or {}
        entries.append({
            "orderCode": order_code,
            "pos": pos,
            "name": _name(pl.get("person")),
            "ab": bat.get("atBats", 0),
            "r": bat.get("runs", 0),
            "h": bat.get("hits", 0),
            "rbi": bat.get("rbi") or bat.get("runsBattedIn", 0),
            "bb": bat.get("baseOnBalls", 0),
            "k": bat.get("strikeOuts", 0),
            "_key": key,
        })

    entries.sort(key=lambda x: x["orderCode"])

    rows = []
    last_code = None
    seen_keys = set()
    for e in entries:
        indent = (e["orderCode"] == last_code)
        last_code = e["orderCode"]
        out = {k: e[k] for k in ("pos","name","ab","r","h","rbi","bb","k")}
        out["orderCode"] = e["orderCode"]
        out["indent"] = indent
        rows.append(out)
        seen_keys.add(e["_key"])

    # players with batting stats but no battingOrder (rare)
    for key, pl in players.items():
        if key in seen_keys:
            continue
        bat = (pl.get("stats") or {}).get("batting") or {}
        if any(bat.get(k) for k in ("atBats","hits","runs","rbi","runsBattedIn","baseOnBalls","strikeOuts")):
            rows.append({
                "pos": ((pl.get("position") or {}).get("abbreviation") or "").upper(),
                "name": _name(pl.get("person")),
                "ab": bat.get("atBats", 0),
                "r": bat.get("runs", 0),
                "h": bat.get("hits", 0),
                "rbi": bat.get("rbi") or bat.get("runsBattedIn", 0),
                "bb": bat.get("baseOnBalls", 0),
                "k": bat.get("strikeOuts", 0),
                "orderCode": 9999,
                "indent": True
            })
    return rows

def extract_pitching_box(box, side):
    t = (box.get("teams", {}) or {}).get(side, {}) or {}
    pitchers = t.get("pitchers") or []
    players = t.get("players") or {}
    rows = []
    for i, pid in enumerate(pitchers):
        pl = players.get(f"ID{pid}") or {}
        pos = ((pl.get("position") or {}).get("abbreviation") or "P").upper()
        person = pl.get("person") or {}
        st = (pl.get("stats") or {}).get("pitching") or {}
        rows.append({
            "pos": pos,
            "name": person.get("fullName") or "",
            "ip": st.get("inningsPitched", 0),
            "h": st.get("hits", 0),
            "r": st.get("runs", 0),
            "er": st.get("earnedRuns", 0),
            "bb": st.get("baseOnBalls", 0),
            "k": st.get("strikeOuts", 0),
            "hr": st.get("homeRuns", 0),
            "p": st.get("numberOfPitches") or st.get("pitchesThrown") or 0,
            "indent": (i > 0)
        })
    return rows

# ---- Build game state ----
def team_last_pitcher_line(box, side):
    t = (box.get("teams", {}) or {}).get(side, {}) or {}
    pitchers = t.get("pitchers") or []
    players = t.get("players") or {}
    if not pitchers: return ""
    pid = pitchers[-1]
    pobj = players.get(f"ID{pid}") or {}
    st = (pobj.get("stats") or {}).get("pitching") or {}
    name = (pobj.get("person") or {}).get("fullName") or ""
    if not name: return ""
    return f"P: {name} • IP {st.get('inningsPitched','-')} • P {st.get('numberOfPitches') or st.get('pitchesThrown') or '-'} • ER {st.get('earnedRuns',0)} • K {st.get('strikeOuts',0)} • BB {st.get('baseOnBalls',0)}"

def fmt_avg(v):
    if v in (None, "", "-"): return ""
    try:
        x = float(v); return f"{x:.3f}".replace("0.", ".")
    except Exception:
        s = str(v).strip()
        return s.replace("0.", ".") if s.startswith("0.") else s

def extract_pregame_lineups(live):
    box = (live.get("liveData") or {}).get("boxscore") or {}
    res = {}
    for side in ("away","home"):
        t = (box.get("teams") or {}).get(side, {}) or {}
        order = t.get("battingOrder") or []
        players = t.get("players") or {}
        lineup = []
        for pid in order:
            pl = players.get(f"ID{pid}", {})
            pos = ((pl.get("position") or {}).get("abbreviation") or "").upper()
            if pos == "P":
                continue
            person = pl.get("person") or {}
            season_bat = (pl.get("seasonStats") or {}).get("batting") or {}
            if not season_bat:
                season_bat = (pl.get("stats") or {}).get("batting") or {}
            avg = fmt_avg(season_bat.get("avg"))
            hr  = season_bat.get("homeRuns") or season_bat.get("hr") or 0
            rbi = season_bat.get("rbi") or season_bat.get("runsBattedIn") or 0
            trip = f"| {avg if avg else ''}/{hr}/{rbi}"
            # name as "K. Stowers" style (first initial + last)
            nm = (person.get("fullName") or "")
            parts = nm.split()
            if parts:
                nm_fmt = f"{parts[0][0]}." + (" " + parts[-1] if len(parts) > 1 else "")
            else:
                nm_fmt = nm
            lineup.append({"pos": pos, "name": nm_fmt, "trip": trip})
        if lineup:
            res[side] = lineup[:9]
    return res

def build_due_up(ls, box, inning_state):
    if inning_state not in ("Middle", "End"):
        return None, []
    try:
        teams = box.get("teams", {}) or {}
        side = "home" if inning_state == "Middle" else "away"
        t = teams.get(side, {}) or {}
        order = t.get("battingOrder") or []
        if not order:
            return side, []
        players = t.get("players") or {}
        next_pid = (ls.get("offense") or {}).get("batter", {}).get("id") or order[0]
        try:
            idx = order.index(next_pid)
        except ValueError:
            idx = 0
        due = []
        for k in range(3):
            pid = order[(idx + k) % len(order)]
            p = players.get(f"ID{pid}") or {}
            nm = (p.get("person") or {}).get("fullName") or ""
            due.append(nm.split()[-1] if nm else "")
        return side, due
    except Exception:
        return None, []

def game_state_and_participants(live, include_records=None):
    gd = live.get("gameData", {}) or {}
    ld = live.get("liveData", {}) or {}
    status = gd.get("status", {}) or {}
    abstract = (status.get("abstractGameState") or "").upper()
    if abstract not in ("PREVIEW","LIVE","FINAL"):
        abstract = "PREVIEW"

    start_iso = gd.get("datetime", {}).get("dateTime") or gd.get("datetime", {}).get("startTimeUTC")
    start_et = to_et(start_iso)

    ls = ld.get("linescore", {}) or {}
    box = ld.get("boxscore", {}) or {}

    inning_state = (ls.get("inningState") or "").strip()
    inning = ls.get("currentInning")
    is_top = ls.get("isTopInning")

    current = ld.get("plays", {}).get("currentPlay", {}) or {}
    matchup = current.get("matchup", {}) or {}
    count = current.get("count", {}) or {}
    balls = count.get("balls", ls.get("balls"))
    strikes = count.get("strikes", ls.get("strikes"))
    outs = count.get("outs", ls.get("outs"))

    bases = {
        "first": bool((ls.get("offense") or {}).get("first")),
        "second": bool((ls.get("offense") or {}).get("second")),
        "third": bool((ls.get("offense") or {}).get("third")),
    }

    # find current batter/pitcher
    home_players = (box.get("teams", {}).get("home", {}).get("players") or {})
    away_players = (box.get("teams", {}).get("away", {}).get("players") or {})
    def find_player(pid):
        if not pid: return None, None
        key = f"ID{pid}"
        if key in home_players: return "home", home_players[key]
        if key in away_players: return "away", away_players[key]
        return None, None

    batter_id = (matchup.get("batter") or {}).get("id")
    pitcher_id = (matchup.get("pitcher") or {}).get("id")

    pitch_side, pitch_obj = find_player(pitcher_id)
    pitch_stats = {}
    if pitch_obj:
        st = (pitch_obj.get("stats") or {}).get("pitching") or {}
        pitch_stats = {
            "name": (pitch_obj.get("person") or {}).get("fullName"),
            "ip": st.get("inningsPitched"),
            "p": st.get("numberOfPitches") or st.get("pitchesThrown"),
            "er": st.get("earnedRuns"),
            "k": st.get("strikeOuts"),
            "bb": st.get("baseOnBalls"),
            "side": pitch_side
        }

    bat_side, bat_obj = find_player(batter_id)
    bat_line = {}
    if bat_obj:
        st = (bat_obj.get("stats") or {}).get("batting") or {}
        ab = st.get("atBats"); h = st.get("hits")
        outcomes = batter_outcomes(live, batter_id)
        line = (f"{h}-{ab}" if h is not None and ab is not None else "")
        suffix = f" • {outcomes}" if outcomes else ""
        bat_line = {"name": (bat_obj.get("person") or {}).get("fullName"),
                    "line": (line + suffix).strip(),
                    "side": bat_side}

    due_side, due_list = build_due_up(ls, box, inning_state)
    due_str = ", ".join([n for n in due_list if n]) if due_list else ""

    last_pitcher_line = {
        "home": team_last_pitcher_line(box, "home"),
        "away": team_last_pitcher_line(box, "away"),
    }

    records = include_records or {"away":"", "home":""}

    if abstract == "LIVE":
        if inning_state in ("Middle", "End") and inning:
            chip = f"{'MID' if inning_state=='Middle' else 'END'} {inning}"
        else:
            chip = f"{'Top' if is_top else 'Bot'} {inning} • {balls}-{strikes}, {outs} out{'s' if outs!=1 else ''}"
    elif abstract == "PREVIEW":
        chip = start_et
    else:
        chip = "Final"

    return {
        "abstract": abstract,
        "startET": start_et,
        "inning": inning,
        "inningState": inning_state,
        "isTop": bool(is_top),
        "balls": balls, "strikes": strikes, "outs": outs,
        "bases": bases,
        "pitcher": pitch_stats,
        "batter": bat_line,
        "records": records,
        "lastPlay": extract_last_play(live),
        "statcast": extract_statcast_line(live),
        "linescore": ls,
        "dueUpSide": due_side,
        "dueUp": due_str,
        "lastPitcherLine": last_pitcher_line,
        "chip": chip,
        "box": box
    }

def shape_game(live, season, records=None):
    gd = live.get("gameData", {}) or {}
    teams = gd.get("teams", {}) or {}
    home = teams.get("home", {}) or {}
    away = teams.get("away", {}) or {}

    state = game_state_and_participants(live, include_records=records)
    ls = state["linescore"]

    status = "scheduled" if state["abstract"] == "PREVIEW" else ("in_progress" if state["abstract"] == "LIVE" else "final")
    game = {
        "gamePk": gd.get("game", {}).get("pk") or live.get("gamePk"),
        "venue": (gd.get("venue") or {}).get("name"),
        "status": status,
        "chip": state["chip"],
        "bases": state["bases"],
        "lastPlay": state["lastPlay"],
        "statcast": state["statcast"],
        "dueUpSide": state["dueUpSide"],
        "dueUp": state["dueUp"],
        "inBreak": state["inningState"] in ("Middle","End"),
        "teams": {
            "home": {"id": home.get("id"), "abbr": home.get("abbreviation") or home.get("clubName"), "record": state["records"].get("home","")},
            "away": {"id": away.get("id"), "abbr": away.get("abbreviation") or away.get("clubName"), "record": state["records"].get("away","")},
        }
    game["scoring"] = extract_scoring_summary(live)
    }

    # linescore columns
    if status == "in_progress":
        game["linescore"] = linescore_blob(ls, force_n=9)
    elif status == "final":
        n_innings = max(9, len(ls.get("innings", []) if ls else []))
        game["linescore"] = linescore_blob(ls, force_n=n_innings)
    else:
        game["linescore"] = None

    # totals -> big score numbers
    if game["linescore"]:
        totals = game["linescore"]["totals"]
        game["teams"]["away"].update({"score": totals["away"]["R"], "hits": totals["away"]["H"], "errors": totals["away"]["E"]})
        game["teams"]["home"].update({"score": totals["home"]["R"], "hits": totals["home"]["H"], "errors": totals["home"]["E"]})
    else:
        game["teams"]["away"]["score"] = None
        game["teams"]["home"]["score"] = None

    # scheduled: probables + pregame lineups
    if status == "scheduled":
        prob = get_probables(gd, season)
        for side in ("away","home"):
            game["teams"][side]["probable"] = prob.get(side, "")
        try:
            game["lineups"] = extract_pregame_lineups(live)
        except Exception:
            game["lineups"] = {}

    # live: current pitcher/batter + break pitcher
    if status == "in_progress":
        pit = state["pitcher"]; bat = state["batter"]
        if pit and pit.get("side") in ("home","away"):
            game["teams"][pit["side"]]["currentPitcher"] = f"P: {pit['name']} • IP {pit.get('ip','-')} • P {pit.get('p','-')} • ER {pit.get('er',0)} • K {pit.get('k',0)} • BB {pit.get('bb',0)}"
        if bat and bat.get("side") in ("home","away"):
            game["teams"][bat["side"]]["currentBatter"] = f"B: {bat['name']} • {bat.get('line','')}"
        # between-innings: show pitcher line for the team about to pitch
        next_pitch_side = None
        if game["inBreak"]:
            if game["dueUpSide"] == "home": next_pitch_side = "away"
            elif game["dueUpSide"] == "away": next_pitch_side = "home"
            if next_pitch_side:
                lp = state["lastPitcherLine"].get(next_pitch_side) or ""
                if lp:
                    game["teams"][next_pitch_side]["breakPitcher"] = lp

    # final: decisions + save with total saves
    if status == "final":
        box = state["box"]
        decisions = live.get("liveData", {}).get("decisions", {}) or {}
        win_id = (decisions.get("winner") or {}).get("id")
        lose_id = (decisions.get("loser") or {}).get("id")
        save_obj = decisions.get("save") or {}

        def pitcher_line_for(pid):
            if not pid: return "", ""
            for side in ("home","away"):
                players = (box.get("teams", {}).get(side, {}).get("players") or {})
                p = players.get(f"ID{pid}")
                if p:
                    st = (p.get("stats") or {}).get("pitching") or {}
                    name = (p.get("person") or {}).get("fullName") or ""
                    return side, f"{name} • IP {st.get('inningsPitched','-')} • P {st.get('numberOfPitches') or st.get('pitchesThrown') or '-'} • ER {st.get('earnedRuns',0)} • K {st.get('strikeOuts',0)} • BB {st.get('baseOnBalls',0)}"
            return "", ""

        win_side, win_line = pitcher_line_for(win_id)
        lose_side, lose_line = pitcher_line_for(lose_id)
        if win_side: game["teams"][win_side]["finalPitcher"] = "W: " + win_line
        if lose_side: game["teams"][lose_side]["finalPitcher"] = "L: " + lose_line

        if save_obj:
            sv_id = save_obj.get("id")
            sv_name = save_obj.get("fullName") or ""
            sv_num = save_obj.get("saves") or save_obj.get("saveNumber") or save_obj.get("save")
            if not sv_num and sv_id:
                try:
                    person = fetch_pitcher_stats(sv_id, SEASON)
                    saves = None
                    for s in person.get("stats", []):
                        if s.get("group", {}).get("displayName") == "pitching":
                            splits = s.get("splits", [])
                            if splits:
                                saves = splits[0].get("stat", {}).get("saves")
                            break
                    if saves is not None:
                        sv_num = saves
                except Exception as ex:
                    log.warning("save total lookup failed for %s: %s", sv_id, ex)
            if win_side and sv_name:
                game["teams"][win_side]["savePitcher"] = f"SV: {sv_name} ({sv_num if sv_num is not None else '-'})"

    # box score (grouped batting + pitching)
    if status in ("in_progress", "final"):
        box = state["box"]
        game["batters"] = {
            "away": extract_batting_box_grouped(box, "away"),
            "home": extract_batting_box_grouped(box, "home"),
        }
        game["pitchers"] = {
            "away": extract_pitching_box(box, "away"),
            "home": extract_pitching_box(box, "home"),
        }
    else:
        game["batters"] = None
        game["pitchers"] = None

    return game

# --------- Routes ---------
@app.route("/")
def home_page():
    return render_template("index.html")

@app.route("/standings")
def standings_page():
    recs, err = fetch_standings_safe()
    data_division = simplify_standings(recs)
    data_wildcard = wildcard_from_division(data_division)
    return render_template(
        "standings.html",
        data_division=data_division,
        data_wildcard=data_wildcard,
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

    # records map
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
    # Local dev
    app.run(host="0.0.0.0", port=5000, debug=True)
