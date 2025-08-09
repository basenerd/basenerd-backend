from flask import Flask, render_template, jsonify, request
import requests, time, logging, re
from datetime import datetime
try:
    import pytz
    ET_TZ = pytz.timezone("America/New_York")
except Exception:
    ET_TZ = None

# ------------- Flask -------------
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("basenerd")

# ------------- HTTP client with retries -------------
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def make_session():
    s = requests.Session()
    retries = Retry(
        total=4,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"])
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": "basenerd/1.0 (+https://basenerd.app)"})
    return s

SESSION = make_session()

# ------------- Constants / caches -------------
STATS = "https://statsapi.mlb.com/api/v1"
LIVE  = "https://statsapi.mlb.com/api/v1.1"

_PITCHER_CACHE = {}   # id -> cached person stats (for probables/saves W-L/ERA/handedness)
_CACHE = {}           # date -> cached games payload

# ------------- Utilities -------------
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

def fmt_avg(v):
    if v in (None, "", "-"):
        return ""
    try:
        x = float(v)
        return f"{x:.3f}".replace("0.", ".")
    except Exception:
        s = str(v).strip()
        if s.startswith("0."):
            return s.replace("0.", ".")
        return s

def fetch_json(url, params=None, timeout=20):
    r = SESSION.get(url, params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()

def fetch_schedule(date_str):
    url = f"{STATS}/schedule"
    params = {"sportId": 1, "date": date_str}
    js = fetch_json(url, params)
    dates = js.get("dates") or []
    if dates:
        return dates[0].get("games", [])
    return []

def fetch_live(game_pk):
    return fetch_json(f"{LIVE}/game/{game_pk}/feed/live")

def fetch_pitcher_stats(pid, season):
    now = time.time()
    c = _PITCHER_CACHE.get(pid)
    if c and now - c["ts"] < 6*3600:
        return c["data"]
    url = f"{STATS}/people/{pid}"
    params = {"hydrate": f"stats(group=pitching,type=season,season={season})"}
    data = fetch_json(url, params).get("people", [{}])[0]
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
        a = inn.get("away", {})
        h = inn.get("home", {})
        away_by.append(a if isinstance(a, (int, str)) else a.get("runs", ""))
        home_by.append(h if isinstance(h, (int, str)) else h.get("runs", ""))
    n = len(away_by)
    if force_n and n < force_n:
        away_by += [""] * (force_n - n)
        home_by += [""] * (force_n - n)
        n = force_n
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
    return {"n": max(n, force_n or 0), "away": away_by, "home": home_by, "totals": totals}

# ---------- Unified latest play ----------
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
    ev = hd.get("launchSpeed")
    la = hd.get("launchAngle")
    dist = hd.get("totalDistance")
    xba = (hd.get("estimatedBA")
           or hd.get("estimatedBa")
           or hd.get("estimatedBattingAverage")
           or hd.get("xba")
           or hd.get("expectedBattingAverage"))
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
        except:
            pass
    if xba is not None:
        try:
            x = float(xba)
            if x > 1.0: x = x / 100.0
            parts.append(f"xBA: {x:.3f}".replace("0.", "."))
        except:
            s = str(xba).strip()
            if s:
                parts.append(f"xBA: {s}")
    return " • ".join(parts)

# ----- Scoring notation helpers -----
PAREN_CODE_RE = re.compile(r"\(([1-9](?:-[1-9]){0,3})\)")
ABBREV_TO_NUM = {"P":1,"C":2,"1B":3,"2B":4,"3B":5,"SS":6,"LF":7,"CF":8,"RF":9}
WORD_TO_NUM = {
    "pitcher":1,"catcher":2,"first baseman":3,"first base":3,"second baseman":4,"second base":4,
    "third baseman":5,"third base":5,"shortstop":6,"left fielder":7,"left field":7,
    "center fielder":8,"center field":8,"right fielder":9,"right field":9
}

# Extracts a sequence like 6-4-3 from play. Prefers credits; falls back to description.
def fielder_chain(play):
    credits = play.get("credits") or []
    assists = []
    putouts = []
    errors = []
    for c in credits:
        credit = (c.get("credit") or "").lower()
        pos = None
        posobj = c.get("position") or {}
        code = posobj.get("code")
        abbr = (posobj.get("abbrev") or "").upper()
        if code and str(code).isdigit():
            pos = int(code)
        elif abbr in ABBREV_TO_NUM:
            pos = ABBREV_TO_NUM[abbr]
        if pos is None:
            continue
        if "assist" in credit:
            assists.append(pos)
        elif "putout" in credit:
            putouts.append(pos)
        elif "error" in credit:
            errors.append(pos)

    if assists or putouts:
        chain = assists + (putouts[-1:] if putouts else [])
        if chain:
            return "-".join(str(n) for n in chain)

    # Fallback: parenthetical like (5-3) in description
    desc = ((play.get("result") or {}).get("description") or "")
    m = PAREN_CODE_RE.search(desc)
    if m:
        return m.group(1)

    # Fallback: "to shortstop" etc.
    m2 = re.search(r"\bto ([a-z ]+?)(?:$|,|\.|\s)", desc.lower())
    if m2:
        w = m2.group(1).strip()
        if w in WORD_TO_NUM:
            return str(WORD_TO_NUM[w])

    return ""

def out_air_prefix(event_type):
    et = (event_type or "").lower()
    if "pop" in et:
        return "P"
    if "line" in et:
        return "L"
    # default fly
    return "F"

# Convert a single play to standard scorekeeping token
def play_to_token(play):
    res = (play.get("result") or {})
    et = (res.get("eventType") or "").lower()
    desc = (res.get("description") or "")

    # Hitting events with simple codes
    if et == "single": return "1B"
    if et == "double": return "2B"
    if et == "triple": return "3B"
    if et == "home_run": return "HR"
    if "walk" in et: return "BB"
    if et == "hit_by_pitch": return "HBP"
    if et == "catcher_interf": return "CI"
    if et in ("intent_walk", "intentional_walk"): return "BB"

    # Strikeouts
    if et.startswith("strikeout"):
        return "K" if et != "strikeout_double_play" else "KDP"

    # Sacrifices
    if et in ("sac_fly", "sac_fly_double_play"):
        pos = fielder_chain(play)
        return f"SF{pos}" if pos else "SF"
    if et in ("sac_bunt", "sac_bunt_double_play"):
        pos = fielder_chain(play)
        return f"SH{pos}" if pos else "SH"

    # Errors
    if "error" in et:
        chain = fielder_chain(play)
        return f"E{chain}" if chain else "E"

    # Fielder's choice
    if "fielders_choice" in et:
        chain = fielder_chain(play)
        return f"FC{chain}" if chain else "FC"

    # Ground/force/double/triple plays -> chain
    if et in ("groundout", "force_out", "double_play", "triple_play", "grounded_into_double_play"):
        chain = fielder_chain(play)
        return chain or "GO"

    # Air outs (flyout/lineout/popout)
    if any(k in et for k in ("flyout", "lineout", "pop_out", "foul_popout")):
        prefix = out_air_prefix(et)
        chain = fielder_chain(play)
        if chain:
            # for air outs fielder_chain is typically single number (putout fielder)
            return f"{prefix}{chain}"
        # fallback to position word in description
        m2 = re.search(r"\bto ([a-z ]+?)(?:$|,|\.|\s)", desc.lower())
        if m2 and m2.group(1).strip() in WORD_TO_NUM:
            return f"{prefix}{WORD_TO_NUM[m2.group(1).strip()]}"
        return prefix

    # Last resort: parenthetical numbers or event text
    m = PAREN_CODE_RE.search(desc)
    if m:
        return m.group(1)
    evshort = (res.get("event") or "").upper().replace(" ", "_")
    return evshort[:6] if evshort else ""

# Build batter’s sequence of outcome tokens in game order
def batter_outcomes(live, batter_id):
    if not batter_id:
        return ""
    allp = (live.get("liveData", {}) or {}).get("plays", {}).get("allPlays", []) or []
    tokens = []
    for p in allp:
        m = p.get("matchup") or {}
        b = (m.get("batter") or {}).get("id")
        if b != batter_id:
            continue
        # Use MLB classification to include true PAs (walks/HBP included)
        et = ((p.get("result") or {}).get("eventType") or "").lower()
        if not et:
            continue
        token = play_to_token(p)
        if token:
            tokens.append(token)
    return ", ".join(tokens)

# ----- Other helpers you already had -----
def team_last_pitcher_line(box, side):
    t = (box.get("teams", {}) or {}).get(side, {}) or {}
    pitchers = t.get("pitchers") or []
    players = t.get("players") or {}
    if not pitchers:
        return ""
    pid = pitchers[-1]
    pobj = players.get(f"ID{pid}") or {}
    st = (pobj.get("stats") or {}).get("pitching") or {}
    name = (pobj.get("person") or {}).get("fullName") or ""
    if not name:
        return ""
    return f"P: {name} • IP {st.get('inningsPitched','-')} • P {st.get('numberOfPitches') or st.get('pitchesThrown') or '-'} • ER {st.get('earnedRuns',0)} • K {st.get('strikeOuts',0)} • BB {st.get('baseOnBalls',0)}"

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
            lineup.append({"pos": pos, "name": (person.get("fullName") or ""), "trip": trip})
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

# ------------- State builders -------------
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
        "chip": chip
    }

def extract_pregame_lineups_wrapped(live):
    try:
        return extract_pregame_lineups(live)
    except Exception:
        return {}

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
    }

    if status == "in_progress":
        game["linescore"] = linescore_blob(ls, force_n=9)
    elif status == "final":
        n_innings = max(9, len(ls.get("innings", []) if ls else []))
        game["linescore"] = linescore_blob(ls, force_n=n_innings)
    else:
        game["linescore"] = None

    if game["linescore"]:
        totals = game["linescore"]["totals"]
        game["teams"]["away"].update({"score": totals["away"]["R"], "hits": totals["away"]["H"], "errors": totals["away"]["E"]})
        game["teams"]["home"].update({"score": totals["home"]["R"], "hits": totals["home"]["H"], "errors": totals["home"]["E"]})
    else:
        game["teams"]["away"]["score"] = None
        game["teams"]["home"]["score"] = None

    if status == "scheduled":
        prob = get_probables(gd, season)
        for side in ("away","home"):
            game["teams"][side]["probable"] = prob.get(side, "")
        game["lineups"] = extract_pregame_lineups_wrapped(live)

    if status == "in_progress":
        pit = state["pitcher"]; bat = state["batter"]
        if pit and pit.get("side") in ("home","away"):
            game["teams"][pit["side"]]["currentPitcher"] = f"P: {pit['name']} • IP {pit.get('ip','-')} • P {pit.get('p','-')} • ER {pit.get('er',0)} • K {pit.get('k',0)} • BB {pit.get('bb',0)}"
        if bat and bat.get("side") in ("home","away"):
            game["teams"][bat["side"]]["currentBatter"] = f"B: {bat['name']} • {bat.get('line','')}"
        next_pitch_side = None
        if game["inBreak"]:
            if game["dueUpSide"] == "home": next_pitch_side = "away"
            elif game["dueUpSide"] == "away": next_pitch_side = "home"
            if next_pitch_side:
                lp = state["lastPitcherLine"].get(next_pitch_side) or ""
                if lp:
                    game["teams"][next_pitch_side]["breakPitcher"] = lp

    if status == "final":
        box = live.get("liveData", {}).get("boxscore", {})
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
        if win_side:
            game["teams"][win_side]["finalPitcher"] = "W: " + win_line
        if lose_side:
            game["teams"][lose_side]["finalPitcher"] = "L: " + lose_line

        if save_obj:
            sv_id = save_obj.get("id")
            sv_name = save_obj.get("fullName") or ""
            sv_num = save_obj.get("saves") or save_obj.get("saveNumber") or save_obj.get("save")
            if not sv_num and sv_id:
                try:
                    person = fetch_pitcher_stats(sv_id, season)
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

    return game

# ------------- Cache helpers -------------
def cache_get(key):
    x = _CACHE.get(key)
    if not x: return None
    if time.time() - x["ts"] > x["ttl"]:
        return None
    return x["data"]

def cache_set(key, data, ttl):
    _CACHE[key] = {"ts": time.time(), "ttl": ttl, "data": data}

# ------------- Routes -------------
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
        return jsonify(data), 502

    if not schedule:
        data = {"date": date_str, "games": [], "note": "mlb_schedule_empty"}
        cache_set(date_str, data, 60)
        return jsonify(data)

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
        if not pk:
            continue
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

# ---- Debug proxy endpoints (optional) ----
@app.route("/api/debug/schedule")
def debug_schedule():
    date_str = request.args.get("date") or datetime.utcnow().strftime("%Y-%m-%d")
    try:
        raw = fetch_json(f"{STATS}/schedule", {"sportId": 1, "date": date_str})
        return jsonify({"ok": True, "date": date_str, "raw": raw})
    except Exception as e:
        return jsonify({"ok": False, "date": date_str, "error": str(e)}), 502

@app.route("/api/debug/live")
def debug_live():
    pk = request.args.get("pk")
    if not pk:
        return jsonify({"ok": False, "error": "missing pk"}), 400
    try:
        raw = fetch_json(f"{LIVE}/game/{pk}/feed/live")
        return jsonify({"ok": True, "pk": pk, "raw": raw})
    except Exception as e:
        return jsonify({"ok": False, "pk": pk, "error": str(e)}), 502

# ---- Other pages (keep working) ----
@app.route("/standings")
def standings_page():
    return render_template("standings.html")

@app.route("/")
def home_page():
    return render_template("index.html")

@app.route("/ping")
def ping():
    return "ok", 200

# ------------- Entrypoint -------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
