from flask import Flask, render_template, jsonify, request
import requests
import pytz
from datetime import datetime
import logging as log

app = Flask(__name__)

# === Standings (AL/NL) ===
SEASON = 2025
STANDINGS_URL = "https://statsapi.mlb.com/api/v1/standings"
LEAGUE_NAME = {103: "American League", 104: "National League"}
DIVISION_NAME = {
    200: "American League West",
    201: "American League East",
    202: "American League Central",
    203: "National League West",
    204: "National League East",
    205: "National League Central",
}
TEAM_ABBR = {
    109: "ARI", 144: "ATL", 110: "BAL", 111: "BOS", 112: "CHC", 145: "CHW", 113: "CIN",
    114: "CLE", 115: "COL", 116: "DET", 117: "HOU", 118: "KCR", 108: "LAA", 119: "LAD",
    146: "MIA", 158: "MIL", 142: "MIN", 121: "NYM", 147: "NYY", 133: "OAK", 143: "PHI",
    134: "PIT", 135: "SDP", 136: "SEA", 137: "SFG", 138: "STL", 139: "TBR", 140: "TEX",
    141: "TOR", 120: "WSH",
}
PRIMARY_PARAMS = {"leagueId": "103,104", "season": str(SEASON), "standingsTypes": "byDivision"}
FALLBACK_PARAMS = {"leagueId": "103,104", "season": str(SEASON)}

def fetch_standings():
    r = requests.get(STANDINGS_URL, params=PRIMARY_PARAMS, timeout=20)
    r.raise_for_status()
    data = r.json() or {}
    recs = data.get("records") or []
    if recs:
        return recs
    r2 = requests.get(STANDINGS_URL, params=FALLBACK_PARAMS, timeout=20)
    r2.raise_for_status()
    data2 = r2.json() or {}
    return data2.get("records") or []

def _normalize_pct(pct_str):
    if pct_str in (None, ""):
        return 0.0
    try:
        s = str(pct_str).strip()
        return float("0" + s) if s.startswith(".") else float(s)
    except Exception:
        return 0.0

def _last10(tr: dict) -> str:
    recs = (tr.get("records") or {}).get("splitRecords") or []
    for rec in recs:
        if rec.get("type") == "lastTen":
            return f"{rec.get('wins', 0)}-{rec.get('losses', 0)}"
    return ""

def _abbr(team: dict) -> str:
    tid = team.get("id")
    if tid in TEAM_ABBR:
        return TEAM_ABBR[tid]
    name = (team.get("name") or "").replace(" ", "")
    return (name[:3] or "TBD").upper()

def simplify_standings(records):
    leagues = {"National League": [], "American League": []}
    for block in (records or []):
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
                "team_abbr": _abbr(team),
                "team_id": team.get("id"),
                "w": tr.get("wins"),
                "l": tr.get("losses"),
                "pct": _normalize_pct(tr.get("winningPercentage")),
                "gb": tr.get("gamesBack"),
                "streak": (tr.get("streak") or {}).get("streakCode", ""),
                "last10": _last10(tr),
                "runDiff": tr.get("runDifferential"),
            })
        leagues[league_name].append({"division": division_name, "rows": rows})
    return leagues

# === Routes ===
@app.route("/")
def home():
    return render_template("index.html")

@app.route("/standings")
def standings():
    try:
        records = fetch_standings()
        data = simplify_standings(records)
        return render_template("standings.html", data=data, season=SEASON)
    except Exception as e:
        log.exception("standings error")
        safe = {"National League": [], "American League": []}
        return render_template("standings.html", data=safe, season=SEASON, error=str(e))

# === Today’s Games ===
ET_TZ = pytz.timezone("America/New_York")

def extract_last_play(live):
    ld = live.get("liveData", {}) or {}
    plays = ld.get("plays", {}) or {}
    cp = plays.get("currentPlay") or {}
    def id_for(p):
        if not p: return ""
        pid = p.get("playId")
        if pid: return str(pid)
        about = p.get("about", {}) or {}
        return f"{about.get('atBatIndex','')}-{p.get('result',{}).get('eventType','')}"
    if cp:
        desc = (cp.get("result", {}) or {}).get("description")
        if desc:
            return {"id": id_for(cp), "text": desc}
    ap = plays.get("allPlays") or []
    if ap:
        last = ap[-1]
        desc = (last.get("result", {}) or {}).get("description")
        return {"id": id_for(last), "text": desc or ""}
    return {"id": "", "text": ""}

@app.route("/todaysgames")
def todays_games_page():
    return render_template("todaysgames.html")

@app.route("/api/games")
def api_games():
    date_str = request.args.get("date") or datetime.now().strftime("%Y-%m-%d")
    sched_url = "https://statsapi.mlb.com/api/v1/schedule"
    sched_params = {
        "sportId": 1,
        "date": date_str,
        "language": "en",
        "hydrate": "team,linescore,probablePitcher,flags,review,decisions"
    }
    sched = requests.get(sched_url, params=sched_params, timeout=10).json()
    games_out = []
    for date in sched.get("dates", []):
        for g in date.get("games", []):
            gamePk = g["gamePk"]
            live = requests.get(f"https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live").json()
            last_play = extract_last_play(live)
            start_et = datetime.strptime(g["gameDate"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc).astimezone(ET_TZ)
            games_out.append({
                "gamePk": gamePk,
                "status": g["status"]["detailedState"],
                "startTimeLocal": start_et.strftime("%-I:%M %p ET"),
                "teams": g["teams"],
                "lastPlay": last_play["text"],
                "lastPlayId": last_play["id"]
            })
    return jsonify({"games": games_out})

if __name__ == "__main__":
    app.run(debug=True)
