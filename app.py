from flask import Flask, render_template, jsonify
import requests
import logging

app = Flask(__name__)

# ---- Logging ----
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("basenerd")

# ---- Config ----
SEASON = 2025
BASE_URL = "https://statsapi.mlb.com/api/v1/standings"
HEADERS = {"User-Agent": "basenerd/1.0"}

# League & Division ID -> Name (fallbacks if API omits names at top level)
LEAGUE_NAME = {103: "American League", 104: "National League"}
DIVISION_NAME = {
    # AL
    200: "American League West",
    201: "American League East",
    202: "American League Central",
    # NL
    203: "National League West",
    204: "National League East",
    205: "National League Central",
}

# Hardcoded MLB team abbreviations (consistent 3-letter codes)
# Keys are MLB StatsAPI team IDs
TEAM_ABBR = {
    109: "ARI",  # Arizona Diamondbacks
    144: "ATL",  # Atlanta Braves
    110: "BAL",  # Baltimore Orioles
    111: "BOS",  # Boston Red Sox
    112: "CHC",  # Chicago Cubs
    145: "CHW",  # Chicago White Sox
    113: "CIN",  # Cincinnati Reds
    114: "CLE",  # Cleveland Guardians
    115: "COL",  # Colorado Rockies
    116: "DET",  # Detroit Tigers
    117: "HOU",  # Houston Astros
    118: "KCR",  # Kansas City Royals
    108: "LAA",  # Los Angeles Angels
    119: "LAD",  # Los Angeles Dodgers
    146: "MIA",  # Miami Marlins
    158: "MIL",  # Milwaukee Brewers
    142: "MIN",  # Minnesota Twins
    121: "NYM",  # New York Mets
    147: "NYY",  # New York Yankees
    133: "OAK",  # Oakland Athletics
    143: "PHI",  # Philadelphia Phillies
    134: "PIT",  # Pittsburgh Pirates
    135: "SDP",  # San Diego Padres
    136: "SEA",  # Seattle Mariners
    137: "SFG",  # San Francisco Giants
    138: "STL",  # St. Louis Cardinals
    139: "TBR",  # Tampa Bay Rays
    140: "TEX",  # Texas Rangers
    141: "TOR",  # Toronto Blue Jays
    120: "WSH",  # Washington Nationals
}

PRIMARY_PARAMS = {
    "leagueId": "103,104",           # 103=AL, 104=NL
    "season": str(SEASON),
    "standingsTypes": "byDivision",  # robust for division layout
}
FALLBACK_PARAMS = {
    "leagueId": "103,104",
    "season": str(SEASON),
}

# ---- Helpers ----
def fetch_standings():
    """Fetch standings with a fallback if the primary query returns empty."""
    r = requests.get(BASE_URL, params=PRIMARY_PARAMS, headers=HEADERS, timeout=15)
    r.raise_for_status()
    data = r.json() or {}
    recs = data.get("records") or []
    if recs:
        return recs

    log.warning("Primary standings empty. Retrying without standingsTypes.")
    r2 = requests.get(BASE_URL, params=FALLBACK_PARAMS, headers=HEADERS, timeout=15)
    r2.raise_for_status()
    data2 = r2.json() or {}
    return data2.get("records") or []

def normalize_pct(pct_str):
    """'.586' -> 0.586; handles None/'' or already-floats safely."""
    if pct_str in (None, ""):
        return 0.0
    try:
        s = str(pct_str).strip()
        return float("0" + s) if s.startswith(".") else float(s)
    except Exception:
        return 0.0

def get_last10(tr: dict) -> str:
    """
    Return 'W-L' for last ten games from records.splitRecords[type=lastTen].
    MLB nests this under teamRecords[i].records.splitRecords.
    """
    recs = (tr.get("records") or {}).get("splitRecords") or []
    for rec in recs:
        if rec.get("type") == "lastTen":
            return f"{rec.get('wins', 0)}-{rec.get('losses', 0)}"
    return ""

def hardcoded_abbr(team: dict) -> str:
    """Always prefer our hardcoded 3-letter code; fallback to a derived one."""
    tid = team.get("id")
    if tid in TEAM_ABBR:
        return TEAM_ABBR[tid]
    name = (team.get("name") or "").replace(" ", "")
    return (name[:3] or "TBD").upper()

def simplify_standings(records):
    """
    Convert MLB API records into easy-to-render structure:

    {
      "National League": [
        {"division": "National League East", "rows": [
          {"team_name","team_abbr","team_id","w","l","pct","gb","streak","last10","runDiff"}, ...
        ]},
        ...
      ],
      "American League": [ ... ]
    }
    """
    leagues = {"National League": [], "American League": []}
    total_rows = 0

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
                "team_abbr": hardcoded_abbr(team),
                "team_id": team.get("id"),
                "w": tr.get("wins"),
                "l": tr.get("losses"),
                "pct": normalize_pct(tr.get("winningPercentage")),
                "gb": tr.get("gamesBack"),
                "streak": (tr.get("streak") or {}).get("streakCode", ""),
                "last10": get_last10(tr),
                "runDiff": tr.get("runDifferential"),
            })

        # Ensure it lands under AL/NL (even if API omits league names)
        if league_name in leagues:
            leagues[league_name].append({"division": division_name, "rows": rows})
        elif league_id == 103:
            leagues["American League"].append({"division": division_name, "rows": rows})
        elif league_id == 104:
            leagues["National League"].append({"division": division_name, "rows": rows})
        else:
            leagues["National League"].append({"division": division_name, "rows": rows})

        total_rows += len(rows)

    log.info("simplify_standings: NL_divs=%d AL_divs=%d total_rows=%d",
             len(leagues["National League"]), len(leagues["American League"]), total_rows)
    return leagues

# ---- Routes ----
@app.route("/")
def home():
    return render_template("index.html")

@app.route("/standings")
def standings():
    try:
        records = fetch_standings()
        data = simplify_standings(records)
        # Ensure keys exist even if empty
        if "National League" not in data: data["National League"] = []
        if "American League" not in data: data["American League"] = []
        return render_template("standings.html", data=data, season=SEASON)
    except Exception as e:
        log.exception("Failed to fetch standings")
        safe = {"National League": [], "American League": []}
        return render_template("standings.html", data=safe, season=SEASON, error=str(e)), 200

@app.route("/debug/standings.json")
def debug_standings():
    """Peek endpoint so you can see exactly what the server fetched."""
    try:
        return jsonify({"records": fetch_standings()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/ping")
def ping():
    return "ok", 200

# ---- Local dev entrypoint ----
if __name__ == "__main__":
    # Render uses gunicorn; this is for local testing.
    app.run(host="0.0.0.0", port=5000, debug=True)
