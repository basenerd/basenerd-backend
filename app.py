from flask import Flask, render_template, jsonify
import requests
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("basenerd")

SEASON = 2025
BASE_URL = "https://statsapi.mlb.com/api/v1/standings"
HEADERS = {"User-Agent": "basenerd/1.0"}

LEAGUE_NAME = {103: "American League", 104: "National League"}
DIVISION_NAME = {
    200: "American League West",
    201: "American League East",
    202: "American League Central",
    203: "National League West",
    204: "National League East",
    205: "National League Central",
}

PRIMARY_PARAMS = {"leagueId": "103,104", "season": str(SEASON), "standingsTypes": "byDivision"}
FALLBACK_PARAMS = {"leagueId": "103,104", "season": str(SEASON)}

def fetch_standings():
    r = requests.get(BASE_URL, params=PRIMARY_PARAMS, headers=HEADERS, timeout=15)
    r.raise_for_status()
    data = r.json() or {}
    recs = data.get("records") or []
    if recs:
        return recs
    r2 = requests.get(BASE_URL, params=FALLBACK_PARAMS, headers=HEADERS, timeout=15)
    r2.raise_for_status()
    return (r2.json() or {}).get("records") or []

def normalize_pct(pct_str):
    if not pct_str:
        return 0.0
    try:
        return float(pct_str)
    except Exception:
        try:
            s = str(pct_str).strip()
            return float("0" + s) if s.startswith(".") else float(s)
        except Exception:
            return 0.0

def get_last10(tr):
    for rec in (tr.get("splitRecords") or []):
        if rec.get("type") == "lastTen":
            w = rec.get("wins", 0)
            l = rec.get("losses", 0)
            return f"{w}-{l}"
    return ""

def simplify_standings(records):
    leagues = {"National League": [], "American League": []}
    total_rows = 0

    for block in records:
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
                "team_id": team.get("id"),
                "w": tr.get("wins"),
                "l": tr.get("losses"),
                "pct": normalize_pct(tr.get("winningPercentage")),
                "gb": tr.get("gamesBack"),
                "streak": (tr.get("streak") or {}).get("streakCode", ""),
                "last10": get_last10(tr),
                "runDiff": tr.get("runDifferential"),
            })

        if league_name in leagues:
            leagues[league_name].append({"division": division_name, "rows": rows})
            total_rows += len(rows)
        else:
            (leagues["American League"] if league_id == 103 else leagues["National League"]).append(
                {"division": division_name, "rows": rows}
            )
            total_rows += len(rows)

    log.info("simplify_standings: NL_divs=%d AL_divs=%d total_rows=%d",
             len(leagues["National League"]), len(leagues["American League"]), total_rows)
    return leagues

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
        log.exception("Failed to fetch standings")
        return render_template("standings.html",
                               data={"National League": [], "American League": []},
                               season=SEASON, error=str(e)), 200

@app.route("/debug/standings.json")
def debug_standings():
    try:
        return jsonify({"records": fetch_standings()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)