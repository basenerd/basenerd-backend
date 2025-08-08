from flask import Flask, render_template, jsonify
import requests
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("basenerd")

SEASON = 2025
BASE_URL = "https://statsapi.mlb.com/api/v1/standings"
PRIMARY_PARAMS = {
    "leagueId": "103,104",           # 103=AL, 104=NL
    "season": str(SEASON),
    "standingsTypes": "byDivision"   # <-- more reliable than "regularSeason"
}
FALLBACK_PARAMS = {
    "leagueId": "103,104",
    "season": str(SEASON)
}
HEADERS = {"User-Agent": "basenerd/1.0"}

def fetch_standings():
    # Try primary
    r = requests.get(BASE_URL, params=PRIMARY_PARAMS, headers=HEADERS, timeout=15)
    r.raise_for_status()
    data = r.json() or {}
    recs = data.get("records") or []
    if recs:
        return recs

    # Fallback: try without standingsTypes
    log.warning("Primary standings returned empty. Trying fallback without standingsTypes.")
    r2 = requests.get(BASE_URL, params=FALLBACK_PARAMS, headers=HEADERS, timeout=15)
    r2.raise_for_status()
    data2 = r2.json() or {}
    return data2.get("records") or []

def simplify_standings(records):
    leagues = {"National League": [], "American League": []}
    total_rows = 0

    for block in records:
        league_name = (block.get("league") or {}).get("name")
        division_name = (block.get("division") or {}).get("name") or "Division"
        rows = []
        for tr in block.get("teamRecords", []) or []:
            team = tr.get("team", {}) or {}
            wpct = tr.get("winningPercentage")
            rows.append({
                "team_name": team.get("name", "Team"),
                "team_id": team.get("id"),
                "w": tr.get("wins"),
                "l": tr.get("losses"),
                "pct": float(wpct) if wpct not in (None, "") else 0.0,
                "gb": tr.get("gamesBack"),
            })
        if league_name in leagues:
            leagues[league_name].append({"division": division_name, "rows": rows})
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
    # See exactly what Render gets
    try:
        records = fetch_standings()
        return jsonify({"records": records})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)