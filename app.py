from flask import Flask, render_template, jsonify
import requests
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("basenerd")

SEASON = 2025
BASE_URL = "https://statsapi.mlb.com/api/v1/standings"
HEADERS = {"User-Agent": "basenerd/1.0"}

# League & Division ID -> Name fallbacks (API sometimes omits names at top level)
LEAGUE_NAME = {
    103: "American League",
    104: "National League",
}
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

PRIMARY_PARAMS = {
    "leagueId": "103,104",         # 103=AL, 104=NL
    "season": str(SEASON),
    "standingsTypes": "byDivision" # most reliable for division layout
}
FALLBACK_PARAMS = {
    "leagueId": "103,104",
    "season": str(SEASON),
}

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
    """'.586' -> 0.586 (float). Handles None/'' safely."""
    if not pct_str:
        return 0.0
    try:
        return float(pct_str)
    except Exception:
        # pct sometimes already a float, or malformed; try strip leading dot
        try:
            return float(pct_str.strip())
        except Exception:
            if isinstance(pct_str, str) and pct_str.startswith("."):
                try:
                    return float("0" + pct_str)
                except Exception:
                    pass
    return 0.0

def simplify_standings(records):
    """
    Convert MLB API records into easy-to-render structure:

    {
      "National League": [
        {"division": "National League East", "rows": [ {team_name, team_id, w, l, pct, gb}, ... ]},
        ...
      ],
      "American League": [ ... ]
    }
    """
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
            })

        if league_name in leagues:
            leagues[league_name].append({"division": division_name, "rows": rows})
            total_rows += len(rows)
        else:
            # If somehow league_name isn't AL/NL, try to classify by league_id
            if league_id == 103:
                leagues["American League"].append({"division": division_name, "rows": rows})
            elif league_id == 104:
                leagues["National League"].append({"division": division_name, "rows": rows})
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

# Peek endpoint so you can see exactly what Render fetches
@app.route("/debug/standings.json")
def debug_standings():
    try:
        return jsonify({"records": fetch_standings()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # Local dev; Render uses gunicorn per your render.yaml
    app.run(host="0.0.0.0", port=5000, debug=True)