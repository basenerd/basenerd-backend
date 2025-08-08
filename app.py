from flask import Flask, render_template, jsonify
import requests
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("basenerd")

SEASON = 2025
STANDINGS_URL = (
    "https://statsapi.mlb.com/api/v1/standings"
    "?leagueId=103,104&season={season}&standingsTypes=regularSeason"
).format(season=SEASON)


def simplify_standings(records):
    leagues = {"National League": [], "American League": []}
    total_rows = 0

    for block in records or []:
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

    log.info("simplify_standings: leagues=%s NL_divs=%d AL_divs=%d total_rows=%d",
             list(leagues.keys()),
             len(leagues["National League"]),
             len(leagues["American League"]),
             total_rows)

    return leagues


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/standings")
def standings():
    try:
        r = requests.get(STANDINGS_URL, timeout=15, headers={"User-Agent": "basenerd/1.0"})
        r.raise_for_status()
        payload = r.json() or {}
        data = simplify_standings(payload.get("records", []))
        return render_template("standings.html", data=data, season=SEASON)
    except Exception as e:
        log.exception("Failed to fetch standings")
        # Show a friendly message in the page instead of a 500
        return render_template("standings.html", data={"National League": [], "American League": []},
                               season=SEASON,
                               error=str(e)), 200


# Quick peek at what the API returns live on Render
@app.route("/debug/standings.json")
def debug_standings():
    r = requests.get(STANDINGS_URL, timeout=15, headers={"User-Agent": "basenerd/1.0"})
    r.raise_for_status()
    return jsonify(r.json())