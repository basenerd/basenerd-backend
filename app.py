from flask import Flask, render_template
import requests

app = Flask(__name__)

SEASON = 2025
STANDINGS_URL = (
    "https://statsapi.mlb.com/api/v1/standings"
    "?leagueId=103,104&season={season}&standingsTypes=regularSeason"
).format(season=SEASON)


def simplify_standings(records):
    """
    Flatten the MLB Stats API response into a dict the template can loop easily:
      {
        "National League": [ { "division": "NL West", "rows": [ {...}, ... ] }, ... ],
        "American League": [ ... ]
      }
    """
    leagues = {"National League": [], "American League": []}

    for block in records or []:
        # Defensive lookups
        league_name = (block.get("league") or {}).get("name")
        division_name = (block.get("division") or {}).get("name") or "Division"
        team_records = block.get("teamRecords", []) or []

        rows = []
        for tr in team_records:
            team = tr.get("team", {}) or {}
            wpct = tr.get("winningPercentage")
            # Normalize fields
            rows.append(
                {
                    "team_name": team.get("name", "Team"),
                    "team_id": team.get("id"),
                    "w": tr.get("wins"),
                    "l": tr.get("losses"),
                    "pct": float(wpct) if wpct not in (None, "") else 0.0,
                    "gb": tr.get("gamesBack"),
                }
            )

        if league_name in leagues:
            leagues[league_name].append({"division": division_name, "rows": rows})

    return leagues


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/standings")
def standings():
    # Fetch once per request (simple + safe). You can add caching later if you want.
    r = requests.get(STANDINGS_URL, timeout=15)
    r.raise_for_status()
    payload = r.json() or {}
    data = simplify_standings(payload.get("records", []))
    return render_template("standings.html", data=data, season=SEASON)


if __name__ == "__main__":
    # Local dev server; Render will use gunicorn per your render.yaml
    app.run(host="0.0.0.0", port=5000, debug=True)
if __name__ == "__main__":
    app.run(debug=True)