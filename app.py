from datetime import datetime
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from flask import Flask, render_template, request

from services.mlb_api import get_standings

app = Flask(__name__)

@app.get("/")
def home():
    return render_template("home.html", title="Basenerd")

@app.get("/standings")
def standings():
    # If user provides ?season=2025, we use it; otherwise default to current year
    season = request.args.get("season", default=datetime.utcnow().year, type=int)

    try:
        data = get_standings(season)
        records = data.get("records", [])
    except Exception as e:
        # Keep it user-friendly on the page; log details in Render logs.
        return render_template(
            "standings.html",
            title="Standings",
            season=season,
            divisions=[],
            error=str(e),
        )

    # Convert MLB response into a clean structure for the template
divisions = []
for rec in records:
    division = rec.get("division") or {}
    league = rec.get("league") or {}

    division_name = division.get("name", "Unknown Division")
    league_name = league.get("name", "Unknown League")

    team_rows = []
    for tr in rec.get("teamRecords", []):
        team = tr.get("team", {}) or {}

        team_id = team.get("id")
        abbrev = team.get("abbreviation") or team.get("teamName") or ""

        # MLB static logo (works great for small icons)
        logo_url = f"https://www.mlbstatic.com/team-logos/{team_id}.svg" if team_id else None

        team_rows.append({
            "team_id": team_id,
            "abbrev": abbrev,
            "logo_url": logo_url,
            "w": tr.get("wins"),
            "l": tr.get("losses"),
            "pct": tr.get("winningPercentage"),
            "gb": tr.get("gamesBack"),
            "streak": (tr.get("streak") or {}).get("streakCode"),
            "run_diff": tr.get("runDifferential"),
        })

    divisions.append({
        "name": division_name,
        "league": league_name,
        "teams": team_rows
    })

    # Sort divisions for a nicer display order (AL then NL)
    def sort_key(d):
        n = d["name"]
        if n.startswith("American League"):
            return (0, n)
        if n.startswith("National League"):
            return (1, n)
        return (2, n)

    divisions.sort(key=sort_key)

    return render_template(
        "standings.html",
        title="Standings",
        season=season,
        divisions=divisions,
        error=None,
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
