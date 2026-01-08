from datetime import datetime
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from flask import Flask, render_template, request

from services.mlb_api import get_standings

app = Flask(__name__)

@app.get("/")
def home():
    return render_template("home.html", title="Basenerd")

from datetime import datetime
from flask import request, render_template

from services.mlb_api import get_standings


@app.get("/standings")
def standings():
    # 1) Pick season (allow ?season=2025). Default to current year.
    season = request.args.get("season", default=datetime.utcnow().year, type=int)

    # Helper to fetch standings records safely
    def fetch_records(season_year: int):
        data = get_standings(season_year)
        return data.get("records", []) if isinstance(data, dict) else []

    # 2) Fetch standings; if empty (common in early year), fall back to previous season
    try:
        records = fetch_records(season)
        if not records:
            records = fetch_records(season - 1)
            if records:
                season = season - 1
    except Exception as e:
        return render_template(
            "standings.html",
            title="Standings",
            season=season,
            divisions=[],
            error=str(e),
        )

    # 3) Transform MLB response → template-friendly structure
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

            # Small team logo (SVG). If SVG ever fails in a browser, we can switch to PNG.
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
            "teams": team_rows,
        })

    # 4) Sort divisions into a stable, readable order
    # MLB division names usually look like:
    # "American League East", "National League West", etc.
    league_order = {"National League": 0, "American League": 1}
    div_order = {"East": 0, "Central": 1, "West": 2}

    def division_sort_key(d):
        name = d.get("name", "")
        league = d.get("league", "")
        # figure out East/Central/West from the division name
        suffix = "East" if name.endswith("East") else "Central" if name.endswith("Central") else "West" if name.endswith("West") else ""
        return (league_order.get(league, 99), div_order.get(suffix, 99), name)

    divisions.sort(key=division_sort_key)

    # 5) Render page
    return render_template(
        "standings.html",
        title="Standings",
        season=season,
        divisions=divisions,
        error=None,
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
