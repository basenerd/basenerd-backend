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
        division_name = (rec.get("division") or {}).get("name", "Unknown Division")
        team_rows = []

        for tr in rec.get("teamRecords", []):
            team = tr.get("team", {})
            team_rows.append({
                "name": team.get("name", ""),
                "abbrev": team.get("abbreviation") or team.get("teamName") or "",
                "w": tr.get("wins"),
                "l": tr.get("losses"),
                "pct": tr.get("winningPercentage"),
                "gb": tr.get("gamesBack"),
                "streak": (tr.get("streak") or {}).get("streakCode"),
                "last10": tr.get("records", {}).get("splitRecords", []),  # not always present the same way
                "run_diff": tr.get("runDifferential"),
            })

        divisions.append({"name": division_name, "teams": team_rows})

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
