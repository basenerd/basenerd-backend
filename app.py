from flask import Flask, render_template
import requests

app = Flask(__name__)

def simplify_standings(records):
    leagues = {"National League": [], "American League": []}
    for block in records or []:
        # Handle both dict- and object-style responses
        league = (getattr(block, "league", None) or block.get("league", {})).get("name")
        division = (getattr(block, "division", None) or block.get("division", {})).get("name")

        rows = []
        team_records = getattr(block, "teamRecords", None) or block.get("teamRecords", [])
        for tr in team_records:
            team = (getattr(tr, "team", None) or tr.get("team", {}))
            rows.append({
                "team_name": team.get("name", "Team"),
                "team_id": team.get("id"),
                "w": getattr(tr, "wins", None) if hasattr(tr, "wins") else tr.get("wins"),
                "l": getattr(tr, "losses", None) if hasattr(tr, "losses") else tr.get("losses"),
                "pct": float((getattr(tr, "winningPercentage", None) or tr.get("winningPercentage") or 0)),
                "gb": getattr(tr, "gamesBack", None) if hasattr(tr, "gamesBack") else tr.get("gamesBack"),
            })

        if league in leagues:
            leagues[league].append({"division": division, "rows": rows})
    return leagues

@app.route("/standings")
def standings():
    r = requests.get(
        "https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season=2025&standingsTypes=regularSeason",
        timeout=15
    )
    data = simplify_standings(r.json().get("records", []))
    return render_template("standings.html", data=data)

@app.route("/")
def home():
    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)