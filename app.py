from datetime import datetime
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from flask import Flask, render_template, request
from services.mlb_api import get_player, get_player_stats, get_standings

app = Flask(__name__)

@app.get("/")
def home():
    return render_template("home.html", title="Basenerd")

from datetime import datetime
from flask import request, render_template

from services.mlb_api import get_standings


from datetime import datetime
from flask import request, render_template

from services.mlb_api import get_teams, get_team, search_players  # add to imports

@app.get("/teams")
def teams():
    current_year = datetime.utcnow().year
    season = request.args.get("season", default=current_year, type=int)
    data = get_teams(season)
    teams_list = data.get("teams", [])
    return render_template("teams.html", title="Teams", season=season, teams=teams_list)

@app.get("/team/<int:team_id>")
def team(team_id):
    data = get_team(team_id)
    team_obj = (data.get("teams") or [{}])[0]
    return render_template("team.html", title=team_obj.get("name", "Team"), team=team_obj)

@app.get("/players")
def players():
    q = request.args.get("q", "").strip()
    results = search_players(q) if q else []
    return render_template("players.html", q=q, results=results)

@app.get("/player/<int:player_id>")
def player(player_id):
    season = request.args.get("season", type=int)  # optional: /player/123?season=2025
    bio = get_player(player_id)
    stats = get_player_stats(player_id, season=season)
    return render_template("player.html", bio=bio, stats=stats, season=season)

@app.get("/standings")
def standings():
    # Build season dropdown first so it's available even on errors
    current_year = datetime.utcnow().year
    seasons = list(range(current_year, current_year - 6, -1))

    # Pick season (allow ?season=2025). Default to current year.
    season = request.args.get("season", default=current_year, type=int)

    def fetch_records(season_year: int):
        data = get_standings(season_year)

        # DEBUG: uncomment for Render logs
        # print("DEBUG get_standings type:", type(data), "season:", season_year)

        # If get_standings accidentally returns a Response object, try to JSON it
        if hasattr(data, "json"):
            data = data.json()

        if not isinstance(data, dict):
            return []

        return data.get("records", []) or []

    try:
        records = fetch_records(season)

        # If empty (common early year), fall back to previous season
        if not records:
            prev = fetch_records(season - 1)
            if prev:
                records = prev
                season = season - 1

    except Exception as e:
        return render_template(
            "standings.html",
            title="Standings",
            season=season,
            seasons=seasons,
            divisions=[],
            error=str(e),
        )

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

    # Sort divisions into stable order
    league_order = {"National League": 0, "American League": 1}
    div_order = {"East": 0, "Central": 1, "West": 2}

    def division_sort_key(d):
        name = d.get("name", "")
        league = d.get("league", "")
        suffix = "East" if name.endswith("East") else "Central" if name.endswith("Central") else "West" if name.endswith("West") else ""
        return (league_order.get(league, 99), div_order.get(suffix, 99), name)

    divisions.sort(key=division_sort_key)

    # Optional: if STILL empty, surface an explicit message in template via error
    error = None
    if not divisions:
        error = "Standings API returned no records for the selected (or fallback) season."
        
    al_divs = [d for d in divisions if d.get("league") == "American League"]
    nl_divs = [d for d in divisions if d.get("league") == "National League"]

    print("DEBUG divisions:", len(divisions), "AL:", len(al_divs), "NL:", len(nl_divs))
    if divisions:
        print("DEBUG first division:", divisions[0]["league"], divisions[0]["name"], "teams:", len(divisions[0]["teams"]))

    return render_template(
    "standings.html",
    title="Standings",
    season=season,
    seasons=seasons,
    divisions=divisions,   # keep for debugging / future use
    al_divs=al_divs,       # <-- important if template uses these
    nl_divs=nl_divs,       # <-- important if template uses these
    error=error,
)



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
