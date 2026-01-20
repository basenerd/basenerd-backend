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

from services.mlb_api import get_teams

@app.get("/teams")
def teams():
    from datetime import datetime
    
    now = datetime.utcnow()
    current_year = now.year
    default_season = current_year if now.month >= 3 else current_year - 1
    season = request.args.get("season", default=default_season, type=int)

    try:
        data = get_teams(season)
        teams_raw = data.get("teams", [])

        # reshape into what teams.html expects
        al_divs = {}
        nl_divs = {}
        for t in teams_raw:
            team_id = t.get("id")
            league_name = (t.get("league") or {}).get("name") or ""
            division_name = (t.get("division") or {}).get("name") or ""
            full_div_name = division_name  # e.g. "American League East"

            team_obj = {
                "team_id": team_id,
                "name": t.get("name"),  # FULL NAME
                "league": league_name,
                "division": division_name,
                "logo_url": f"https://www.mlbstatic.com/team-logos/{team_id}.svg" if team_id else None,
            }
            if "American League" in division_name or league_name == "American League":
                al_divs.setdefault(full_div_name, []).append(team_obj)
            elif "National League" in division_name or league_name == "National League":
                nl_divs.setdefault(full_div_name, []).append(team_obj)
        # sort teams alphabetically within each division (by full name)
        for d in al_divs:
            al_divs[d].sort(key=lambda x: (x.get("name") or ""))
        for d in nl_divs:
            nl_divs[d].sort(key=lambda x: (x.get("name") or ""))
        # optional: order divisions in the classic East/Central/West order
        def div_sort_key(div_name: str):
            if "East" in div_name: return 0
            if "Central" in div_name: return 1
            if "West" in div_name: return 2
            return 99

        al_divs = dict(sorted(al_divs.items(), key=lambda kv: div_sort_key(kv[0])))
        nl_divs = dict(sorted(nl_divs.items(), key=lambda kv: div_sort_key(kv[0])))\

        return render_template("teams.html",
                               title="Teams",
                               season=season,
                               al_divs=al_divs,
                               nl_divs=nl_divs)

    except Exception as e:
       return render_template("teams.html", title="Teams", season=season, al_divs={}, nl_divs={}, error=str(e))


from services.mlb_api import get_team

@app.get("/team/<int:team_id>")
def team(team_id):
    data = get_team(team_id)
    raw = (data.get("teams") or [{}])[0]

    team_obj = {
        "team_id": raw.get("id"),
        "name": raw.get("name"),
        "abbrev": raw.get("abbreviation"),
        "location": raw.get("locationName"),
        "first_year": raw.get("firstYearOfPlay"),
        "league": (raw.get("league") or {}).get("name"),
        "division": (raw.get("division") or {}).get("name"),
        "venue": (raw.get("venue") or {}).get("name"),
        "logo_url": f"https://www.mlbstatic.com/team-logos/{raw.get('id')}.svg" if raw.get("id") else None,
    }

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
            al_divs=[],
            nl_divs=[],
            error=str(e),
        )

    divisions = []

for rec in records:
    # --- Extract league + division safely from StatsAPI shape ---
    league = (rec.get("league") or {}).get("name", "")
    division = (rec.get("division") or {}).get("name", "")

    # Fallback mapping if API ever returns IDs instead of names
    if isinstance(league, int):
        league = "American League" if league == 103 else "National League" if league == 104 else str(league)

    if isinstance(division, int):
        div_map = {
            201: "American League West",
            202: "American League East",
            203: "American League Central",
            204: "National League West",
            205: "National League East",
            206: "National League Central",
        }
        division = div_map.get(division, str(division))

    div_name = division or "Unknown Division"

    # --- Build team rows for this division ---
    teams = []
    for tr in rec.get("teamRecords", []):
        team = tr.get("team", {})

        teams.append({
            "team_id": team.get("id"),
            "abbrev": team.get("abbreviation"),
            "w": tr.get("wins"),
            "l": tr.get("losses"),
            "pct": tr.get("pct"),
            "gb": tr.get("gamesBack"),
            "streak": tr.get("streak", {}).get("streakCode"),
            "run_diff": tr.get("runDifferential"),
            "logo_url": f"https://www.mlbstatic.com/team-logos/{team.get('id')}.svg"
        })

    # Sort teams by wins descending
    teams.sort(key=lambda x: (-x["w"], x["l"]))

    divisions.append({
        "name": div_name,
        "league": league,
        "teams": teams
    })


# --- Split into AL / NL lists ---
al_divs = [d for d in divisions if "American League" in d["name"] or d["league"] == "American League"]
nl_divs = [d for d in divisions if "National League" in d["name"] or d["league"] == "National League"]

print("DEBUG divisions:", len(divisions), "AL:", len(al_divs), "NL:", len(nl_divs))
if divisions:
    print("DEBUG first division:", divisions[0]["league"], divisions[0]["name"], "teams:", len(divisions[0]["teams"]))

return render_template(
    "standings.html",
    title="Standings",
    season=season,
    seasons=seasons,
    divisions=divisions,   # keep for debugging / future use
    al_divs=al_divs,
    nl_divs=nl_divs,
    error=error,
)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
