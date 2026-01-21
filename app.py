from datetime import datetime
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from flask import Flask, render_template, request, jsonify
from services.mlb_api import get_player, get_player_stats, get_standings, get_team_schedule

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

from flask import render_template

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
        nl_divs = dict(sorted(nl_divs.items(), key=lambda kv: div_sort_key(kv[0])))

        return render_template("teams.html",
                               title="Teams",
                               season=season,
                               al_divs=al_divs,
                               nl_divs=nl_divs)

    except Exception as e:
       return render_template("teams.html", title="Teams", season=season, al_divs={}, nl_divs={}, error=str(e))


from services.mlb_api import get_team

from services.mlb_api import get_team, get_40man_roster_grouped

@app.get("/team/<int:team_id>")
def team(team_id):
    # Team metadata
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

    # Roster (don’t let API hiccups take down the page)
    roster_grouped, roster_other = {}, {}
    try:
        roster_grouped, roster_other = get_40man_roster_grouped(team_id)
    except Exception as e:
        print(f"[team] roster fetch failed for team_id={team_id}: {e}")

    return render_template(
        "team.html",
        title=team_obj.get("name", "Team"),
        team=team_obj,
        roster_grouped=roster_grouped,
        roster_other=roster_other
    )

@app.get("/game/<int:game_pk>")
def game(game_pk):
    return render_template("game.html", title=f"Game {game_pk}", game_pk=game_pk)

@app.get("/team/<int:team_id>/schedule_json")
def team_schedule_json(team_id):
    season = request.args.get("season", type=int)
    if not season:
        season = datetime.utcnow().year

    data = get_team_schedule(team_id, season)

    games_out = []
    for d in data.get("dates", []) or []:
        for g in d.get("games", []) or []:
            teams = g.get("teams", {}) or {}
            home = teams.get("home", {}) or {}
            away = teams.get("away", {}) or {}

            home_team = (home.get("team") or {})
            away_team = (away.get("team") or {})

            is_home = (home_team.get("id") == team_id)
            opp_team = away_team if is_home else home_team

            venue = g.get("venue", {}) or {}
            loc = venue.get("location", {}) or {}

            probables = g.get("probablePitchers", {}) or {}
            team_prob = probables.get("home" if is_home else "away") or {}
            opp_prob = probables.get("away" if is_home else "home") or {}

            decisions = g.get("decisions", {}) or {}
            winner = decisions.get("winner") or {}
            loser = decisions.get("loser") or {}

            games_out.append({
                "gamePk": g.get("gamePk"),
                "gameDate": g.get("gameDate"),
                "status": (g.get("status") or {}).get("detailedState"),
                "gameType": g.get("gameType"),  # <--- ADD THIS
                "isHome": is_home,
                "opp": {
                    "id": opp_team.get("id"),
                    "abbrev": (opp_team.get("abbreviation") or "").upper()
                },

                "venue": {
                    "name": venue.get("name"),
                    "city": loc.get("city"),
                    "state": loc.get("state") or loc.get("stateAbbrev"),
                },

                "score": {
                    "home": home.get("score"),
                    "away": away.get("score"),
                },

                "pitchers": {
                    "teamProbable": team_prob.get("fullName"),
                    "oppProbable": opp_prob.get("fullName"),
                    "winner": winner.get("fullName"),
                    "loser": loser.get("fullName"),
                }
            })

    return jsonify({"teamId": team_id, "season": season, "games": games_out})

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
    now = datetime.utcnow()
    current_year = now.year
    default_season = current_year if now.month >= 3 else current_year - 1
    seasons = list(range(current_year, current_year - 6, -1))

    season = request.args.get("season", default=default_season, type=int)

    def fetch_records(season_year: int):
        data = get_standings(season_year)

        # If get_standings ever returns a Response-like object, try to json() it
        if hasattr(data, "json") and not isinstance(data, dict):
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
            al_divs=[],
            nl_divs=[],
            error=str(e),
        )

    divisions = []

    for rec in records:
        league = (rec.get("league") or {}).get("name", "")
        division = (rec.get("division") or {}).get("name", "")

        # Fallback mapping if IDs are returned
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

        teams = []
        for tr in rec.get("teamRecords", []):
            team = tr.get("team", {})
            team_id = team.get("id")

            teams.append({
                "team_id": team_id,
                "abbrev": team.get("abbreviation"),
                "w": tr.get("wins"),
                "l": tr.get("losses"),
                "pct": tr.get("pct"),
                "gb": tr.get("gamesBack"),
                "streak": (tr.get("streak") or {}).get("streakCode"),
                "run_diff": tr.get("runDifferential"),
                "logo_url": f"https://www.mlbstatic.com/team-logos/{team_id}.svg" if team_id else None,
            })

        teams.sort(key=lambda x: (-(x["w"] or 0), (x["l"] or 0)))

        divisions.append({
            "name": div_name,
            "league": league,
            "teams": teams
        })

    # Split AFTER the loop (important)
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
        al_divs=al_divs,
        nl_divs=nl_divs,
        error=None,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
