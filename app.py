from datetime import datetime

from flask import Flask, render_template, request

from services.mlb_api import get_standings, get_teams, get_team

app = Flask(__name__)


@app.get("/")
def home():
    return render_template("home.html", title="Basenerd")


@app.get("/standings")
def standings():
    # 1) Choose season (supports /standings?season=2025)
    season = request.args.get("season", default=datetime.utcnow().year, type=int)

    # Season dropdown options (last 6)
    current_year = datetime.utcnow().year
    seasons = list(range(current_year, current_year - 6, -1))

    # Helper to fetch records safely
    def fetch_records(year: int):
        data = get_standings(year)
        return data.get("records", []) if isinstance(data, dict) else []

    # 2) Fetch standings; if empty (common early year), fall back to previous season
    try:
        records = fetch_records(season)
        if not records:
            fallback = fetch_records(season - 1)
            if fallback:
                season = season - 1
                records = fallback
    except Exception as e:
        return render_template(
            "standings.html",
            title="Standings",
            season=season,
            seasons=seasons,
            divisions=[],
            error=str(e),
        )

    # 3) Transform MLB response -> template-friendly structure
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

            # Small team logo (SVG). If SVG ever fails, switch to PNG.
            logo_url = f"https://www.mlbstatic.com/team-logos/{team_id}.svg" if team_id else None

            team_rows.append(
                {
                    "team_id": team_id,
                    "abbrev": abbrev,
                    "logo_url": logo_url,
                    "w": tr.get("wins"),
                    "l": tr.get("losses"),
                    "pct": tr.get("winningPercentage"),
                    "gb": tr.get("gamesBack"),
                    "streak": (tr.get("streak") or {}).get("streakCode"),
                    "run_diff": tr.get("runDifferential"),
                }
            )

        divisions.append({"name": division_name, "league": league_name, "teams": team_rows})

    # 4) Sort divisions into a stable order (NL then AL; East/Central/West)
    league_order = {"National League": 0, "American League": 1}
    div_order = {"East": 0, "Central": 1, "West": 2}

    def division_sort_key(d):
        name = d.get("name", "")
        lg = d.get("league", "")
        suffix = (
            "East"
            if name.endswith("East")
            else "Central"
            if name.endswith("Central")
            else "West"
            if name.endswith("West")
            else ""
        )
        return (league_order.get(lg, 99), div_order.get(suffix, 99), name)

    divisions.sort(key=division_sort_key)

    # 5) Render
    return render_template(
        "standings.html",
        title="Standings",
        season=season,
        seasons=seasons,
        divisions=divisions,
        error=None,
    )


@app.get("/teams")
def teams():
    season = request.args.get("season", default=datetime.utcnow().year, type=int)

    try:
        data = get_teams(season)
        teams_raw = data.get("teams", [])

        # Early-year edge case: fall back a season if empty
        if not teams_raw:
            data_prev = get_teams(season - 1)
            teams_prev = data_prev.get("teams", [])
            if teams_prev:
                season = season - 1
                teams_raw = teams_prev

        teams_clean = []
        for t in teams_raw:
            team_id = t.get("id")
            teams_clean.append(
                {
                    "team_id": team_id,
                    "name": t.get("name"),
                    "abbrev": t.get("abbreviation"),
                    "league": (t.get("league") or {}).get("name"),
                    "division": (t.get("division") or {}).get("name"),
                    "logo_url": f"https://www.mlbstatic.com/team-logos/{team_id}.svg"
                    if team_id
                    else None,
                }
            )

        teams_clean.sort(key=lambda x: (x["league"] or "", x["division"] or "", x["abbrev"] or ""))

        return render_template(
            "teams.html",
            title="Teams",
            season=season,
            teams=teams_clean,
            error=None,
        )

    except Exception as e:
        return render_template(
            "teams.html",
            title="Teams",
            season=season,
            teams=[],
            error=str(e),
        )


@app.get("/teams/<int:team_id>")
def team(team_id: int):
    try:
        data = get_team(team_id)
        t = (data.get("teams") or [{}])[0]

        team_obj = {
            "team_id": t.get("id"),
            "name": t.get("name"),
            "abbrev": t.get("abbreviation"),
            "league": (t.get("league") or {}).get("name"),
            "division": (t.get("division") or {}).get("name"),
            "venue": (t.get("venue") or {}).get("name"),
            "logo_url": f"https://www.mlbstatic.com/team-logos/{team_id}.svg",
        }

        return render_template(
            "team.html",
            title=team_obj.get("abbrev") or "Team",
            team=team_obj,
            error=None,
        )

    except Exception as e:
        return render_template(
            "team.html",
            title="Team",
            team=None,
            error=str(e),
        )


# Local dev only — Render will run: gunicorn app:app
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
