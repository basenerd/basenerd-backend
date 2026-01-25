from datetime import datetime
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from flask import Flask, render_template, request, jsonify
from services.mlb_api import (
    get_random_player_id,
    get_player_full,
    get_player_headshot_url,
    extract_year_by_year_rows,
    get_player_career_totals,
    get_player_awards,
    build_accolade_pills,
    build_award_year_map,
    group_year_by_year,
    get_player, 
    get_player_stats, 
    get_player_role, 
    get_standings, 
    get_team_schedule,
    get_team_transactions
    )

from services.articles import load_articles, get_article
from services.articles import get_markdown_page
from services.postseason import get_postseason_series, build_playoff_bracket




app = Flask(__name__)

@app.get("/about")
def about():
    page = get_markdown_page("about.md")
    if not page:
        return "About page not found", 404
    return render_template("page.html", title=page["title"], page=page)

from services.mlb_api import get_random_player_id, get_player_full

from services.mlb_api import (
    get_random_player_id,
    get_player_full,
    get_player_headshot_url,
    extract_career_statline,
)

from services.mlb_api import extract_year_by_year_rows

@app.get("/random-player")
def random_player_landing():
    # Just render the page with no player yet
    return render_template(
        "random_player.html",
        player=None,
        yby=None,
        headshot_url=None,
        title="Random Player • Basenerd"
    )


@app.get("/random-player/play")
def random_player_play():
    for _ in range(15):
        pid = get_random_player_id("players_index.json")
        try:
            player = get_player_full(pid)
            headshot_url = get_player_headshot_url(pid, size=420)
            yby = extract_year_by_year_rows(player)
            role = get_player_role(player)
            if role == "pitching":
                pitching_groups = group_year_by_year(yby, "pitching")
                hitting_groups = []
            elif role == "hitting":
                hitting_groups = group_year_by_year(yby, "hitting")
                pitching_groups = []
            else:
                # two-way: show both (if you’d rather force one, tell me which)
                hitting_groups = group_year_by_year(yby, "hitting")
                pitching_groups = group_year_by_year(yby, "pitching")
            # true career totals from separate endpoint (no summing)
            career_hitting = get_player_career_totals(pid, "hitting") if role != "pitching" else None
            career_pitching = get_player_career_totals(pid, "pitching") if role != "hitting" else None
            # accolades
            awards = get_player_awards(pid)
            accolades = build_accolade_pills(awards)
            award_year_map = build_award_year_map(awards)

            return render_template(
                "random_player.html",
                player=player,
                headshot_url=headshot_url,
                yby=yby,
                hitting_groups=hitting_groups,
                pitching_groups=pitching_groups,
                career_hitting=career_hitting,
                career_pitching=career_pitching,
                accolades=accolades,
                award_year_map=award_year_map,
                title="Random Player • Basenerd"
            )
        except Exception:
            continue

    return "Could not fetch a random player right now.", 500




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

from datetime import timedelta

from datetime import timedelta
import re

@app.get("/team/<int:team_id>/transactions_json")
def team_transactions_json(team_id):
    allowed = {7, 14, 30, 60}
    days = request.args.get("days", default=30, type=int)
    if days not in allowed:
        days = 30

    end_dt = datetime.utcnow().date()
    start_dt = end_dt - timedelta(days=days)
    start_date = start_dt.strftime("%Y-%m-%d")
    end_date = end_dt.strftime("%Y-%m-%d")

    data = get_team_transactions(team_id, start_date, end_date)
    txs = (data.get("transactions") or [])

    def pick_date(t: dict) -> str:
        s = (
            t.get("effectiveDate")
            or t.get("transactionDate")
            or t.get("resolutionDate")
            or t.get("date")
            or ""
        )
        return s[:10] if len(s) >= 10 else s

    def team_label(team_obj: dict) -> str:
        if not team_obj:
            return "—"
        return (
            team_obj.get("abbreviation")
            or team_obj.get("teamCode")
            or team_obj.get("name")
            or team_obj.get("locationName")
            or "—"
        )

    def norm(s: str) -> str:
        s = (s or "").strip().lower()
        s = re.sub(r"\s+", " ", s)
        return s

    def strip_player_from_desc(desc: str, player_name: str) -> str:
        """
        Remove the player's name from the description so multi-player trades group together.
        Do a case-insensitive replace; also collapse whitespace after.
        """
        if not desc:
            return ""
        if not player_name:
            return desc
        # Escape name for regex safety; remove first occurrence
        pat = re.compile(re.escape(player_name), re.IGNORECASE)
        out = pat.sub("", desc, count=1)
        out = re.sub(r"\s+", " ", out).strip()
        return out

    grouped = {}  # key -> group dict

    for t in txs:
        date_ymd = pick_date(t)

        person = t.get("person") or {}
        player_id = person.get("id") or None
        player_name = (person.get("fullName") or "").strip()

        tx_type = t.get("type") or t.get("transactionType") or t.get("transactionTypeDescription") or "—"
        desc = t.get("description") or t.get("note") or t.get("notes") or "—"

        from_team = team_label(t.get("fromTeam") or {})
        to_team = team_label(t.get("toTeam") or {})

        base_desc = strip_player_from_desc(desc, player_name)

        key = "|".join([
            norm(date_ymd),
            norm(tx_type),
            norm(from_team),
            norm(to_team),
            norm(base_desc),
        ])
        
        if key not in grouped:
            grouped[key] = {
                "date": date_ymd,
                "type": tx_type,
                "from": from_team,
                "to": to_team,
                # Store the CLEAN base description, not per-player desc
                "base_description": base_desc,
                "players": [],
            }

        if player_id and player_name:
            # Avoid duplicate players in the same group
            if not any(p.get("id") == player_id for p in grouped[key]["players"]):
                grouped[key]["players"].append({"id": player_id, "name": player_name})

    out = []
    for g in grouped.values():
        # Build final description once per group
        desc = g["base_description"]
    
        # If base_description ended up empty (rare), fall back safely
        if not desc or desc == "—":
            desc = g.get("type", "Transaction")
    
        out.append({
            "date": g["date"],
            "description": desc,
            "players": g["players"]
        })
    

    # Sort newest-first
    out.sort(key=lambda x: (x.get("date") or ""), reverse=True)

    return jsonify({
        "teamId": team_id,
        "days": days,
        "startDate": start_date,
        "endDate": end_date,
        "transactions": out,
        "rawCount": len(txs),
        "groupedCount": len(out),
    })


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

@app.get("/articles")
def articles():
    articles = load_articles()
    return render_template("articles.html", title="Articles", articles=articles)

@app.get("/article/<slug>")
def article(slug):
    a = get_article(slug)
    if not a:
        return render_template("article.html", title="Not Found", article=None), 404
    return render_template("article.html", title=a["title"], article=a)

from datetime import datetime
from flask import request, render_template
from services.standings_db import fetch_standings_ranked, build_divs


from datetime import datetime
from flask import request, render_template
from services.standings_db import fetch_standings_ranked
from services.standings_db import build_divs  # if you put it there; otherwise import from wherever you placed it

def build_divs(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    # Group rows into divisions for your template structure:
    # d = { name: "AL East", teams: [ ... ] }
    by_league_div = {}
    for r in rows:
        league = r.get("league") or ""
        division = r.get("division") or ""
        by_league_div.setdefault((league, division), []).append(r)

    al_divs, nl_divs = [], []

    for (league, division), teams in by_league_div.items():
        # Ensure teams are sorted by the computed division_rank
        teams_sorted = sorted(teams, key=lambda x: (x.get("division_rank") or 999, -(x.get("pct") or 0)))

        # Map DB row -> template team object
        mapped = []
        for t in teams_sorted:
            team_id = t["team_id"]
            mapped.append({
                "team_id": team_id,
                "abbrev": t.get("team_abbrev") or "",
                "w": t.get("w"),
                "l": t.get("l"),
                "pct": (f'{t["pct"]:.3f}' if t.get("pct") is not None else "—"),
                "gb": t.get("gb") or "—",
                "streak": t.get("streak") or "—",
                "run_diff": t.get("run_differential"),
                "logo_url": f"https://www.mlbstatic.com/team-logos/{team_id}.svg",
                # Optional if you want to use these later:
                "division_leader": t.get("division_leader"),
                "wild_card": t.get("wild_card"),
                "division_rank": t.get("division_rank"),
                "wc_gb": t.get("wc_gb"),
            })

        div_obj = {"name": division, "teams": mapped}

        if league == "American League":
            al_divs.append(div_obj)
        elif league == "National League":
            nl_divs.append(div_obj)

    # Keep divisions in a consistent order (optional)
    def div_sort_key(d):
        name = d["name"]
        order = {
            "American League East": 1, "American League Central": 2, "American League West": 3,
            "National League East": 1, "National League Central": 2, "National League West": 3,
        }
        return order.get(name, 99), name

    al_divs = sorted(al_divs, key=div_sort_key)
    nl_divs = sorted(nl_divs, key=div_sort_key)

    return al_divs, nl_divs

@app.get("/standings")
def standings():
    from datetime import datetime
    from flask import request, render_template
    from services.standings_db import fetch_standings_ranked, build_divs
    from services.postseason_db import fetch_postseason_series_rows, build_playoff_picture

    now = datetime.utcnow()
    current_year = now.year
    default_season = current_year if now.month >= 3 else current_year - 1

    season = request.args.get("season", default=default_season, type=int)
    seasons = list(range(2021, default_season + 1))

    view = (request.args.get("view", "division") or "division").lower()
    if view not in ("division", "wildcard", "playoffs"):
        view = "division"

    # helper: build the little “chip” dict the template expects
    def _chip(row):
        if not row:
            return None
        tid = int(row.get("team_id") or 0)
        abbrev = (row.get("team_abbrev") or row.get("abbrev") or "").upper()
        return {
            "team_id": tid,
            "abbrev": abbrev,
            "logo_url": f"https://www.mlbstatic.com/team-logos/{tid}.svg" if tid else None,
        }

    # helper: pick division winners + WC straight from DB rows (tiebreak-safe)
    def _pick_playoff(rows, league_name: str):
        league_rows = [r for r in rows if (r.get("league") == league_name)]

        div_winners = [r for r in league_rows if int(r.get("division_rank") or 0) == 1]
        east = next((r for r in div_winners if "East" in (r.get("division") or "")), None)
        central = next((r for r in div_winners if "Central" in (r.get("division") or "")), None)
        west = next((r for r in div_winners if "West" in (r.get("division") or "")), None)

        wc_rows = sorted(
            [r for r in league_rows if int(r.get("wild_card_rank") or 0) in (1, 2, 3)],
            key=lambda x: int(x.get("wild_card_rank") or 99),
        )

        return {
            "east": _chip(east),
            "central": _chip(central),
            "west": _chip(west),
            "wc1": _chip(wc_rows[0]) if len(wc_rows) > 0 else None,
            "wc2": _chip(wc_rows[1]) if len(wc_rows) > 1 else None,
            "wc3": _chip(wc_rows[2]) if len(wc_rows) > 2 else None,
        }

    # Wild card table rows (exclude division leaders)
    def _map_wc_row(r):
        tid = int(r.get("team_id") or 0)
        return {
            "team_id": tid,
            "abbrev": (r.get("team_abbrev") or r.get("abbrev") or "").upper(),
            "w": r.get("w"),
            "l": r.get("l"),
            "pct": r.get("pct"),
            "wc_gb": r.get("wc_gb"),
            "streak": r.get("streak") or "—",
            "run_diff": r.get("run_differential"),
            "wild_card_rank": r.get("wild_card_rank"),
            "logo_url": f"https://www.mlbstatic.com/team-logos/{tid}.svg" if tid else None,
        }

    def _build_wc(rows, league_name: str):
        league_rows = [r for r in rows if (r.get("league") == league_name)]
        league_rows = [r for r in league_rows if int(r.get("division_rank") or 0) != 1]

        league_rows_sorted = sorted(
            league_rows,
            key=lambda x: (int(x.get("wild_card_rank") or 99), -(x.get("pct") or 0)),
        )
        return [_map_wc_row(r) for r in league_rows_sorted]

    error = None
    al_divs, nl_divs = [], []
    al_playoff, nl_playoff = None, None
    al_wc, nl_wc = [], []
    playoff_picture = None

    try:
        rows = fetch_standings_ranked(season)

        if not rows:
            error = f"No standings found for {season}. Make sure standings + standings_season_final are populated."
        else:
            # Division tables
            al_divs, nl_divs = build_divs(rows)

            # Top playoff chips block
            al_playoff = _pick_playoff(rows, "American League")
            nl_playoff = _pick_playoff(rows, "National League")

            # Wild card tables
            al_wc = _build_wc(rows, "American League")
            nl_wc = _build_wc(rows, "National League")

        # Playoff Picture bracket (DB views). If the views have no data for that season, show an error banner.
        if view == "playoffs":
            ps_rows = fetch_postseason_series_rows(season)
            playoff_picture = build_playoff_picture(ps_rows) if ps_rows else {"AL": {"F": [], "D": [], "L": []},
                                                                             "NL": {"F": [], "D": [], "L": []},
                                                                             "WS": {"W": []}}
            if ps_rows == []:
                error = error or f"No postseason series found for {season} in vw_postseason_series / vw_postseason_series_team_enriched."

    except Exception as e:
        error = f"Standings DB query failed: {e}"

    return render_template(
        "standings.html",
        title="Standings",
        season=season,
        seasons=seasons,
        view=view,
        al_divs=al_divs,
        nl_divs=nl_divs,
        al_playoff=al_playoff,
        nl_playoff=nl_playoff,
        al_wc=al_wc,
        nl_wc=nl_wc,
        playoff_picture=playoff_picture,
        is_current_season=(season == default_season),
        error=error,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
