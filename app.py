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
    get_team_transactions,
    league_name_to_short,
    build_stat_distributions,
    get_qualified_league_player_stats,
    pct_to_bg,
    percentile_from_sorted,
    to_float,
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
        yby_bg_hitting=yby_bg_hitting,
        yby_bg_pitching=yby_bg_pitching,
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

    # --- Helpers ---

    POS_CODES = [
        "RHP","LHP","P","C","1B","2B","3B","SS",
        "LF","CF","RF","OF","IF","DH","INF","UT","PH","PR"
    ]
    _pos_re = "|".join(POS_CODES)

    def pick_date(t: dict) -> str:
        s = (
            t.get("effectiveDate")
            or t.get("transactionDate")
            or t.get("resolutionDate")
            or t.get("date")
            or ""
        )
        return s[:10] if len(s) >= 10 else s

    def canonical_trade_signature(desc: str) -> str:
        """
        Convert trade descriptions so multi-player rows collapse to one signature.
        Replaces 'POS Player Name' with 'POS PLAYER'.
        """
        if not desc:
            return ""

        s = desc.strip()

        # Replace "RHP Freddy Peralta", "SS Jett Williams", etc.
        s = re.sub(
            rf"\b({_pos_re})\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+(?:\s+(?:Jr\.|Sr\.|II|III|IV))?)\b",
            r"\1 PLAYER",
            s
        )

        s = re.sub(r"\s+", " ", s).strip().lower()
        return s

    # --- Grouping ---

    grouped = {}

    for t in txs:
        date_ymd = pick_date(t)

        person = t.get("person") or {}
        player_id = person.get("id") or None
        player_name = (person.get("fullName") or "").strip()

        desc = (
            t.get("description")
            or t.get("note")
            or t.get("notes")
            or "—"
        )

        # Build grouping key
        sig = canonical_trade_signature(desc)
        key = f"{date_ymd}|{sig}"

        # Create group if new
        if key not in grouped:
            grouped[key] = {
                "date": date_ymd,
                # Keep the longest description we encounter as canonical display text
                "description": desc,
                "players": []
            }
        else:
            if len(desc or "") > len(grouped[key]["description"] or ""):
                grouped[key]["description"] = desc

        # Collect involved players
        if player_id and player_name:
            if not any(p["id"] == player_id for p in grouped[key]["players"]):
                grouped[key]["players"].append({
                    "id": player_id,
                    "name": player_name
                })

    # --- Final output ---

    out = list(grouped.values())
    out.sort(key=lambda x: (x.get("date") or ""), reverse=True)

    return jsonify({
        "teamId": team_id,
        "days": days,
        "startDate": start_date,
        "endDate": end_date,
        "rawCount": len(txs),
        "groupedCount": len(out),
        "transactions": out
    })


@app.get("/players")
def players():
    q = request.args.get("q", "").strip()
    results = search_players(q) if q else []
    return render_template("players.html", q=q, results=results)

from datetime import datetime
from services.mlb_api import (
    get_player,
    get_player_role,
    get_player_full,
    extract_year_by_year_rows,
    group_year_by_year,
    get_player_career_totals,
    get_player_season_stats_live,
    find_best_season_with_stats,
    get_player_game_log,
    get_player_transactions,
    build_player_header,
)

from datetime import datetime
from flask import request, render_template

from services.mlb_api import (
    get_player,
    get_player_full,
    get_player_role,
    get_player_career_totals,
    extract_year_by_year_rows,
    get_player_game_log,
    extract_game_log_rows,
)

from datetime import datetime
from flask import request, render_template

from services.mlb_api import (
    get_player,
    get_player_role,
    get_player_full,
    extract_year_by_year_rows,
    group_year_by_year,
    get_player_career_totals,
    get_player_game_log,
    extract_game_log_rows,
    get_player_transactions,
    get_player_awards,
    build_accolade_pills,
    build_award_year_map,
)

@app.get("/player/<int:player_id>")
def player(player_id: int):
    now = datetime.utcnow()
    current_year = now.year

    # optional query params
    log_year = request.args.get("log_year", type=int)

    bio = get_player(player_id)
    role = get_player_role(bio)

    # debut year (stop searching after debut year)
    debut = (bio.get("mlbDebutDate") or "")[:10]
    try:
        debut_year = int(debut[:4]) if debut else (current_year - 25)
    except Exception:
        debut_year = current_year - 25

    # -------------------------
    # Year-by-year source (single source of truth)
    # -------------------------
    player_full = get_player_full(player_id)
    yby = extract_year_by_year_rows(player_full)

    # Build groups (same as random player)
    if role == "pitching":
        pitching_groups = group_year_by_year(yby, "pitching")
        hitting_groups = []
    elif role == "hitting":
        hitting_groups = group_year_by_year(yby, "hitting")
        pitching_groups = []
    else:
        hitting_groups = group_year_by_year(yby, "hitting")
        pitching_groups = group_year_by_year(yby, "pitching")

    # -------------------------
    # Season snapshot (derived from yby rows)
    # If gamesPlayed is null/0 -> go back a year
    # -------------------------
    def _to_int(x):
        try:
            return int(x)
        except Exception:
            return 0

    # Prefer Total row when a year has multiple teams
    # key: (year, kind) -> row dict
    by_year_kind = {}
    for r in (yby or []):
        yr_raw = r.get("year")
        try:
            yr = int(yr_raw)
        except Exception:
            continue
    
        kind = r.get("kind")  # "hitting" / "pitching"
        if kind not in ("hitting", "pitching"):
            continue
    
        key = (yr, kind)
        cur = by_year_kind.get(key)
    
        if cur is None:
            by_year_kind[key] = r
        else:
            # prefer Total
            if r.get("team") == "Total" and cur.get("team") != "Total":
                by_year_kind[key] = r

    def games_ok(stat: dict) -> bool:
        if not stat:
            return False
        return _to_int(stat.get("gamesPlayed")) > 0

    season_found = None
    snapshot_hitting = {}
    snapshot_pitching = {}

    for yr in range(current_year, debut_year - 1, -1):
        hit_row = by_year_kind.get((yr, "hitting"))
        pit_row = by_year_kind.get((yr, "pitching"))

        hit_stat = (hit_row or {}).get("stat") or {}
        pit_stat = (pit_row or {}).get("stat") or {}

        if role == "two-way":
            # choose the year if either has games
            if games_ok(hit_stat) or games_ok(pit_stat):
                season_found = yr
                snapshot_hitting = hit_stat if games_ok(hit_stat) else {}
                snapshot_pitching = pit_stat if games_ok(pit_stat) else {}
                break
        elif role == "pitching":
            if games_ok(pit_stat):
                season_found = yr
                snapshot_pitching = pit_stat
                break
        else:
            if games_ok(hit_stat):
                season_found = yr
                snapshot_hitting = hit_stat
                break

    
    # -------------------------
    # League-relative gradients (qualified pools by league)
    # -------------------------
    EXCLUDE_KEYS = {"gamesPlayed", "gamesStarted"}  # user: no gradients for games
    # Rate stats (used only for the non-qualified gray rule)
    HITTING_RATE_KEYS = {"avg", "obp", "slg", "ops", "babip", "woba"}
    PITCHING_RATE_KEYS = {"era", "whip", "walksPer9Inn", "hitsPer9Inn", "homeRunsPer9Inn", "strikeoutsPer9Inn"}

    _dist_cache = {}

    def is_rate_stat(kind: str, key: str) -> bool:
        if kind == "hitting":
            if key in HITTING_RATE_KEYS:
                return True
            return key.endswith("Rate")
        else:
            if key in PITCHING_RATE_KEYS:
                return True
            return key.endswith("Rate") or "Per9" in key or "Per9Inn" in key

    # Stat directions: lower is better (invert percentile)
    HITTING_INVERT = {"strikeOutRate", "caughtStealingRate"}
    PITCHING_INVERT = {"era", "whip", "walksPer9Inn", "hitsPer9Inn", "homeRunsPer9Inn", "earnedRuns", "runs", "homeRuns", "baseOnBalls", "hits"}

    def invert_needed(kind: str, key: str) -> bool:
        if kind == "hitting":
            return key in HITTING_INVERT or key == "strikeOuts"  # if this ever shows up
        return key in PITCHING_INVERT

    def league_for_group(g: dict) -> str:
        # If the player appeared in both leagues in the same year, use the league of the team they finished with.
        parts = g.get("parts") or []
        if parts:
            last = parts[-1]
            return league_name_to_short(last.get("league") or "") or ""
        total = (g.get("total") or {})
        return league_name_to_short(total.get("league") or "") or ""

    def bg_for_value(season: int, kind: str, league_short: str, key: str, raw_val, qualified: bool):
        if key in EXCLUDE_KEYS:
            return None

        v = to_float(raw_val)
        if v is None:
            return None

        # non-qualified: gray out rate stats only; keep counting stats normal
        if not qualified and is_rate_stat(kind, key):
            return "rgba(0, 0, 0, 0.06)"

        # If not qualified and not a rate stat → no gradient
        if not qualified:
            return None

        ck = f"{season}:{kind}:{league_short}"
        dists = _dist_cache.get(ck)
        if dists is None:
            dists = build_stat_distributions(season, kind, league_short)
            _dist_cache[ck] = dists
        arr = dists.get(key) or []
        if not arr:
            return None

        p = percentile_from_sorted(arr, v)
        if invert_needed(kind, key):
            p = 1.0 - p
        return pct_to_bg(p)

    # Build background maps for:
    # - snapshot card (season_found)
    # - year-by-year table rows (total + team splits)
    snapshot_bg_hitting = {}
    snapshot_bg_pitching = {}
    snapshot_qual_hitting = False
    snapshot_qual_pitching = False

    yby_bg_hitting = {}
    yby_bg_pitching = {}

    def build_row_bg(row_key: str, season: int, kind: str, league_short: str, stat: dict, qualified: bool):
        out = {}
        if not isinstance(stat, dict):
            return out
        for k, raw in stat.items():
            bg = bg_for_value(season, kind, league_short, k, raw, qualified)
            if bg:
                out[k] = bg
        return out

    # Snapshot qualification (use qualified pool membership to match MLB rule)
    if season_found:
        y = str(season_found)
        # Find league for that season from the grouped rows (prefers last team if multi-league)
        hg_match = next((g for g in (hitting_groups or []) if str(g.get("year")) == y), None)
        pg_match = next((g for g in (pitching_groups or []) if str(g.get("year")) == y), None)

        if hg_match and snapshot_hitting:
            lg = league_for_group(hg_match)
            pool = get_qualified_league_player_stats(season_found, "hitting", lg)
            snapshot_qual_hitting = (player_id in pool)
            snapshot_bg_hitting = build_row_bg("snapshot", season_found, "hitting", lg, snapshot_hitting, snapshot_qual_hitting)

        if pg_match and snapshot_pitching:
            lg = league_for_group(pg_match)
            pool = get_qualified_league_player_stats(season_found, "pitching", lg)
            snapshot_qual_pitching = (player_id in pool)
            snapshot_bg_pitching = build_row_bg("snapshot", season_found, "pitching", lg, snapshot_pitching, snapshot_qual_pitching)

    # Year-by-year rows (total + parts)
    for g in (hitting_groups or []):
        season = int(g.get("year") or 0)
        if not season:
            continue
        lg = league_for_group(g)
        pool = get_qualified_league_player_stats(season, "hitting", lg)
        qual = (player_id in pool)
        total = (g.get("total") or {})
        stat = total.get("stat") or {}
        row_key = f"{season}:Total"
        yby_bg_hitting[row_key] = build_row_bg(row_key, season, "hitting", lg, stat, qual)

        for p in (g.get("parts") or []):
            ps = (p.get("stat") or {})
            team = (p.get("team") or "").strip() or "UNK"
            rk = f"{season}:{team}"
            yby_bg_hitting[rk] = build_row_bg(rk, season, "hitting", lg, ps, qual)

    for g in (pitching_groups or []):
        season = int(g.get("year") or 0)
        if not season:
            continue
        lg = league_for_group(g)
        pool = get_qualified_league_player_stats(season, "pitching", lg)
        qual = (player_id in pool)
        total = (g.get("total") or {})
        stat = total.get("stat") or {}
        row_key = f"{season}:Total"
        yby_bg_pitching[row_key] = build_row_bg(row_key, season, "pitching", lg, stat, qual)

        for p in (g.get("parts") or []):
            ps = (p.get("stat") or {})
            team = (p.get("team") or "").strip() or "UNK"
            rk = f"{season}:{team}"
            yby_bg_pitching[rk] = build_row_bg(rk, season, "pitching", lg, ps, qual)

# -------------------------
    # Career totals (mini cards)
    # -------------------------
    career_hitting = get_player_career_totals(player_id, "hitting") if role != "pitching" else None
    career_pitching = get_player_career_totals(player_id, "pitching") if role != "hitting" else None

    # -------------------------
    # Awards (for year-by-year pills)
    # -------------------------
    awards = []
    accolades = []
    award_year_map = {}
    try:
        awards = get_player_awards(player_id)
        accolades = build_accolade_pills(awards)
        award_year_map = build_award_year_map(awards)
    except Exception:
        pass

    # -------------------------
    # Game logs (dropdown)
    # -------------------------
    years = list(range(debut_year, current_year + 1))
    years.reverse()

    if not log_year:
        log_year = season_found or current_year

    try:
        game_log_blocks = get_player_game_log(player_id, season=log_year)
        game_logs = extract_game_log_rows(game_log_blocks)  # opponent abbreviations enforced in helper
    except Exception:
        game_logs = {"hitting": [], "pitching": []}

    # -------------------------
    # Transactions
    # -------------------------
    try:
        transactions = get_player_transactions(player_id)
    except Exception:
        transactions = []

    return render_template(
        "player.html",
        title=f"{bio.get('fullName','Player')} • Basenerd",
        bio=bio,
        role=role,

        # season snapshot derived from yby
        season_found=season_found,
        snapshot_hitting=snapshot_hitting,
        snapshot_pitching=snapshot_pitching,
        snapshot_bg_hitting=snapshot_bg_hitting,
        snapshot_bg_pitching=snapshot_bg_pitching,
        snapshot_qual_hitting=snapshot_qual_hitting,
        snapshot_qual_pitching=snapshot_qual_pitching,

        # career
        career_hitting=career_hitting,
        career_pitching=career_pitching,

        # year-by-year (random-player table compatibility)
        yby=yby,
        hitting_groups=hitting_groups,
        pitching_groups=pitching_groups,
        yby_bg_hitting=yby_bg_hitting,
        yby_bg_pitching=yby_bg_pitching,
        accolades=accolades,
        award_year_map=award_year_map,

        # game logs
        years=years,
        log_year=log_year,
        game_logs=game_logs,

        # transactions
        transactions=transactions,
    )



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
