from datetime import datetime
import json
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from flask import Flask, render_template, request, jsonify
from services.spray_db import fetch_player_spray
from services.savant_profile import get_player_savant_profile
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
    get_games_for_date,
    find_next_date_with_games,
    get_game_feed,
    normalize_game_detail,
    get_schedule_game_by_pk,
    normalize_schedule_game,
    get_stats_leaderboard,
    get_teams,
    get_team,
    get_40man_roster_grouped,
    get_40man_directory, 
    filter_40man_by_letter, 
    suggest_40man_players,
    )
from services.pitching_report import (
    list_pitching_games,
    pitching_report_summary,
    pitching_heatmap,
)
from services.articles import load_articles, get_article
from services.articles import get_markdown_page
from services.postseason import get_postseason_series, build_playoff_bracket

import random
# Load the active players list once when the app starts
try:
    with open("active_players.json", "r") as f:
        ACTIVE_PLAYER_IDS = json.load(f)
except Exception as e:
    print(f"Warning: Could not load active_players.json: {e}")
    ACTIVE_PLAYER_IDS = []


app = Flask(__name__)

@app.template_global()
def sr_color(pct):
    if pct is None:
        return "rgba(229,231,235,1)"
    try:
        p = int(pct)
    except Exception:
        return "rgba(229,231,235,1)"
    p = max(0, min(100, p))

    # blue -> gray -> red
    if p <= 50:
        t = p / 50.0
        r = round(37  + (229 - 37) * t)
        g = round(99  + (231 - 99) * t)
        b = round(235 + (235 - 235) * t)
    else:
        t = (p - 50) / 50.0
        r = round(229 + (239 - 229) * t)
        g = round(231 + (68  - 231) * t)
        b = round(235 + (68  - 235) * t)

    return f"rgb({r},{g},{b})"
    
@app.get("/about")
def about():
    page = get_markdown_page("about.md")
    if not page:
        return "About page not found", 404
    return render_template("page.html", title=page["title"], page=page)

from services.mlb_api import get_random_player_id, get_player_full

def teamname_to_svg(team_name: str) -> str:
    s = (team_name or "").lower().strip()
    s = s.replace(" ", "_")
    s = re.sub(r"[^a-z0-9_]", "", s)
    return f"{s}.svg"
    
from services.mlb_api import (
    get_random_player_id,
    get_player_full,
    get_player_headshot_url,
    extract_career_statline,
)
# -------------------------
# Venue -> Stadium SVG mapping (MLB parks)
# Filenames are nickname-only: angels.svg, marlins.svg, etc.
# If a venue isn't recognized (special event parks), we fall back to home team's park.
# -------------------------
_VENUE_TO_SVG = {
    # AL
    "angel stadium": "angels.svg",
    "minute maid park": "astros.svg",
    "oakland coliseum": "athletics.svg",
    "t-mobile park": "mariners.svg",
    "globe life field": "rangers.svg",

    "camden yards": "orioles.svg",
    "yankee stadium": "yankees.svg",
    "fenway park": "red_sox.svg",
    "rogers centre": "blue_jays.svg",
    "tropicana field": "rays.svg",

    "progressive field": "guardians.svg",
    "comerica park": "tigers.svg",
    "guaranteed rate field": "white_sox.svg",
    "kauffman stadium": "royals.svg",
    "target field": "twins.svg",

    # NL
    "truist park": "braves.svg",
    "loanDepot park": "marlins.svg",
    "nationals park": "nationals.svg",
    "citi field": "mets.svg",
    "citizens bank park": "phillies.svg",

    "wrigley field": "cubs.svg",
    "great american ball park": "reds.svg",
    "american family field": "brewers.svg",
    "pnc park": "pirates.svg",
    "busch stadium": "cardinals.svg",

    "chase field": "dbacks.svg",
    "coors field": "rockies.svg",
    "dodger stadium": "dodgers.svg",
    "petco park": "padres.svg",
    "oracle park": "giants.svg",
}

def _norm_venue_name(name: str) -> str:
    s = (name or "").strip().lower()
    s = s.replace("®", "").replace("’", "'")
    s = re.sub(r"\s+", " ", s)
    return s



from services.mlb_api import extract_year_by_year_rows

@app.get("/player/<int:player_id>/spray.json")
def player_spray_json(player_id: int):
    season = request.args.get("season", type=int)

    # If season wasn't provided, fall back to current year.
    # (Your template defaults to season_found anyway.)
    if not season:
        season = datetime.utcnow().year

    points = fetch_player_spray(player_id, season, limit=1000)
    return jsonify({"player_id": player_id, "season": season, "points": points})
@app.get("/player/<int:player_id>/pitching_games.json")
def player_pitching_games_json(player_id: int):
    season = request.args.get("season", type=int) or datetime.utcnow().year
    try:
        games = list_pitching_games(player_id, season)
        return jsonify({"ok": True, "player_id": player_id, "season": season, "games": games})
    except Exception as e:
        return jsonify({"ok": False, "player_id": player_id, "season": season, "reason": str(e)}), 200

@app.get("/player/<int:player_id>/pitching_report.json")
def player_pitching_report_json(player_id: int):
    season = request.args.get("season", type=int)
    game_pk = request.args.get("game_pk", type=int)
    if not season:
        season = datetime.utcnow().year
    data = pitching_report_summary(player_id, season, game_pk)
    return jsonify(data)

@app.get("/player/<int:player_id>/pitching_heatmap.json")
def player_pitching_heatmap_json(player_id: int):
    season = request.args.get("season", type=int)
    game_pk = request.args.get("game_pk", type=int)
    pitch_type = request.args.get("pitch_type", default="", type=str)
    stand = request.args.get("stand", default="L", type=str)  # "L" or "R"
    metric = request.args.get("metric", default="density", type=str)

    if not season:
        season = datetime.utcnow().year
    if not pitch_type:
        return jsonify({"ok": False, "reason": "missing_pitch_type"})

    data = pitching_heatmap(
        pitcher_id=player_id,
        season=season,
        pitch_type=pitch_type,
        stand_lr=stand.upper(),
        metric=metric.lower(),
        game_pk=game_pk,
    )
    return jsonify(data)    
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

@app.get("/stats")
def stats_page():
    """
    Stats leaderboard UI page.
    Data loads via /stats_json so we can paginate/sort/filter without reloading.
    """
    now = datetime.utcnow()
    current_year = now.year
    default_season = current_year if now.month >= 3 else current_year - 1

    # initial defaults
    season = request.args.get("season", default=default_season, type=int)
    group = (request.args.get("group") or "batting").strip().lower()

    # map UI group -> API stat group
    group_map = {
        "batting": "hitting",
        "pitching": "pitching",
        "fielding": "fielding",
        "baserunning": "running",
    }
    api_group = group_map.get(group, "hitting")

    # Build team dropdown options for the chosen season
    try:
        teams_data = get_teams(season)
        teams_raw = teams_data.get("teams", []) or []
        teams = [{"id": t.get("id"), "name": t.get("name")} for t in teams_raw if t.get("id")]
        teams.sort(key=lambda x: (x.get("name") or ""))
    except Exception:
        teams = []

    # Position options (simple, you can expand any time)
    positions = ["", "P", "C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "OF", "IF", "DH"]
    leagues = [
        {"id": "", "label": "All Leagues"},
        {"id": 103, "label": "AL"},
        {"id": 104, "label": "NL"},
    ]

    return render_template(
        "stats.html",
        title="Stats • Basenerd",
        season=season,
        group=group,          # batting/pitching/fielding/baserunning
        api_group=api_group,  # hitting/pitching/fielding/running
        teams=teams,
        positions=positions,
        leagues=leagues
    )


@app.get("/stats_json")
def stats_json():
    """
    JSON data endpoint used by stats.html

    Supports:
      - cols: comma-separated stat keys to display
      - filters: JSON array of AND rules [{"stat":"homeRuns","op":">=","value":20}, ...]
      - click-to-sort via sort/order
      - filtered paging: page is 1-based, 50 rows per page *after* filtering
    """
    now = datetime.utcnow()
    current_year = now.year
    default_season = current_year if now.month >= 3 else current_year - 1

    group = (request.args.get("group") or "batting").strip().lower()
    group_map = {
        "batting": "hitting",
        "pitching": "pitching",
        "fielding": "fielding",
        "baserunning": "running",
    }
    api_group = group_map.get(group, "hitting")

    season = request.args.get("season", default=default_season, type=int)
    team_id = request.args.get("team_id", type=int)
    league_id = request.args.get("league_id", type=int)
    position = (request.args.get("position") or "").strip() or None

    # Default QUALIFIED (per your request)
    pool = (request.args.get("pool") or "QUALIFIED").strip().upper()

    sort_stat = (request.args.get("sort") or "").strip()
    order = (request.args.get("order") or "desc").strip().lower()
    if order not in ("asc", "desc"):
        order = "desc"

    page = request.args.get("page", default=1, type=int)
    if not page or page < 1:
        page = 1

    # Selected columns (echoed back; UI renders from this)
    cols_raw = (request.args.get("cols") or "").strip()
    selected_cols = [c.strip() for c in cols_raw.split(",") if c.strip()] if cols_raw else []

    # Filters: JSON list of {stat, op, value}
    filters_raw = (request.args.get("filters") or "").strip()
    try:
        filters = json.loads(filters_raw) if filters_raw else []
    except Exception:
        filters = []

    # --------------------------
    # Helpers for filtering
    # --------------------------
    def _to_float(x):
        try:
            if x is None:
                return None
            # already numeric?
            if isinstance(x, (int, float)):
                return float(x)
            s = str(x).strip()
            if s == "" or s.lower() == "none":
                return None
            return float(s)
        except Exception:
            return None

    def _passes_one(stat_dict: dict, rule: dict) -> bool:
        if not isinstance(stat_dict, dict):
            return False
        k = (rule.get("stat") or "").strip()
        op = (rule.get("op") or "").strip()
        v_raw = rule.get("value")

        if not k or not op:
            return True  # ignore malformed rule

        left = _to_float(stat_dict.get(k))
        right = _to_float(v_raw)

        # If missing/non-numeric -> FAIL (your desired behavior)
        if left is None or right is None:
            return False

        if op == ">=": return left >= right
        if op == "<=": return left <= right
        if op == ">":  return left > right
        if op == "<":  return left < right
        if op == "=":  return left == right
        if op == "!=": return left != right
        return True

    def _passes_all(stat_dict: dict, rules: list) -> bool:
        if not rules:
            return True
        for r in rules:
            if not _passes_one(stat_dict, r):
                return False
        return True

    # --------------------------
    # Filtered paging strategy
    # --------------------------
    PAGE_SIZE = 50
    CHUNK = 200
    MAX_SCAN = 4000  # safety cap (raw rows scanned)
    target_start = (page - 1) * PAGE_SIZE
    target_end = target_start + PAGE_SIZE
    
    # Disallow sort keys that StatsAPI /stats can't sort by (person fields, non-stat fields)
    UNSORTABLE = {"age", "numberOfPitches"}
    if sort_stat in UNSORTABLE:
        sort_stat = ""  # force default below

    # If no sortStat provided, pick a reasonable default for group
    if not sort_stat:
        if api_group == "pitching":
            sort_stat = "era"
        elif api_group == "fielding":
            sort_stat = "fielding"
        elif api_group == "running":
            sort_stat = "stolenBases"
        else:
            sort_stat = "ops"

    # Collect filtered rows until we can serve this filtered page
    raw_offset = 0
    filtered_rows = []
    scanned = 0
    available_keys = set()

    while len(filtered_rows) < target_end and scanned < MAX_SCAN:
        data = get_stats_leaderboard(
            group=api_group,
            season=season,
            sort_stat=sort_stat,
            order=order,
            team_id=team_id,
            position=position,
            league_id=league_id,
            player_pool=pool,
            limit=CHUNK,
            offset=raw_offset,
            game_type="R",
        )

        rows = (data.get("rows") or [])
        if not rows:
            break

        scanned += len(rows)

        for r in rows:
            st = (r.get("stat") or {})
            if isinstance(st, dict):
                for k in st.keys():
                    available_keys.add(k)

            if _passes_all(st, filters):
                filtered_rows.append(r)

        # If API returned fewer than chunk, we’re at the end
        if len(rows) < CHUNK:
            break

        raw_offset += CHUNK

    # Slice the requested filtered page
    page_rows = filtered_rows[target_start:target_end]

    # Rank labels are relative to filtered ordering
    rank_start = target_start + 1
    rank_end = target_start + len(page_rows)

    # Default columns returned to UI (so it can pre-check)
    default_cols = {
        "hitting": ["gamesPlayed","plateAppearances","avg","obp","slg","ops","homeRuns","rbi","runs","hits","stolenBases"],
        "pitching": ["wins","losses","era","whip","inningsPitched","strikeOuts","baseOnBalls","hits","homeRuns"],
        "fielding": ["games","innings","fielding","assists","putOuts","errors","doublePlays"],
        "running": ["stolenBases","caughtStealing","stolenBasePercentage","runs"],
    }.get(api_group, [])

    # Approx “matched” count: we only know how many we matched in the scanned window
    filtered_total_approx = len(filtered_rows)

    return jsonify({
        "group": group,
        "apiGroup": api_group,
        "season": season,
        "teamId": team_id,
        "leagueId": league_id,
        "position": position,
        "playerPool": pool,
        "sortStat": sort_stat,
        "order": order,

        "page": page,
        "pageSize": PAGE_SIZE,

        "rangeLabel": f"{rank_start}-{rank_end}" if rank_end >= rank_start else "—",
        "rankStart": rank_start,

        "rows": page_rows,

        # Option B: all keys discovered from API scan
        "availableKeys": sorted(list(available_keys)),

        # for UI default-checks
        "defaultCols": default_cols,

        # diagnostics
        "scannedCount": scanned,
        "filteredTotalApprox": filtered_total_approx,

        # echo back what UI asked for
        "selectedCols": selected_cols,
        "filters": filters,
    })


@app.get("/random-player/play")
def random_player_play():
    mode = request.args.get("mode")
    
    for _ in range(15):
        try:
            # === NEW Active Player LOGIC ===
            if mode == "active" and ACTIVE_PLAYER_IDS:
                # Instant selection from your new file!
                pid = random.choice(ACTIVE_PLAYER_IDS)
            else:
                # Standard logic (1990-Present)
                pid = get_random_player_id("players_index.json")

            # Fetch the data
            player = get_player_full(pid)
            
            # (No need to check 'active' status here anymore—we know they are active!)

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
                hitting_groups = group_year_by_year(yby, "hitting")
                pitching_groups = group_year_by_year(yby, "pitching")

            career_hitting = get_player_career_totals(pid, "hitting") if role != "pitching" else None
            career_pitching = get_player_career_totals(pid, "pitching") if role != "hitting" else None
            
            awards = get_player_awards(pid)
            accolades = build_accolade_pills(awards)
            award_year_map = build_award_year_map(awards)

            yby_bg_hitting = {}
            yby_bg_pitching = {}

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
                title="Random Player • Basenerd",
                mode=mode
            )

        except Exception as e:
            print(f"[random-player] retry due to error: {e}")
            continue

    return "Could not fetch a random player right now.", 500




@app.get("/")
def home():
    # Pull recent articles for the homepage module (safe if none exist)
    try:
        articles = load_articles() or []
        # If your loader doesn't already sort, keep newest first when dates exist
        # (won't crash if date missing)
        def _key(a):
            return (a.get("date") or "")
        articles = sorted(articles, key=_key, reverse=True)
    except Exception as e:
        print(f"[home] load_articles failed: {e}")
        articles = []

    return render_template("home.html", title="Basenerd", articles=articles)


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
    user_tz = "America/Phoenix"
    print(f"[TEAM ROUTE HIT] team_id={team_id}")

    # -------------------------
    # Team metadata
    # -------------------------
    try:
        data = get_team(team_id) or {}
        teams = data.get("teams") or []
        raw = teams[0] if teams else {}
    except Exception as e:
        print(f"[team] get_team failed team_id={team_id}: {e}")
        raw = {}

    team_obj = {
        "team_id": raw.get("id"),
        "name": raw.get("name"),
        "abbrev": raw.get("abbreviation"),
        "location": raw.get("locationName"),
        "first_year": raw.get("firstYearOfPlay"),
        "league": (raw.get("league") or {}).get("name"),
        "division": (raw.get("division") or {}).get("name"),
        "division_id": (raw.get("division") or {}).get("id"),
        "venue": (raw.get("venue") or {}).get("name"),
        "logo_url": f"https://www.mlbstatic.com/team-logos/{raw.get('id')}.svg" if raw.get("id") else None,
    }
    print("[team] team_obj:", team_obj)

    # If team lookup failed, bail early with a clear message
    if not team_obj.get("team_id"):
        return render_template(
            "team.html",
            title="Team",
            team={"team_id": team_id, "name": "Team Not Found", "logo_url": None},
            roster_grouped={},
            roster_other=[],
            last_game=None,
            next_game=None,
            division_rows=[],
            leaders=None,
            error=f"Team id {team_id} not found from StatsAPI.",
        )

    # -------------------------
    # Roster (safe defaults)
    # -------------------------
    roster_grouped = {}
    roster_other = []  # MUST be list for template loops
    try:
        roster_grouped, roster_other = get_40man_roster_grouped(team_id)
        roster_grouped = roster_grouped or {}
        roster_other = roster_other or []
    except Exception as e:
        print(f"[team] roster fetch failed team_id={team_id}: {e}")
        roster_grouped, roster_other = {}, []

    print(
        "[team] roster sizes:",
        {k: len(v or []) for k, v in (roster_grouped or {}).items()},
        "other:",
        len(roster_other or []),
    )

    # -------------------------
    # Schedule helpers
    # -------------------------
    def _flatten_team_schedule(team_id: int, season: int):
        sched = get_team_schedule(team_id, season) or {}
        games = []
        for d in (sched.get("dates") or []):
            for g in (d.get("games") or []):
                games.append(g)
        return games

    def _parse_dt(iso):
        try:
            # StatsAPI uses Z; normalize to +00:00
            return datetime.fromisoformat(iso.replace("Z", "+00:00"))
        except Exception:
            return None

    def _is_final_game(g):
        st = (g.get("status") or {})
        ds = (st.get("detailedState") or "").lower()
        code = (st.get("statusCode") or "").upper()
        return (code in ("F", "O")) or ("final" in ds) or ("game over" in ds)

    def _is_played(g):
        st = (g.get("status") or {})
        ds = (st.get("detailedState") or "").lower()
        code = (st.get("statusCode") or "").upper()
        if code in ("F", "O"):
            return True
        if "in progress" in ds or "live" in ds or "completed" in ds:
            return True
        if "final" in ds or "game over" in ds:
            return True
        return False

    # -------------------------
    # Decide "last played season" for last_game (fallback)
    # -------------------------
    cur_year = datetime.utcnow().year
    games_cur = _flatten_team_schedule(team_id, cur_year)
    reg_cur = [g for g in games_cur if (g.get("gameType") or "").upper() == "R"]

    season = cur_year if (reg_cur and any(_is_played(g) for g in reg_cur)) else (cur_year - 1)
    print("[team] season chosen:", season, "games_cur:", len(games_cur), "reg_cur:", len(reg_cur))

    games_season = _flatten_team_schedule(team_id, season)
    print("[team] games_season:", len(games_season))

    # -------------------------
    # Build game cards
    # -------------------------
    def _record_str(side_obj):
        rec = (side_obj or {}).get("leagueRecord") or {}
        w = rec.get("wins")
        l = rec.get("losses")
        if w is None or l is None:
            return None
        return f"{w}-{l}"

    def _pp_simple(pp_obj):
        if not pp_obj:
            return "PP: TBD"
        nm = pp_obj.get("fullName")
        return f"PP: {nm}" if nm else "PP: TBD"

    def _game_card_from_sched(g):
        teams = g.get("teams") or {}
        home = teams.get("home") or {}
        away = teams.get("away") or {}
        home_team = (home.get("team") or {})
        away_team = (away.get("team") or {})
        venue = g.get("venue") or {}

        # Prefer your normalize_schedule_game for consistent pills/time labels
        try:
            sg = get_schedule_game_by_pk(g.get("gamePk"))
            ng = normalize_schedule_game(sg, tz_name=user_tz)
            time_local = ng.get("timeLocal") or ""
            status_pill = ng.get("statusPill") or ((g.get("status") or {}).get("detailedState") or "")
            detailed_state = ng.get("detailedState") or ((g.get("status") or {}).get("detailedState") or "")
            competition_label = ng.get("competitionLabel") or ""
        except Exception:
            time_local = ""
            status_pill = (g.get("status") or {}).get("detailedState") or ""
            detailed_state = (g.get("status") or {}).get("detailedState") or ""
            competition_label = ""

        prob = g.get("probablePitchers") or {}
        dec = g.get("decisions") or {}

        v_city = venue.get("city")
        v_state = venue.get("state") or venue.get("stateAbbrev")
        venue_loc = f"{v_city}, {v_state}" if (v_city and v_state) else (v_city or "")

        return {
            "gamePk": g.get("gamePk"),
            "competitionLabel": competition_label,
            "statusPill": status_pill,
            "detailedState": detailed_state,
            "timeLocal": time_local,
            "venue": venue.get("name"),
            "venueLocation": venue_loc,
            "away": {
                "abbrev": (away_team.get("abbreviation") or "").upper(),
                "logo": f"https://www.mlbstatic.com/team-logos/{away_team.get('id')}.svg" if away_team.get("id") else None,
                "record": _record_str(away),
                "pp": _pp_simple(prob.get("away") or {}),
                "score": away.get("score"),
            },
            "home": {
                "abbrev": (home_team.get("abbreviation") or "").upper(),
                "logo": f"https://www.mlbstatic.com/team-logos/{home_team.get('id')}.svg" if home_team.get("id") else None,
                "record": _record_str(home),
                "pp": _pp_simple(prob.get("home") or {}),
                "score": home.get("score"),
            },
            "pitching": {
                "winner": (dec.get("winner") or {}).get("fullName"),
                "loser": (dec.get("loser") or {}).get("fullName"),
            }
        }

    # -------------------------
    # Last Game (from chosen "last played season")
    # -------------------------
    finals = [g for g in games_season if _is_final_game(g)]
    finals.sort(key=lambda x: _parse_dt(x.get("gameDate") or "") or datetime.min)
    last_game = _game_card_from_sched(finals[-1]) if finals else None

    # -------------------------
    # Next Game (IMPORTANT FIX): always from current year schedule
    # -------------------------
    now_utc = datetime.utcnow().replace(tzinfo=None)
    games_next = _flatten_team_schedule(team_id, cur_year)

    upcoming = []
    for g in games_next:
        dt = _parse_dt(g.get("gameDate") or "")
        if not dt:
            continue
        dt_naive = dt.replace(tzinfo=None)
        if dt_naive >= now_utc and not _is_final_game(g):
            upcoming.append(g)

    upcoming.sort(key=lambda x: _parse_dt(x.get("gameDate") or "") or datetime.max)
    next_game = _game_card_from_sched(upcoming[0]) if upcoming else None

    print("[team] last_game:", bool(last_game), "next_game:", bool(next_game))

    # -------------------------
    # Division standings (IMPORTANT FIX): correct standings JSON shape
    # -------------------------
    division_rows = []
    try:
        standings = get_standings(season) or {}
        div_id = team_obj.get("division_id")

        for rec in (standings.get("records") or []):
            div = rec.get("division") or {}
            if div_id and div.get("id") != div_id:
                continue

            teamrecs = rec.get("teamRecords") or []

            def _winpct(tr):
                try:
                    return float((tr.get("winningPercentage") or "0").strip())
                except Exception:
                    return 0.0

            for tr in sorted(teamrecs, key=_winpct, reverse=True):
                t = tr.get("team") or {}
                lr = tr.get("leagueRecord") or {}
                division_rows.append({
                    "abbrev": (t.get("abbreviation") or "").upper(),
                    "logo": f"https://www.mlbstatic.com/team-logos/{t.get('id')}.svg" if t.get("id") else None,
                    "w": lr.get("wins"),
                    "l": lr.get("losses"),
                    "pct": tr.get("winningPercentage"),
                    "gb": tr.get("gamesBack"),
                    "is_selected": (t.get("id") == team_id),
                })

            break  # found division
    except Exception as e:
        print(f"[team] standings failed: {e}")

    print("[team] division_rows:", len(division_rows))

    # -------------------------
    # Leaders
    # -------------------------
    headshot_fallback = (
        "data:image/svg+xml;utf8,"
        "<svg xmlns='http://www.w3.org/2000/svg' width='64' height='64' viewBox='0 0 64 64'>"
        "<rect width='64' height='64' rx='32' fill='%23222'/>"
        "<circle cx='32' cy='26' r='12' fill='%23555'/>"
        "<path d='M14 60c3-14 14-20 18-20s15 6 18 20' fill='%23555'/>"
        "</svg>"
    )

    def _top_leader(group, stat_key, label, order="desc", pool="ALL"):
        try:
            out = get_stats_leaderboard(
                group=group,
                season=season,
                sort_stat=stat_key,
                order=order,
                team_id=team_id,
                player_pool=pool,
                limit=1,
                offset=0,
                game_type="R",
            )
    
            rows = (out or {}).get("rows") or []
            if not rows:
                return None
    
            top = rows[0]
    
            # ✅ YOUR normalized keys from mlb_api.py:
            pid = top.get("playerId")
            pname = top.get("name") or "—"
            statline = top.get("stat") or {}
    
            # value comes from the stat dict (keys vary by stat)
            val = statline.get(stat_key)
            if val is None:
                val = statline.get((stat_key or "").lower())
            if val is None:
                val = "—"
    
            return {
                "label": label,
                "player_id": pid,
                "player_name": pname,
                "value": val,
                "headshot": top.get("headshot"),
                "fallback": headshot_fallback,
            }
    
        except Exception as e:
            print(f"[team] leader failed {group} {stat_key}: {e}")
            return None



    leaders_hit, leaders_pit = [], []

    for spec in [
        ("hitting", "avg", "AVG", "desc", "QUALIFIED"),
        ("hitting", "homeRuns", "HR", "desc", "ALL"),
        ("hitting", "rbi", "RBI", "desc", "ALL"),
        ("hitting", "stolenBases", "SB", "desc", "ALL"),
    ]:
        row = _top_leader(*spec)
        if row:
            leaders_hit.append(row)

    for spec in [
        ("pitching", "inningsPitched", "IP", "desc", "ALL"),
        ("pitching", "era", "ERA", "asc", "QUALIFIED"),
        ("pitching", "strikeOuts", "K", "desc", "ALL"),
        ("pitching", "whip", "WHIP", "asc", "QUALIFIED"),
    ]:
        row = _top_leader(*spec)
        if row:
            leaders_pit.append(row)

    leaders = {"hitting": leaders_hit, "pitching": leaders_pit} if (leaders_hit or leaders_pit) else None
    print("[team] leaders present:", bool(leaders))

    # -------------------------
    # Render
    # -------------------------
    print("[team] context summary:",
          "team?", bool(team_obj),
          "last_game?", bool(last_game),
          "next_game?", bool(next_game),
          "division_rows", len(division_rows),
          "leaders?", bool(leaders),
          "roster_grouped_keys", list((roster_grouped or {}).keys()))

    return render_template(
        "team.html",
        title=team_obj.get("name", "Team"),
        team=team_obj,
        roster_grouped=roster_grouped,
        roster_other=roster_other,
        last_game=last_game,
        next_game=next_game,
        division_rows=division_rows,
        leaders=leaders,
    )


from datetime import timedelta

@app.get("/games")
def games():
    """
    Shows games for a chosen date.
    - Default: today
    - If no games today: auto-advance to next future date with games
    """
    # later: make this per-user; for now default to Phoenix (your requirement)
    user_tz = "America/Phoenix"

    picked = (request.args.get("date") or "").strip()  # YYYY-MM-DD
    today_ymd = datetime.utcnow().date().strftime("%Y-%m-%d")

    target = picked or today_ymd
    games_list = get_games_for_date(target, tz_name=user_tz)

    auto_advanced = False
    if not picked and not games_list:
        try:
            nxt = find_next_date_with_games(today_ymd, max_days_ahead=120)
        except Exception:
            nxt = None
        
        if nxt:
            target = nxt
            games_list = get_games_for_date(target, tz_name=user_tz)
            auto_advanced = True
            
    return render_template(
        "games.html",
        title="Games",
        date=target,
        games=games_list,
        auto_advanced=auto_advanced,
        user_tz=user_tz,
    )
@app.get("/game/<int:game_pk>")
def game_detail(game_pk: int):
    user_tz = "America/Phoenix"

    feed = get_game_feed(game_pk)
    game_obj = normalize_game_detail(feed, tz_name=user_tz)

    # ✅ always available for template debugging / links
    if isinstance(game_obj, dict):
        game_obj.setdefault("gamePk", game_pk)

    # -------------------------
    # Stadium SVG (venue-based, fallback to home team park)
    # -------------------------
    stadium_svg = "generic.svg"
    try:
        venue_name = (((feed or {}).get("gameData") or {}).get("venue") or {}).get("name")
        candidate = _VENUE_TO_SVG.get(_norm_venue_name(venue_name or ""))
        if candidate and os.path.exists(os.path.join("static", "stadium_svgs", candidate)):
            stadium_svg = candidate
        else:
            # fallback: home team nickname
            home_id = (((feed or {}).get("gameData") or {}).get("teams") or {}).get("home", {}).get("id")
            if home_id:
                tdata = get_team(home_id) or {}
                teams = tdata.get("teams") or []
                t0 = teams[0] if teams else {}
                team_name = t0.get("teamName")  # nickname only
                if team_name:
                    candidate2 = teamname_to_svg(team_name)
                    if os.path.exists(os.path.join("static", "stadium_svgs", candidate2)):
                        stadium_svg = candidate2
    except Exception as e:
        print("stadium svg lookup failed:", e)

    return render_template("game.html", title="Game", game=game_obj, user_tz=user_tz, stadium_svg=stadium_svg)

from flask import jsonify

@app.get("/game/<int:game_pk>/gamecast.json")
def gamecast_json(game_pk: int):
    """
    Lightweight JSON endpoint for the GameCast tab.
    Polled by the browser every ~3s ONLY when GameCast tab is active.

    Returns:
      - game state: inning/half/balls/strikes/outs, score, lastPlay
      - runners on base
      - current batter/pitcher + their game stat lines + headshots
      - current PA pitches (for zone plot + pitches table) incl. outcome flags
      - away/home live lineups (includes substitutions as they appear) incl. headshots + per-PA log
      - defense positions (best-effort from boxscore)
    """
    feed = get_game_feed(game_pk) or {}
    game_data = (feed.get("gameData") or {})
    live_data = (feed.get("liveData") or {})
    linescore = (live_data.get("linescore") or {})
    boxscore = (live_data.get("boxscore") or {})
    plays = (live_data.get("plays") or {})
    all_plays = plays.get("allPlays") or []
    current_play = plays.get("currentPlay") or (all_plays[-1] if all_plays else {})

    # -------------------------
    # Helpers
    # -------------------------
    def _safe(d, *keys, default=None):
        cur = d
        for k in keys:
            if not isinstance(cur, dict):
                return default
            cur = cur.get(k)
        return default if cur is None else cur

    def _i(x, default=None):
        try:
            if x is None or x == "":
                return default
            return int(x)
        except Exception:
            try:
                return int(float(x))
            except Exception:
                return default

    def _f(x, default=None):
        try:
            if x is None or x == "":
                return default
            return float(x)
        except Exception:
            return default

    def _half_norm(s):
        sl = (s or "").lower().strip()
        if sl == "top":
            return "Top"
        if sl == "bottom":
            return "Bottom"
        return s or ""

    def _ordinal(n: int) -> str:
        try:
            n = int(n)
        except Exception:
            return ""
        if 10 <= (n % 100) <= 20:
            suf = "th"
        else:
            suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
        return f"{n}{suf}"

    def _last_name(full: str) -> str:
        s = (full or "").strip()
        if not s:
            return ""
        parts = s.split()
        return parts[-1] if parts else s

    def _short_name(full: str) -> str:
        s = (full or "").strip()
        if not s:
            return ""
        parts = s.split()
        if len(parts) == 1:
            return s
        return f"{parts[0][0]}. {parts[-1]}"

    def _player_map(side_key: str):
        team = _safe(boxscore, "teams", side_key, default={}) or {}
        return (team.get("players") or {})

    def _find_player_stats(pid: int, side_key: str):
        """
        Returns dict: batting stats + pitching stats + name/pos if found
        """
        players = _player_map(side_key)
        key1 = f"ID{pid}"
        pdata = players.get(key1)
        if not pdata:
            # fallback scan (rare)
            for _, p in players.items():
                if _i(_safe(p, "person", "id"), None) == pid:
                    pdata = p
                    break
        if not pdata:
            return {}

        person = pdata.get("person") or {}
        pos = _safe(pdata, "position", "abbreviation", default=None)
        bat = _safe(pdata, "stats", "batting", default={}) or {}
        pit = _safe(pdata, "stats", "pitching", default={}) or {}
        return {
            "id": _i(person.get("id"), None),
            "name": (person.get("fullName") or "").strip(),
            "pos": pos,
            "batting": bat,
            "pitching": pit,
        }

    def _batter_line(bat: dict):
        # AB-H, HR, RBI, BB, K
        if not isinstance(bat, dict) or not bat:
            return ""
        ab = bat.get("atBats")
        h = bat.get("hits")
        hr = bat.get("homeRuns")
        rbi = bat.get("rbi")
        bb = bat.get("baseOnBalls")
        k = bat.get("strikeOuts")
        if all(v in (None, 0, "0", "") for v in [ab, h, hr, rbi, bb, k]):
            return ""
        return f"{ab}-{h} • HR {hr} • RBI {rbi} • BB {bb} • K {k}"

    def _pitcher_line(pit: dict):
        # IP, H, R, ER, BB, K
        if not isinstance(pit, dict) or not pit:
            return ""
        ip = pit.get("inningsPitched")
        h = pit.get("hits")
        r = pit.get("runs")
        er = pit.get("earnedRuns")
        bb = pit.get("baseOnBalls")
        k = pit.get("strikeOuts")
        if all(v in (None, 0, "0", "") for v in [ip, h, r, er, bb, k]):
            return ""
        return f"IP {ip} • H {h} • R {r} • ER {er} • BB {bb} • K {k}"

    # Build per-player PA logs from allPlays (best-effort)
    pa_log = {}
    for p in (all_plays or []):
        matchup = (p.get("matchup") or {})
        batter = (matchup.get("batter") or {})
        pid = _i(batter.get("id"), None)
        if not pid:
            continue
        about = (p.get("about") or {})
        inn = _i(about.get("inning"), None)
        desc = (_safe(p, "result", "description", default="") or "").strip()
        if not desc:
            continue
        key = _ordinal(inn) if inn else ""
        pa_log.setdefault(pid, []).append({"inning": key, "desc": desc})

    def _extract_lineup(side_key: str):
        """
        Build live lineup list from boxscore players:
          - includes started/subbed/entered players (subs appear)
          - sorts by battingOrder when present
        """
        players = _player_map(side_key)
        out = []
        for _, pdata in (players or {}).items():
            person = pdata.get("person") or {}
            pid = _i(person.get("id"), None)
            name = (person.get("fullName") or "").strip()
            pos = _safe(pdata, "position", "abbreviation", default=None)
            bat = _safe(pdata, "stats", "batting", default={}) or {}

            gs = pdata.get("gameStatus")
            gs_str = ""
            if isinstance(gs, dict):
                gs_str = (gs.get("status") or gs.get("detailedState") or gs.get("code") or "")
            else:
                gs_str = gs or ""
            appeared = str(gs_str).lower() in ("started", "substituted", "entered")
            has_bat = any((bat.get(k) not in (None, "", 0, "0")) for k in ("atBats", "hits", "runs", "rbi", "baseOnBalls", "strikeOuts", "homeRuns"))
            has_pa = bool(pid and pa_log.get(pid))
            if not (appeared or has_bat or has_pa):
                continue

            bo_raw = pdata.get("battingOrder")
            bo = None
            try:
                bo = int(str(bo_raw)) if bo_raw not in (None, "") else None
            except Exception:
                bo = None

            out.append({
                "id": pid,
                "name": name,
                "lastName": _last_name(name),
                "shortName": _short_name(name),
                "headshot": get_player_headshot_url(pid, 84) if pid else "",
                "pos": pos,
                "batLine": _batter_line(bat),
                "battingOrder": bo,
                "pas": pa_log.get(pid, []),
            })

        out.sort(key=lambda r: (r["battingOrder"] is None, r["battingOrder"] or 999999, r["name"] or ""))
        for r in out:
            if isinstance(r.get("battingOrder"), int):
                r["spot"] = max(1, r["battingOrder"] // 100)
            else:
                r["spot"] = None
        return out

    def _extract_defense_positions(def_side_key: str):
        """
        Best-effort defense map from boxscore positions.
        Returns dict like {"P": {...}, "C": {...}, "1B": {...}, ...}
        """
        players = _player_map(def_side_key)
        wanted = ["P", "C", "1B", "2B", "3B", "SS", "LF", "CF", "RF"]
        found = {k: None for k in wanted}
        for _, pdata in (players or {}).items():
            pos = _safe(pdata, "position", "abbreviation", default=None)
            if pos not in found:
                continue
            person = pdata.get("person") or {}
            pid = _i(person.get("id"), None)
            name = (person.get("fullName") or "").strip()
            if not found[pos] and pid and name:
                found[pos] = {"id": pid, "name": name, "lastName": _last_name(name)}
        return found

    # -------------------------
    # Current situation
    # -------------------------
    status = _safe(game_data, "status", "detailedState", default="") or ""
    count = (current_play.get("count") or {})
    about = (current_play.get("about") or {})
    matchup = (current_play.get("matchup") or {})

    inning = _i(about.get("inning"), None)
    half = _half_norm(about.get("halfInning"))
    balls = _i(count.get("balls"), 0) or 0
    strikes = _i(count.get("strikes"), 0) or 0
    outs = _i(count.get("outs"), 0) or 0

    away_runs = _i(_safe(linescore, "teams", "away", "runs"), 0) or 0
    home_runs = _i(_safe(linescore, "teams", "home", "runs"), 0) or 0

    last_play = (_safe(current_play, "result", "description", default="") or "").strip()
    if not last_play and all_plays:
        last_play = (_safe(all_plays[-1], "result", "description", default="") or "").strip()

    # Runners on base
    offense = (linescore.get("offense") or {})

    def _runner_obj(base_key):
        o = offense.get(base_key)
        if not isinstance(o, dict):
            return None
        pid = _i(_safe(o, "id"), None) or _i(_safe(o, "person", "id"), None)
        nm = (_safe(o, "fullName", default=None) or _safe(o, "person", "fullName", default=None) or "").strip()
        if not pid and not nm:
            return None
        return {"id": pid, "name": nm, "lastName": _last_name(nm)}

    runners = {
        "first": _runner_obj("first"),
        "second": _runner_obj("second"),
        "third": _runner_obj("third"),
    }

    # Batter / Pitcher
    batter_id = _i(_safe(matchup, "batter", "id"), None)
    pitcher_id = _i(_safe(matchup, "pitcher", "id"), None)
    batter_name = (_safe(matchup, "batter", "fullName", default="") or "").strip()
    pitcher_name = (_safe(matchup, "pitcher", "fullName", default="") or "").strip()

    # Batter is on offense; pitcher on defense. Determine offense by half-inning.
    offense_side = "away" if (half.lower().startswith("top")) else "home"
    defense_side = "home" if offense_side == "away" else "away"

    batter_stats = _find_player_stats(batter_id, offense_side) if batter_id else {}
    pitcher_stats = _find_player_stats(pitcher_id, defense_side) if pitcher_id else {}

    batter_line = _batter_line((batter_stats.get("batting") or {}))
    pitcher_line = _pitcher_line((pitcher_stats.get("pitching") or {}))

    # -------------------------
    # Current PA pitch list (for zone + pitches table) incl outcome flags
    # -------------------------
    pitches_out = []
    for ev in (current_play.get("playEvents") or []):
        if not ev.get("isPitch"):
            continue

        details = ev.get("details") or {}
        pitch_data = ev.get("pitchData") or {}
        coords = (pitch_data.get("coordinates") or {})
        breaks = (pitch_data.get("breaks") or {})

        pitch_type = (_safe(details, "type", "description", default=None) or "").strip()
        mph = _f(pitch_data.get("startSpeed"), None)
        spin = _i(_safe(pitch_data, "breaks", "spinRate"), None) or _i(pitch_data.get("spinRate"), None)

        px = _f(coords.get("pX"), None)
        pz = _f(coords.get("pZ"), None)
        sz_top = _f(pitch_data.get("strikeZoneTop"), None)
        sz_bot = _f(pitch_data.get("strikeZoneBottom"), None)

        call = (details.get("call") or {})
        code = (call.get("code") or "").upper()
        ddesc = (details.get("description") or "").lower()

        is_ball = bool(details.get("isBall")) or (code == "B")
        is_strike = bool(details.get("isStrike")) or (code in ("C", "S"))
        is_in_play = bool(details.get("isInPlay")) or (code == "X") or ("in play" in ddesc) or ("hit into play" in ddesc)
        is_foul = bool(details.get("isFoul")) or (code == "F") or ("foul" in ddesc and not is_in_play)

        pitches_out.append({
            "n": _i(details.get("pitchNumber"), None) or (len(pitches_out) + 1),
            "pitchType": pitch_type or (_safe(details, "type", "code", default="") or "").upper(),
            "mph": mph,
            "spinRate": spin,
            "vertMove": _f(breaks.get("breakVerticalInduced"), None),
            "horizMove": _f(breaks.get("breakHorizontal"), None),

            # for zone renderer:
            "px": px,
            "pz": pz,
            "sz_top": sz_top,
            "sz_bot": sz_bot,

            # outcome flags for coloring (match game.html renderZone)
            "isBall": is_ball,
            "isStrike": is_strike,
            "isFoul": is_foul,
            "isInPlay": is_in_play,
        })

    # -------------------------
    # Lineups (live, includes subs)
    # -------------------------
    away_lineup = _extract_lineup("away")
    home_lineup = _extract_lineup("home")

    # Defense positions shown on field: defense side only
    defense_positions = _extract_defense_positions(defense_side)

    return jsonify({
        "ok": True,
        "gamePk": game_pk,
        "status": status,
        "inning": inning,
        "half": half,
        "balls": balls,
        "strikes": strikes,
        "outs": outs,
        "score": {"away": away_runs, "home": home_runs},
        "lastPlay": last_play,

        "batter": {
            "id": batter_id,
            "name": batter_stats.get("name") or batter_name,
            "pos": batter_stats.get("pos"),
            "headshot": get_player_headshot_url(batter_id, 120) if batter_id else "",
            "line": batter_line,
        },
        "pitcher": {
            "id": pitcher_id,
            "name": pitcher_stats.get("name") or pitcher_name,
            "pos": pitcher_stats.get("pos"),
            "headshot": get_player_headshot_url(pitcher_id, 120) if pitcher_id else "",
            "line": pitcher_line,
        },

        "runners": runners,
        "pitches": pitches_out,

        "lineups": {"away": away_lineup, "home": home_lineup},
        "defenseSide": defense_side,
        "defense": defense_positions,
    })



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
    season = datetime.now().year

    letter = (request.args.get("letter") or "A").upper()
    letters = [chr(c) for c in range(ord("A"), ord("Z")+1)] + ["#"]
    if letter not in letters:
        letter = "A"

    q = (request.args.get("q") or "").strip()

    if q:
        matches = suggest_40man_players(q, season=season, limit=200)
        directory = get_40man_directory(season=season)
        by_id = {p["id"]: p for p in directory}
        players = [by_id.get(m["id"]) for m in matches if by_id.get(m["id"])]
    else:
        players = filter_40man_by_letter(letter, season=season)

    alpha = [chr(c) for c in range(ord("A"), ord("Z")+1)]
    if letter == "#":
        prev_letter = "Z"
        next_letter = "A"
    else:
        i = alpha.index(letter)
        prev_letter = alpha[i-1] if i > 0 else "Z"
        next_letter = alpha[i+1] if i < len(alpha)-1 else "A"

    return render_template(
        "players.html",
        q=q,
        letter=letter,
        letters=letters,
        prev_letter=prev_letter,
        next_letter=next_letter,
        players=players,
    )
    
@app.get("/players/suggest")
def players_suggest():
    season = datetime.now().year
    q = (request.args.get("q") or "").strip()
    return jsonify(suggest_40man_players(q, season=season, limit=10))
    
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
        # -------------------------
    # Stadium SVG (player current team)
    # Your filenames are nickname-only: angels.svg, blue_jays.svg, etc.
    # So we use team["teamName"] from the team endpoint.
    # -------------------------
    stadium_svg = "generic.svg"

    try:
        team_id = (bio.get("currentTeam") or {}).get("id")
        if team_id:
            tdata = get_team(team_id) or {}
            teams = tdata.get("teams") or []
            t0 = teams[0] if teams else {}
            team_name = t0.get("teamName")  # nickname only (Angels, Marlins, Blue Jays, etc)
    
            if team_name:
                candidate = teamname_to_svg(team_name)
                if os.path.exists(os.path.join("static", "stadium_svgs", candidate)):
                    stadium_svg = candidate
    except Exception as e:
        print("stadium svg lookup failed:", e)
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

        # Even if not qualified, we still color COUNTING stats using the qualified pool distribution.
        # Only rate stats are forced to gray when not qualified.

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
    savant_profile = {"available": True, "season": 2025, "groups": []}
    print("DEBUG: building savant_profile", "player_id=", int(bio["id"]), "season=", season_found)
    season_for_scouting = int(season_found or log_year or 2025)
    player_id = int(bio["id"])  # or int(bio["id"]) depending on your bio object
    
    savant_profile = get_player_savant_profile(player_id, season_for_scouting, min_pa=1)
    
    print("DEBUG: savant_profile available =", savant_profile.get("available"), "groups =", len(savant_profile.get("groups", [])))
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
        # savant profile
        savant_profile=savant_profile,
        stadium_svg=stadium_svg,
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
