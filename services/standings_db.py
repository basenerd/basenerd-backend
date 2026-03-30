# services/standings_db.py
from __future__ import annotations
import os
import urllib.request
import json
import psycopg


def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def fetch_standings_ranked(season: int) -> list[dict]:
    sql = """
    SELECT
      s.season,
      s.league,
      s.division,
      s.team_id,
      s.team_abbrev,
      s.team_name,
      s.w,
      s.l,
      s.pct,
      s.gb,
      s.wc_gb,
      s.rs,
      s.ra,
      (s.rs - s.ra) AS run_differential,
      s.streak,
      s.division_rank,
      s.wild_card_rank,
      s.last_updated
    FROM standings s
    WHERE s.season = %s
    ORDER BY s.league, s.division, s.division_rank NULLS LAST, s.pct DESC;
    """

    with psycopg.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (season,))
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def _as_int(x, default=None):
    if x is None:
        return default
    try:
        return int(x)
    except Exception:
        return default


def _as_float(x, default=None):
    if x is None:
        return default
    try:
        return float(x)
    except Exception:
        return default


def build_divs(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    by_league_div: dict[tuple[str, str], list[dict]] = {}
    for r in rows:
        league = (r.get("league") or "").strip()
        division = (r.get("division") or "").strip()
        by_league_div.setdefault((league, division), []).append(r)

    al_divs: list[dict] = []
    nl_divs: list[dict] = []

    for (league, division), teams in by_league_div.items():
        def sort_key(x):
            dr = _as_int(x.get("division_rank"), 999)
            pct = _as_float(x.get("pct"), 0.0)
            return (dr, -pct)

        teams_sorted = sorted(teams, key=sort_key)

        mapped: list[dict] = []
        for t in teams_sorted:
            team_id = _as_int(t.get("team_id"), 0)

            division_rank = _as_int(t.get("division_rank"), None)
            wild_card_rank = _as_int(t.get("wild_card_rank"), None)

            is_winner = (division_rank == 1)
            is_wc = (wild_card_rank in (1, 2, 3)) and not is_winner

            mapped.append(
                {
                    "team_id": team_id,
                    "abbrev": t.get("team_abbrev") or "",
                    "w": _as_int(t.get("w"), None),
                    "l": _as_int(t.get("l"), None),

                    # IMPORTANT: keep numeric for gradient logic in template
                    "pct": _as_float(t.get("pct"), None),

                    "gb": t.get("gb") or "—",
                    "wc_gb": t.get("wc_gb") or "—",
                    "streak": t.get("streak") or "—",
                    "run_diff": _as_int(t.get("run_differential"), None),
                    "logo_url": f"https://www.mlbstatic.com/team-logos/{team_id}.svg",

                    # IMPORTANT: expose ranks to the template
                    "division_rank": division_rank,
                    "wild_card_rank": wild_card_rank,

                    # convenience flags for template (ints)
                    "division_leader": 1 if is_winner else 0,
                    "wild_card": 1 if is_wc else 0,
                }
            )

        div_obj = {"name": division, "teams": mapped}

        if league == "American League":
            al_divs.append(div_obj)
        elif league == "National League":
            nl_divs.append(div_obj)

    def div_sort_key(d: dict) -> int:
        order = {
            "American League East": 1,
            "American League Central": 2,
            "American League West": 3,
            "National League East": 1,
            "National League Central": 2,
            "National League West": 3,
        }
        return order.get(d.get("name", ""), 99)

    al_divs.sort(key=div_sort_key)
    nl_divs.sort(key=div_sort_key)

    return al_divs, nl_divs


def fetch_live_standings(season: int) -> list[dict]:
    """Fetch regular-season standings live from MLB Stats API.

    Returns rows in the same shape as fetch_standings_ranked() so callers
    (build_divs, _pick_playoff, _build_wc) need zero changes.
    """
    url = (
        f"https://statsapi.mlb.com/api/v1/standings"
        f"?leagueId=103,104&season={season}&standingsTypes=regularSeason"
        f"&hydrate=team(division,league)"
    )
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read())

    rows: list[dict] = []
    for div_block in data.get("records", []):
        raw_div = (div_block.get("division") or {}).get("name", "")
        raw_lg  = (div_block.get("league")   or {}).get("name", "")
        for tr in div_block.get("teamRecords", []):
            team = tr.get("team") or {}
            tid = team.get("id")
            if not tid:
                continue
            # Prefer team-level hydrated names (record-level often lacks name)
            division = (team.get("division") or {}).get("name", "") or raw_div
            league   = (team.get("league")   or {}).get("name", "") or raw_lg
            w = _as_int(tr.get("wins"), 0)
            l = _as_int(tr.get("losses"), 0)
            rs = _as_int(tr.get("runsScored"), 0)
            ra = _as_int(tr.get("runsAllowed"), 0)
            streak_obj = tr.get("streak") or {}
            rows.append({
                "season": season,
                "league": league,
                "division": division,
                "team_id": tid,
                "team_abbrev": team.get("abbreviation", ""),
                "team_name": team.get("name", ""),
                "w": w,
                "l": l,
                "pct": _as_float(tr.get("winningPercentage"), None),
                "gb": tr.get("gamesBack", "—"),
                "wc_gb": tr.get("wildCardGamesBack", "—"),
                "rs": rs,
                "ra": ra,
                "run_differential": rs - ra if rs is not None and ra is not None else None,
                "streak": streak_obj.get("streakCode", "—"),
                "division_rank": _as_int(tr.get("divisionRank"), None),
                "wild_card_rank": _as_int(tr.get("wildCardRank"), None),
            })
    return rows


def fetch_spring_standings(season: int) -> tuple[list[dict], list[dict]]:
    """Fetch spring training standings from MLB Stats API.
    Returns (cactus_teams, grapefruit_teams) sorted by springLeagueRank.
    """
    url = (
        f"https://statsapi.mlb.com/api/v1/standings"
        f"?leagueId=114,115&season={season}&standingsTypes=springTraining"
    )
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())

    def _parse_record(rec):
        teams = []
        for tr in rec.get("teamRecords", []):
            team = tr.get("team", {})
            tid = team.get("id", 0)
            streak_obj = tr.get("streak", {})
            lr = tr.get("leagueRecord", {})
            teams.append({
                "team_id": tid,
                "abbrev": team.get("abbreviation", ""),
                "team_name": team.get("name", ""),
                "w": tr.get("wins", 0),
                "l": tr.get("losses", 0),
                "t": lr.get("ties", 0),
                "pct": _as_float(tr.get("winningPercentage"), None),
                "gb": tr.get("gamesBack", "—"),
                "streak": streak_obj.get("streakCode", "—"),
                "run_diff": tr.get("runDifferential", 0),
                "rs": tr.get("runsScored", 0),
                "ra": tr.get("runsAllowed", 0),
                "rank": _as_int(tr.get("springLeagueRank"), 99),
                "logo_url": f"https://www.mlbstatic.com/team-logos/{tid}.svg",
            })
        teams.sort(key=lambda x: x["rank"])
        return teams

    records = data.get("records", [])

    # API returns Cactus League (leagueId=114) first, Grapefruit (115) second.
    # Identify by checking for known Cactus teams (AZ=109, SF=137, LAD=119).
    cactus_ids = {109, 137, 119}
    cactus, grapefruit = [], []
    for rec in records:
        team_ids = {tr.get("team", {}).get("id", 0) for tr in rec.get("teamRecords", [])}
        if team_ids & cactus_ids:
            cactus = _parse_record(rec)
        else:
            grapefruit = _parse_record(rec)

    return cactus, grapefruit
