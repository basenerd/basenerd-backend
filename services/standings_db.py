# services/standings_db.py
import os
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
      ssf.league,
      ssf.division,
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

      -- NEW (must exist in DB)
      s.division_rank,
      s.wild_card_rank,

      s.last_updated
    FROM standings s
    JOIN standings_season_final ssf
      ON ssf.team_id = s.team_id
     AND ssf.season = s.season
    WHERE s.season = %s
    ORDER BY ssf.league, ssf.division, s.division_rank NULLS LAST, s.pct DESC;
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
