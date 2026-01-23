# services/standings_db.py
import os
import psycopg

def _db_url():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def fetch_standings_ranked(season: int) -> list[dict]:
    sql = """
    WITH cte AS (
      SELECT
        s.season,
        ssf.league AS league,
        ssf.division AS division,
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
        CASE WHEN s.gb = '-' AND s.wc_gb = '-' THEN 1 ELSE 0 END AS division_leader,
        CASE WHEN s.wc_gb LIKE '%%+%%' OR (s.gb != '-' AND s.wc_gb = '-') THEN 1 ELSE 0 END AS wild_card,
        s.last_updated
      FROM standings s
      JOIN standings_season_final ssf
        ON ssf.team_id = s.team_id
       AND ssf.season = s.season
      WHERE s.season = %s
      ORDER BY ssf.league, ssf.division, s.pct DESC
    )
    SELECT
      cte.*,
      DENSE_RANK() OVER (
        PARTITION BY division
        ORDER BY pct DESC, division_leader DESC
      ) AS division_rank
    FROM cte
    ORDER BY league, division, division_rank ASC;
    """

    with psycopg.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (season,))
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def build_divs(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    by_league_div = {}
    for r in rows:
        league = r.get("league") or ""
        division = r.get("division") or ""
        by_league_div.setdefault((league, division), []).append(r)

    al_divs, nl_divs = [], []

    for (league, division), teams in by_league_div.items():
        teams_sorted = sorted(
            teams,
            key=lambda x: (x.get("division_rank") or 999, -(x.get("pct") or 0))
        )

        mapped = []
        for t in teams_sorted:
            team_id = t["team_id"]
            mapped.append({
                "team_id": team_id,
                "abbrev": t.get("team_abbrev") or "",
                "w": t.get("w"),
                "l": t.get("l"),
                "pct": f'{t["pct"]:.3f}' if t.get("pct") is not None else "—",
                "gb": t.get("gb") or "—",
                "streak": t.get("streak") or "—",
                "run_diff": t.get("run_differential"),
                "logo_url": f"https://www.mlbstatic.com/team-logos/{team_id}.svg",
                "division_rank": t.get("division_rank"),
                "division_leader": t.get("division_leader"),
                "wild_card": t.get("wild_card"),
            })

        div_obj = {"name": division, "teams": mapped}

        if league == "American League":
            al_divs.append(div_obj)
        elif league == "National League":
            nl_divs.append(div_obj)

    # Keep division order stable
    def div_sort_key(d):
        order = {
            "American League East": 1,
            "American League Central": 2,
            "American League West": 3,
            "National League East": 1,
            "National League Central": 2,
            "National League West": 3,
        }
        return order.get(d["name"], 99)

    al_divs.sort(key=div_sort_key)
    nl_divs.sort(key=div_sort_key)

    return al_divs, nl_divs
