# services/standings_db.py
import os
import psycopg
from datetime import datetime

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
