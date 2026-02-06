# services/spray_db.py
import os
import psycopg
from datetime import date


def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


_HIT_EVENTS = {"single", "double", "triple", "home_run"}


def fetch_player_spray(player_id: int, season: int, limit: int = 8000) -> list[dict]:
    """
    Returns Statcast batted-ball points for a single batter in a season.

    Expects columns in statcast_pitches:
      - batter (int)
      - events (text)
      - hc_x (numeric)
      - hc_y (numeric)
      - game_pk (int)
      - inning (int)            [optional but you said you have it]
      - inning_topbot (text)   [optional but you said you have it]
      - game_date (date or timestamp)  <-- used for season bounding
    """

    start = date(season, 1, 1)
    end = date(season + 1, 1, 1)

    sql = """
      SELECT
        game_pk,
        inning,
        inning_topbot,
        events,
        hc_x,
        hc_y
      FROM statcast_pitches
      WHERE batter = %s
        AND hc_x IS NOT NULL
        AND hc_y IS NOT NULL
        AND game_date >= %s
        AND game_date < %s
      ORDER BY game_date DESC
      LIMIT %s;
    """

    with psycopg.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (player_id, start, end, limit))
            cols = [d.name for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    # Add a small amount of normalization for the front-end
    out: list[dict] = []
    for r in rows:
        ev = (r.get("events") or "").strip().lower()
        out.append(
            {
                "game_pk": r.get("game_pk"),
                "inning": r.get("inning"),
                "inning_topbot": r.get("inning_topbot"),
                "event": ev or None,
                "is_hit": 1 if ev in _HIT_EVENTS else 0,
                "hc_x": r.get("hc_x"),
                "hc_y": r.get("hc_y"),
            }
        )

    return out
