# services/spray_db.py
import os
from datetime import date

# Support psycopg3 or psycopg2 (matches your other DB helpers)
try:
    import psycopg  # type: ignore
    _PSYCOPG3 = True
except Exception:
    psycopg = None
    _PSYCOPG3 = False

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except Exception:
    psycopg2 = None


def _db_url() -> str:
    url = 'postgresql://basenerd_user:d5LmELIOiEszYPBSLSDT1oIi79gkgDV6@dpg-d5i0tku3jp1c73f1d3gg-a.oregon-postgres.render.com/basenerd?sslmode=require'
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    # Render often gives postgres://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def _get_conn():
    url = _db_url()
    if _PSYCOPG3 and psycopg is not None:
        return psycopg.connect(url)
    if psycopg2 is not None:
        return psycopg2.connect(url)
    raise RuntimeError("Neither psycopg3 nor psycopg2 is installed")


_HIT_EVENTS = {"single", "double", "triple", "home_run"}


def fetch_player_spray(player_id: int, season: int | None = None, limit: int = 12000) -> list[dict]:
    """
    Return batted-ball points for a batter.

    Required columns in statcast_pitches:
      - batter (int)
      - events (text)
      - hc_x (numeric)
      - hc_y (numeric)

    Optional (if present, will be returned):
      - game_pk (int)
      - inning (int)
      - inning_topbot (text)

    For season filtering we *prefer* game_date (date/timestamp). If your table
    uses a different column name, adjust the SQL below. If game_date doesn't
    exist, we'll fall back to no date filter.
    """

    base_select = """
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
    """

    # Try with game_date filter first (common)
    if season:
        start = date(season, 1, 1)
        end = date(season + 1, 1, 1)
        sql_with_date = base_select + """
        AND game_date >= %s
        AND game_date < %s
        ORDER BY game_date DESC
        LIMIT %s;
        """
        params_date = [player_id, start, end, limit]
    else:
        sql_with_date = None
        params_date = None

    sql_no_date = base_select + """
      ORDER BY game_pk DESC
      LIMIT %s;
    """
    params_no_date = [player_id, limit]

    conn = _get_conn()
    try:
        if _PSYCOPG3:
            cur = conn.cursor()
        else:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)  # type: ignore

        rows = None

        # Attempt date-filtered query (if requested)
        if season and sql_with_date:
            try:
                cur.execute(sql_with_date, params_date)
                rows = cur.fetchall()
            except Exception:
                # Most likely: game_date column missing or incompatible
                conn.rollback()
                rows = None

        # Fallback: no date filtering
        if rows is None:
            cur.execute(sql_no_date, params_no_date)
            rows = cur.fetchall()

        out: list[dict] = []
        for r in rows:
            # psycopg3 returns tuples by default; handle both shapes
            if isinstance(r, dict):
                game_pk = r.get("game_pk")
                inning = r.get("inning")
                inning_topbot = r.get("inning_topbot")
                ev = (r.get("events") or "").strip().lower()
                hc_x = r.get("hc_x")
                hc_y = r.get("hc_y")
            else:
                game_pk, inning, inning_topbot, ev_raw, hc_x, hc_y = r
                ev = (ev_raw or "").strip().lower()

            out.append({
                "game_pk": game_pk,
                "inning": inning,
                "inning_topbot": inning_topbot,
                "event": ev or None,
                "is_hit": 1 if ev in _HIT_EVENTS else 0,
                "hc_x": hc_x,
                "hc_y": hc_y,
            })

        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass
