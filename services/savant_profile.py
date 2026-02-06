# services/savant_profile.py
import os
import psycopg


def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def _as_float(x, default=None):
    if x is None:
        return default
    try:
        return float(x)
    except Exception:
        return default


def _as_int(x, default=None):
    if x is None:
        return default
    try:
        return int(x)
    except Exception:
        return default


# Stats where "lower is better" in percentile display
_LOWER_BETTER = {"k_pct", "whiff_pct", "chase_pct"}


def get_player_savant_profile(player_id: int, season: int, min_pa: int = 1) -> dict:
    """
    Returns a dict for player.html:
      {
        "available": bool,
        "season": int,
        "player_id": int,
        "groups": [
          {"title":"Batting", "rows":[{"label","value","pct","fmt","suffix?"}, ...]},
          ...
        ]
      }

    IMPORTANT:
    - This version will render even if you don't have every column populated.
    - Percentiles are computed vs the season pool with pa>=min_pa (where applicable).
    """

    season = _as_int(season, None)
    player_id = _as_int(player_id, None)
    if season is None or player_id is None:
        return {"available": False, "season": season, "player_id": player_id, "groups": []}

    # We compute percentiles in SQL using percent_rank().
    # For lower-better stats, we invert later: pct = 100 - pct.
    sql = """
    WITH pool AS (
      SELECT *
      FROM savant_batting_season
      WHERE season = %s
        AND COALESCE(pa, 0) >= %s
    ),
    ranked AS (
      SELECT
        player_id,

        xwoba,
        xba,
        xslg,
        avg_exit_velocity,
        barrel_pct,
        hardhit_pct,
        sweet_spot_pct,
        chase_pct,
        whiff_pct,
        k_pct,
        bb_pct,

        (percent_rank() OVER (ORDER BY xwoba)) * 100 AS pct_xwoba,
        (percent_rank() OVER (ORDER BY xba)) * 100 AS pct_xba,
        (percent_rank() OVER (ORDER BY xslg)) * 100 AS pct_xslg,
        (percent_rank() OVER (ORDER BY avg_exit_velocity)) * 100 AS pct_avg_exit_velocity,
        (percent_rank() OVER (ORDER BY barrel_pct)) * 100 AS pct_barrel_pct,
        (percent_rank() OVER (ORDER BY hardhit_pct)) * 100 AS pct_hardhit_pct,
        (percent_rank() OVER (ORDER BY sweet_spot_pct)) * 100 AS pct_sweet_spot_pct,
        (percent_rank() OVER (ORDER BY chase_pct)) * 100 AS pct_chase_pct,
        (percent_rank() OVER (ORDER BY whiff_pct)) * 100 AS pct_whiff_pct,
        (percent_rank() OVER (ORDER BY k_pct)) * 100 AS pct_k_pct,
        (percent_rank() OVER (ORDER BY bb_pct)) * 100 AS pct_bb_pct

      FROM pool
    )
    SELECT *
    FROM ranked
    WHERE player_id = %s
    LIMIT 1;
    """

    with psycopg.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (season, min_pa, player_id))
            row = cur.fetchone()
            if not row:
                return {"available": False, "season": season, "player_id": player_id, "groups": []}

            cols = [d.name for d in cur.description]
            r = dict(zip(cols, row))

    def pct(key: str):
        raw = r.get(f"pct_{key}")
        if raw is None:
            return None
        p = int(round(_as_float(raw, 0.0)))
        if key in _LOWER_BETTER:
            p = 100 - p
        return max(0, min(100, p))

    def val(key: str):
        return r.get(key)

    batting_rows = [
        {"label": "xwOBA", "value": _as_float(val("xwoba")), "pct": pct("xwoba"), "fmt": "3f"},
        {"label": "xBA", "value": _as_float(val("xba")), "pct": pct("xba"), "fmt": "3f"},
        {"label": "xSLG", "value": _as_float(val("xslg")), "pct": pct("xslg"), "fmt": "3f"},
        {"label": "Avg EV", "value": _as_float(val("avg_exit_velocity")), "pct": pct("avg_exit_velocity"), "fmt": "1f"},
        {"label": "Barrel%", "value": _as_float(val("barrel_pct")), "pct": pct("barrel_pct"), "fmt": "1f", "suffix": "%"},
        {"label": "HardHit%", "value": _as_float(val("hardhit_pct")), "pct": pct("hardhit_pct"), "fmt": "1f", "suffix": "%"},
        {"label": "SweetSpot%", "value": _as_float(val("sweet_spot_pct")), "pct": pct("sweet_spot_pct"), "fmt": "1f", "suffix": "%"},
        {"label": "Chase%", "value": _as_float(val("chase_pct")), "pct": pct("chase_pct"), "fmt": "1f", "suffix": "%"},
        {"label": "Whiff%", "value": _as_float(val("whiff_pct")), "pct": pct("whiff_pct"), "fmt": "1f", "suffix": "%"},
        {"label": "K%", "value": _as_float(val("k_pct")), "pct": pct("k_pct"), "fmt": "1f", "suffix": "%"},
        {"label": "BB%", "value": _as_float(val("bb_pct")), "pct": pct("bb_pct"), "fmt": "1f", "suffix": "%"},
    ]

    # Strip rows that are fully missing (value=None AND pct=None) so it doesn't look broken
    batting_rows = [x for x in batting_rows if not (x.get("value") is None and x.get("pct") is None)]

    groups = []
    if batting_rows:
        groups.append({"title": "Batting", "rows": batting_rows})

    return {"available": True, "season": season, "player_id": player_id, "groups": groups}
