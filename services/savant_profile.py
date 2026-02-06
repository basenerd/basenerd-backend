import os
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs

import pg8000


def _connect():
    db_url = 'postgresql://basenerd_user:d5LmELIOiEszYPBSLSDT1oIi79gkgDV6@dpg-d5i0tku3jp1c73f1d3gg-a.oregon-postgres.render.com/basenerd?sslmode=require'
    if not db_url:
        raise RuntimeError("DATABASE_URL missing")

    u = urlparse(db_url)
    q = parse_qs(u.query or "")
    sslmode = (q.get("sslmode", ["require"])[0] or "require").lower()
    use_ssl = sslmode != "disable"

    kwargs = dict(
        user=u.username,
        password=u.password,
        host=u.hostname,
        port=u.port or 5432,
        database=(u.path or "").lstrip("/"),
    )
    if use_ssl:
        kwargs["ssl_context"] = True
    return pg8000.connect(**kwargs)


# Stats where LOWER is better -> invert percentile for display
LOWER_BETTER = {"k_pct", "whiff_pct", "chase_pct"}


def get_player_savant_profile(player_id: int, season: int, min_pa: int = 1) -> Dict[str, Any]:
    """
    Returns:
      {
        "season": 2025,
        "player_id": 123,
        "available": True/False,
        "groups": [
           {"title": "Batting", "rows":[{"key":"xwoba","label":"xwOBA","value":0.361,"pct":88}, ...]},
           {"title": "Running", "rows":[...]}
        ]
      }
    """
    conn = _connect()
    try:
        cur = conn.cursor()

        # --- Batting percentiles (league pool = same season, pa>=min_pa) ---
        # percent_rank gives 0..1; multiply by 100.
        # For LOWER_BETTER stats, invert: 100 - pct.
        batting_sql = f"""
        with pool as (
          select *
          from savant_batting_season
          where season = %s
            and coalesce(pa, 0) >= %s
        ),
        ranked as (
          select
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

            (percent_rank() over (order by xwoba)) * 100 as pct_xwoba,
            (percent_rank() over (order by xba)) * 100 as pct_xba,
            (percent_rank() over (order by xslg)) * 100 as pct_xslg,
            (percent_rank() over (order by avg_exit_velocity)) * 100 as pct_avg_exit_velocity,
            (percent_rank() over (order by barrel_pct)) * 100 as pct_barrel_pct,
            (percent_rank() over (order by hardhit_pct)) * 100 as pct_hardhit_pct,
            (percent_rank() over (order by sweet_spot_pct)) * 100 as pct_sweet_spot_pct,

            (percent_rank() over (order by chase_pct)) * 100 as pct_chase_pct,
            (percent_rank() over (order by whiff_pct)) * 100 as pct_whiff_pct,
            (percent_rank() over (order by k_pct)) * 100 as pct_k_pct,
            (percent_rank() over (order by bb_pct)) * 100 as pct_bb_pct

          from pool
        )
        select *
        from ranked
        where player_id = %s
        """
        cur.execute(batting_sql, (season, min_pa, player_id))
        bat_row = cur.fetchone()
        bat_cols = [d[0] for d in cur.description] if cur.description else []

        # Optional: Sprint speed (no PA filter)
        cur.execute(
            """
            with pool as (
              select *
              from savant_sprint_speed_season
              where season = %s
            ),
            ranked as (
              select
                player_id,
                sprint_speed,
                (percent_rank() over (order by sprint_speed)) * 100 as pct_sprint_speed
              from pool
            )
            select *
            from ranked
            where player_id = %s
            """,
            (season, player_id),
        )
        sp_row = cur.fetchone()
        sp_cols = [d[0] for d in cur.description] if cur.description else []

        # Optional: OAA (fielding)
        cur.execute(
            """
            with pool as (
              select *
              from savant_oaa_season
              where season = %s
            ),
            ranked as (
              select
                player_id,
                oaa,
                (percent_rank() over (order by oaa)) * 100 as pct_oaa
              from pool
            )
            select *
            from ranked
            where player_id = %s
            """,
            (season, player_id),
        )
        oaa_row = cur.fetchone()
        oaa_cols = [d[0] for d in cur.description] if cur.description else []

    finally:
        conn.close()

    # If we have no batting row, profile is “not available”
    if not bat_row:
        return {"season": season, "player_id": player_id, "available": False, "groups": []}

    bat = dict(zip(bat_cols, bat_row))
    sp = dict(zip(sp_cols, sp_row)) if sp_row else {}
    oaa = dict(zip(oaa_cols, oaa_row)) if oaa_row else {}

    def pct(key: str) -> Optional[int]:
        v = bat.get(f"pct_{key}")
        if v is None:
            return None
        p = int(round(float(v)))
        if key in LOWER_BETTER:
            p = 100 - p
        return max(0, min(100, p))

    def pct2(raw_pct: Any, invert: bool = False) -> Optional[int]:
        if raw_pct is None:
            return None
        p = int(round(float(raw_pct)))
        if invert:
            p = 100 - p
        return max(0, min(100, p))

    groups: List[Dict[str, Any]] = []

    # Batting group (you can reorder/add/remove)
    batting_rows = [
        {"key": "xwoba", "label": "xwOBA", "value": bat.get("xwoba"), "pct": pct("xwoba"), "fmt": "3f"},
        {"key": "xba", "label": "xBA", "value": bat.get("xba"), "pct": pct("xba"), "fmt": "3f"},
        {"key": "xslg", "label": "xSLG", "value": bat.get("xslg"), "pct": pct("xslg"), "fmt": "3f"},
        {"key": "avg_exit_velocity", "label": "Avg EV", "value": bat.get("avg_exit_velocity"), "pct": pct("avg_exit_velocity"), "fmt": "1f"},
        {"key": "barrel_pct", "label": "Barrel%", "value": bat.get("barrel_pct"), "pct": pct("barrel_pct"), "fmt": "1f", "suffix": "%"},
        {"key": "hardhit_pct", "label": "HardHit%", "value": bat.get("hardhit_pct"), "pct": pct("hardhit_pct"), "fmt": "1f", "suffix": "%"},
        {"key": "sweet_spot_pct", "label": "SweetSpot%", "value": bat.get("sweet_spot_pct"), "pct": pct("sweet_spot_pct"), "fmt": "1f", "suffix": "%"},
        {"key": "chase_pct", "label": "Chase%", "value": bat.get("chase_pct"), "pct": pct("chase_pct"), "fmt": "1f", "suffix": "%"},
        {"key": "whiff_pct", "label": "Whiff%", "value": bat.get("whiff_pct"), "pct": pct("whiff_pct"), "fmt": "1f", "suffix": "%"},
        {"key": "k_pct", "label": "K%", "value": bat.get("k_pct"), "pct": pct("k_pct"), "fmt": "1f", "suffix": "%"},
        {"key": "bb_pct", "label": "BB%", "value": bat.get("bb_pct"), "pct": pct("bb_pct"), "fmt": "1f", "suffix": "%"},
    ]
    groups.append({"title": "Batting", "rows": batting_rows})

    # Running (optional)
    if sp:
        groups.append({
            "title": "Running",
            "rows": [
                {"key": "sprint_speed", "label": "Sprint Speed", "value": sp.get("sprint_speed"),
                 "pct": pct2(sp.get("pct_sprint_speed")), "fmt": "1f"}
            ]
        })

    # Fielding (optional)
    if oaa:
        groups.append({
            "title": "Fielding",
            "rows": [
                {"key": "oaa", "label": "OAA", "value": oaa.get("oaa"),
                 "pct": pct2(oaa.get("pct_oaa")), "fmt": "1f"}
            ]
        })

    return {"season": season, "player_id": player_id, "available": True, "groups": groups}
