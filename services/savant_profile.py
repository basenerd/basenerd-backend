# services/savant_profile.py

from services.db import query_one


def get_player_savant_profile(player_id, season, min_pa=1):
    """
    Build a Savant-style scouting profile for one player + season.

    Returns:
      {
        available: bool,
        season: int,
        groups: [
            { title: str, rows: [ {label, value, pct, fmt, suffix?}, ... ] }
        ]
      }
    """

    row = query_one(
        """
        select
            season,
            player_id,
            player_name,
            team,
            pa,
            avg_exit_velocity,
            xba,
            xslg,
            xwoba,
            barrel_batted_rate,
            hard_hit_percent,
            sweet_spot_percent,
            whiff_percent,
            chase_pct,
            sprint_speed
        from savant_batting_season
        where player_id = %s
          and season = %s
          and pa >= %s
        """,
        (player_id, season, min_pa),
    )

    if not row:
        return {
            "available": False,
            "season": season,
            "groups": []
        }

    # For now: no percentiles yet — just render values with a dummy 50 pct
    # (we will add real percentiles next)
    rows = [
        ("Avg Exit Velo", "avg_exit_velocity", "1f", ""),
        ("xBA", "xba", "3f", ""),
        ("xSLG", "xslg", "3f", ""),
        ("xwOBA", "xwoba", "3f", ""),
        ("Barrel %", "barrel_batted_rate", "1f", "%"),
        ("Hard-Hit %", "hard_hit_percent", "1f", "%"),
        ("Sweet Spot %", "sweet_spot_percent", "1f", "%"),
        ("Whiff %", "whiff_percent", "1f", "%"),
        ("Chase %", "chase_pct", "1f", "%"),
        ("Sprint Speed", "sprint_speed", "1f", ""),
    ]

    def val(col):
        v = row.get(col)
        if v is None:
            return None
        try:
            return float(v)
        except Exception:
            return None

    group = {
        "title": "Offense / Running",
        "rows": []
    }

    for label, col, fmt, suffix in rows:
        group["rows"].append({
            "label": label,
            "value": val(col),
            "pct": 50,   # placeholder
            "fmt": fmt,
            "suffix": suffix
        })

    return {
        "available": True,
        "season": season,
        "groups": [group]
    }
