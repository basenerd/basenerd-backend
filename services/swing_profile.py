# services/swing_profile.py
"""
Swing Profile service — computes bat path / batted ball metrics
from statcast_pitches for a given batter + season.
"""
import os
import psycopg


def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def get_swing_profile(player_id: int, season: int) -> dict:
    """
    Returns swing profile data for the bat-path visualization.

    Output:
    {
      "available": bool,
      "player_id": int,
      "season": int,
      "batted_balls": int,        # total BIP
      "avg_ev": float,            # avg exit velocity
      "max_ev": float,            # max exit velocity
      "avg_la": float,            # avg launch angle
      "p10_la": float,            # 10th percentile launch angle
      "p90_la": float,            # 90th percentile launch angle
      "barrel_pct": float,        # barrel rate (%)
      "hardhit_pct": float,       # hard hit rate (EV >= 95)
      "sweet_spot_pct": float,    # sweet spot rate (LA 8-32)
      "gb_pct": float,            # ground ball % (LA < 10)
      "ld_pct": float,            # line drive % (LA 10-25)
      "fb_pct": float,            # fly ball % (LA 25-50)
      "pu_pct": float,            # popup % (LA >= 50)
      "pull_pct": float,          # pull % (spray_angle logic)
      "cent_pct": float,          # center %
      "oppo_pct": float,          # oppo %
      "la_buckets": [             # launch angle distribution (5-deg bins)
        {"la": -10, "pct": 5.2},
        {"la": -5,  "pct": 8.1},
        ...
      ],
      "ev_la_points": [           # scatter data (sampled)
        {"ev": 98.2, "la": 15, "event": "single"},
        ...
      ]
    }
    """
    sql = """
    SELECT
        launch_speed,
        launch_angle,
        CASE
            WHEN stand = 'L' THEN -1 * spray_angle
            ELSE spray_angle
        END AS adj_spray,
        events
    FROM statcast_pitches
    WHERE batter = %s
      AND game_year = %s
      AND launch_speed IS NOT NULL
      AND launch_angle IS NOT NULL
      AND type = 'X'
    ORDER BY game_date, at_bat_number
    """

    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (player_id, season))
                rows = cur.fetchall()
    except Exception:
        return {"available": False, "player_id": player_id, "season": season}

    if not rows or len(rows) < 5:
        return {"available": False, "player_id": player_id, "season": season}

    evs = []
    las = []
    sprays = []
    events = []
    for ev, la, spray, event in rows:
        try:
            evs.append(float(ev))
            las.append(float(la))
            sprays.append(float(spray) if spray is not None else None)
            events.append(event or "")
        except (TypeError, ValueError):
            continue

    n = len(evs)
    if n < 5:
        return {"available": False, "player_id": player_id, "season": season}

    avg_ev = sum(evs) / n
    max_ev = max(evs)
    avg_la = sum(las) / n

    sorted_la = sorted(las)
    p10_la = sorted_la[max(0, int(n * 0.10))]
    p90_la = sorted_la[min(n - 1, int(n * 0.90))]

    # Barrel: EV >= 98 AND LA between 26-30 (simplified Statcast barrel definition)
    barrels = sum(1 for ev, la in zip(evs, las)
                  if ev >= 98 and 26 <= la <= 30)
    # Expand barrel zone: for each mph over 98, LA range widens
    for ev, la in zip(evs, las):
        if ev >= 98 and not (26 <= la <= 30):
            over = ev - 98
            lo = max(26 - over, 8)
            hi = min(30 + over, 50)
            if lo <= la <= hi:
                barrels += 1

    hardhit = sum(1 for ev in evs if ev >= 95)
    sweet_spot = sum(1 for la in las if 8 <= la <= 32)
    gb = sum(1 for la in las if la < 10)
    ld = sum(1 for la in las if 10 <= la < 25)
    fb = sum(1 for la in las if 25 <= la < 50)
    pu = sum(1 for la in las if la >= 50)

    # Spray direction (adjusted so positive = pull for both L/R)
    valid_sprays = [s for s in sprays if s is not None]
    ns = len(valid_sprays) if valid_sprays else 1
    pull = sum(1 for s in valid_sprays if s is not None and s > 15)
    cent = sum(1 for s in valid_sprays if s is not None and -15 <= s <= 15)
    oppo = sum(1 for s in valid_sprays if s is not None and s < -15)

    # Launch angle distribution in 5-degree buckets from -30 to 70
    la_buckets = []
    for bucket_start in range(-30, 75, 5):
        count = sum(1 for la in las if bucket_start <= la < bucket_start + 5)
        la_buckets.append({"la": bucket_start, "pct": round(count / n * 100, 1)})

    # EV/LA scatter points (all points, frontend can sample if needed)
    ev_la_points = []
    for i in range(n):
        ev_la_points.append({
            "ev": round(evs[i], 1),
            "la": round(las[i], 1),
            "event": events[i] or "out"
        })

    return {
        "available": True,
        "player_id": player_id,
        "season": season,
        "batted_balls": n,
        "avg_ev": round(avg_ev, 1),
        "max_ev": round(max_ev, 1),
        "avg_la": round(avg_la, 1),
        "p10_la": round(p10_la, 1),
        "p90_la": round(p90_la, 1),
        "barrel_pct": round(barrels / n * 100, 1),
        "hardhit_pct": round(hardhit / n * 100, 1),
        "sweet_spot_pct": round(sweet_spot / n * 100, 1),
        "gb_pct": round(gb / n * 100, 1),
        "ld_pct": round(ld / n * 100, 1),
        "fb_pct": round(fb / n * 100, 1),
        "pu_pct": round(pu / n * 100, 1),
        "pull_pct": round(pull / ns * 100, 1),
        "cent_pct": round(cent / ns * 100, 1),
        "oppo_pct": round(oppo / ns * 100, 1),
        "la_buckets": la_buckets,
        "ev_la_points": ev_la_points,
    }
