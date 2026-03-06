# services/pitching_report.py
from __future__ import annotations

import os
import math
from typing import Any, Dict, List, Optional, Tuple

import psycopg


# --- Fixed zone (matches your previous fixed zone request) ---
ZONE_TOP = 3.3942716630565757
ZONE_BOT = 1.594602333802632
ZONE_LEFT = -0.83
ZONE_RIGHT = 0.83

# --- Heatmap grid settings (in Statcast plate_x/plate_z units: feet) ---
X_MIN = -2.0
X_MAX = 2.0
Z_MIN = 0.0
Z_MAX = 5.0

GRID_NX = 60
GRID_NZ = 60

GAUSS_SIGMA = 1.4  # KDE-ish smoothness

# Statcast descriptions that count as "swing"
SWING = {
    "swinging_strike",
    "swinging_strike_blocked",
    "foul",
    "foul_tip",
    "foul_bunt",
    "hit_into_play",
    "hit_into_play_no_out",
    "hit_into_play_score",
}

# Statcast descriptions that count as "whiff" (swing + miss)
WHIFF = {"swinging_strike", "swinging_strike_blocked"}

# Statcast zone numbers: 1-9 = in strike zone, 11-14 = out of zone
IN_ZONE: set = set(range(1, 10))
OUT_OF_ZONE: set = {11, 12, 13, 14}

# Statcast events that end a plate appearance as strikeout or walk
K_EVENTS = {"strikeout", "strikeout_double_play"}
BB_EVENTS = {"walk", "intent_walk"}


def _db_url() -> str:
    url = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_URL_PG") or ""
    if not url:
        raise RuntimeError("Missing DATABASE_URL env var")
    return url


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if not math.isfinite(v):
            return None
        return v
    except Exception:
        return None


def _mean(vals: List[Optional[float]]) -> Optional[float]:
    vv = [v for v in vals if v is not None and math.isfinite(v)]
    if not vv:
        return None
    return float(sum(vv)) / float(len(vv))


def _stand_lr(stand: Optional[str], p_throws: Optional[str]) -> Optional[str]:
    """
    For batter-handedness filtering only. Does NOT flip coordinates.

    If stand == 'S', map based on pitcher handedness:
      RHP => treat as LHH
      LHP => treat as RHH
    """
    if not stand:
        return None
    s = stand.upper()
    if s in ("L", "R"):
        return s
    if s == "S":
        pt = (p_throws or "").upper()
        if pt == "R":
            return "L"
        if pt == "L":
            return "R"
    return None


def _bin_index(v: float, vmin: float, vmax: float, n: int) -> Optional[int]:
    if v is None or not math.isfinite(v):
        return None
    if v < vmin or v > vmax:
        return None
    t = (v - vmin) / (vmax - vmin) if vmax != vmin else 0.0
    i = int(t * n)
    if i == n:
        i = n - 1
    if i < 0 or i >= n:
        return None
    return i


def _zeros_grid() -> List[List[float]]:
    return [[0.0 for _ in range(GRID_NX)] for __ in range(GRID_NZ)]


def _hist2d(points: List[Tuple[float, float]]) -> List[List[float]]:
    H = _zeros_grid()
    for x, z in points:
        ix = _bin_index(x, X_MIN, X_MAX, GRID_NX)
        iz = _bin_index(z, Z_MIN, Z_MAX, GRID_NZ)
        if ix is None or iz is None:
            continue
        H[iz][ix] += 1.0
    return H


def _gaussian_kernel_1d(sigma: float) -> List[float]:
    r = max(1, int(math.ceil(3.0 * sigma)))
    xs = list(range(-r, r + 1))
    k = [math.exp(-(x * x) / (2.0 * sigma * sigma)) for x in xs]
    s = sum(k) or 1.0
    return [v / s for v in k]


def _convolve_1d(arr: List[float], kernel: List[float]) -> List[float]:
    r = (len(kernel) - 1) // 2
    n = len(arr)
    out = [0.0] * n
    for i in range(n):
        acc = 0.0
        for j, w in enumerate(kernel):
            ii = i + (j - r)
            if ii < 0:
                ii = 0
            elif ii >= n:
                ii = n - 1
            acc += arr[ii] * w
        out[i] = acc
    return out


def _blur_grid(grid: List[List[float]], sigma: float = GAUSS_SIGMA) -> List[List[float]]:
    if not grid:
        return grid
    k = _gaussian_kernel_1d(sigma)

    # blur X
    tmp = []
    for row in grid:
        tmp.append(_convolve_1d(row, k))

    # blur Z
    nz = len(tmp)
    nx = len(tmp[0]) if nz else 0
    out = [[0.0 for _ in range(nx)] for _ in range(nz)]
    for x in range(nx):
        col = [tmp[z][x] for z in range(nz)]
        colb = _convolve_1d(col, k)
        for z in range(nz):
            out[z][x] = colb[z]
    return out


def _grid_payload(grid: List[List[float]]) -> Dict[str, Any]:
    return {
        "grid": grid,
        "nx": GRID_NX,
        "nz": GRID_NZ,
        "x_min": X_MIN,
        "x_max": X_MAX,
        "z_min": Z_MIN,
        "z_max": Z_MAX,
        "zone": {"left": ZONE_LEFT, "right": ZONE_RIGHT, "top": ZONE_TOP, "bot": ZONE_BOT},
    }


# ----------------------------
# DB query
# ----------------------------
def _fetch_pitches(
    pitcher_id: int,
    season: int,
    game_pk: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Pull pitch-level Statcast rows for a pitcher.
    Tries a LEFT JOIN with statcast_pitches_live to get stuff_plus;
    falls back to statcast_pitches-only if the join table is missing.
    """
    base_filter = "WHERE sp.pitcher = %s AND sp.game_year = %s"
    params: List[Any] = [int(pitcher_id), int(season)]
    if game_pk:
        base_filter += " AND sp.game_pk = %s"
        params.append(int(game_pk))

    _join_cols = """
      sp.game_pk,
      sp.game_date,
      sp.at_bat_number,
      sp.pitch_number,
      sp.pitch_type,
      sp.pitch_name,
      sp.release_speed,
      sp.release_spin_rate,
      sp.pfx_x,
      sp.pfx_z,
      sp.stand,
      sp.p_throws,
      sp.plate_x,
      sp.plate_z,
      sp.description,
      sp.estimated_woba_using_speedangle,
      sp.zone,
      sp.events"""

    _join_clause = f"""
    FROM statcast_pitches sp
    LEFT JOIN statcast_pitches_live spl
      ON sp.game_pk = spl.game_pk
     AND sp.at_bat_number = spl.at_bat_number
     AND sp.pitch_number = spl.pitch_number
    {base_filter}
    """

    # Try with both stuff_plus and control_plus first
    sql_both = f"SELECT {_join_cols}, spl.stuff_plus, spl.control_plus {_join_clause}"
    # Fallback: stuff_plus only (control_plus column may not exist yet)
    sql_stuff_only = f"SELECT {_join_cols}, spl.stuff_plus, NULL as control_plus {_join_clause}"

    # Final fallback: no JOIN, no stuff_plus
    fb_filter = base_filter.replace("sp.", "")
    fb_params: List[Any] = [int(pitcher_id), int(season)]
    if game_pk:
        fb_params.append(int(game_pk))

    sql_fallback = f"""
    SELECT
      game_pk,
      game_date,
      at_bat_number,
      pitch_number,
      pitch_type,
      pitch_name,
      release_speed,
      release_spin_rate,
      pfx_x,
      pfx_z,
      stand,
      p_throws,
      plate_x,
      plate_z,
      description,
      estimated_woba_using_speedangle,
      zone,
      events
    FROM statcast_pitches
    {fb_filter}
    """

    with psycopg.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql_both, params)
            except Exception:
                conn.rollback()
                try:
                    cur.execute(sql_stuff_only, params)
                except Exception:
                    conn.rollback()
                    cur.execute(sql_fallback, fb_params)
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, r)) for r in rows]


# ----------------------------
# PUBLIC: matches app.py imports
# ----------------------------
def list_pitching_games(pitcher_id: int, season: int) -> List[Dict[str, Any]]:
    sql = """
    SELECT DISTINCT game_pk, MIN(game_date) as game_date
    FROM statcast_pitches
    WHERE pitcher = %s
      AND game_year = %s
    GROUP BY game_pk
    ORDER BY game_date DESC, game_pk DESC
    """
    with psycopg.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (int(pitcher_id), int(season)))
            rows = cur.fetchall()
            result = []
            for r in rows:
                if not r or r[0] is None:
                    continue
                gd = str(r[1])[:10] if r[1] else ""
                result.append({
                    "game_pk": int(r[0]),
                    "date": gd,
                    "label": gd or str(r[0]),
                })
            return result


def pitching_report_summary(
    pitcher_id: int,
    season: int,
    game_pk: Optional[int] = None,
) -> Dict[str, Any]:
    arr = _fetch_pitches(pitcher_id, season, game_pk=game_pk)
    if not arr:
        return {"ok": False, "pitcher_id": int(pitcher_id), "season": int(season), "game_pk": game_pk}

    by_pt: Dict[str, List[Dict[str, Any]]] = {}
    for r in arr:
        pt = (r.get("pitch_type") or "").strip() or "UNK"
        by_pt.setdefault(pt, []).append(r)

    total = len(arr)

    # usage split vs LHH / RHH (switch-hitters mapped by pitcher handedness)
    side_totals: Dict[str, int] = {"L": 0, "R": 0}
    side_by_pt: Dict[str, Dict[str, int]] = {}
    for r in arr:
        pt0 = (r.get("pitch_type") or "").strip() or "UNK"
        side0 = _stand_lr(r.get("stand"), r.get("p_throws"))
        if side0 in ("L", "R"):
            side_totals[side0] += 1
            d = side_by_pt.setdefault(pt0, {"L": 0, "R": 0})
            d[side0] += 1

    mix: List[Dict[str, Any]] = []
    shapes: List[Dict[str, Any]] = []
    pitches: List[Dict[str, Any]] = []

    # Aggregate counters for the top-level basic line
    agg_swings = 0
    agg_whiffs = 0
    agg_in_zone = 0
    agg_ooz = 0
    agg_ooz_swings = 0
    agg_xw: List[float] = []
    agg_events: List[str] = []

    for pt, rows in by_pt.items():
        n = len(rows)
        pitch_name = (rows[0].get("pitch_name") or pt) if rows else pt

        pitches.append({"pitch_type": pt, "pitch_name": pitch_name, "n": n})

        velo = [_safe_float(x.get("release_speed")) for x in rows]
        spin = [_safe_float(x.get("release_spin_rate")) for x in rows]
        hb = [_safe_float(x.get("pfx_x")) for x in rows]  # feet
        vb = [_safe_float(x.get("pfx_z")) for x in rows]  # feet (already induced / gravity-removed)
        sp_vals = [_safe_float(x.get("stuff_plus")) for x in rows]
        cp_vals = [_safe_float(x.get("control_plus")) for x in rows]

        swings = 0
        whiffs = 0
        in_zone = 0
        ooz = 0
        ooz_swings = 0
        xw_list: List[Optional[float]] = []

        for x in rows:
            desc = (x.get("description") or "").lower().strip()
            zone_num = x.get("zone")
            ev = (x.get("events") or "").lower().strip()

            if desc in SWING:
                swings += 1
            if desc in WHIFF:
                whiffs += 1

            if zone_num is not None:
                try:
                    zn = int(zone_num)
                    if zn in IN_ZONE:
                        in_zone += 1
                    elif zn in OUT_OF_ZONE:
                        ooz += 1
                        if desc in SWING:
                            ooz_swings += 1
                except (ValueError, TypeError):
                    pass

            xw = _safe_float(x.get("estimated_woba_using_speedangle"))
            if xw is not None:
                xw_list.append(xw)

            if ev:
                agg_events.append(ev)

        # Aggregate into overall totals
        agg_swings += swings
        agg_whiffs += whiffs
        agg_in_zone += in_zone
        agg_ooz += ooz
        agg_ooz_swings += ooz_swings
        agg_xw.extend(xw_list)

        # Only include stuff_plus/control_plus mean if we have data
        sp_mean = _mean([v for v in sp_vals if v is not None])
        cp_mean = _mean([v for v in cp_vals if v is not None])

        mix.append(
            {
                "pitch_type": pt,
                "pitch_name": pitch_name,
                "n": n,
                "usage": (100.0 * n / total) if total else 0.0,
                "velo": _mean(velo),
                "spin": _mean(spin),
                "hb": (_mean(hb) * 12.0) if _mean(hb) is not None else None,  # inches
                "ivb": (_mean(vb) * 12.0) if _mean(vb) is not None else None,  # inches (pfx_z already induced)
                "xwoba": _mean(xw_list),
                "whiff": (100.0 * whiffs / swings) if swings else None,
                "zone_pct": (100.0 * in_zone / n) if n else None,
                "chase_pct": (100.0 * ooz_swings / ooz) if ooz else None,
                "stuff_plus": sp_mean,
                "control_plus": cp_mean,
            }
        )

        # shapes feed the movement plot
        shapes.append(
            {
                "pitch_type": pt,
                "pitch_name": pitch_name,
                "n": n,
                "pfx_x": _mean(hb),  # feet
                "pfx_z": _mean(vb),  # feet
            }
        )

    # Build aggregate basic line
    agg_pa = len(agg_events)
    agg_k = sum(1 for e in agg_events if e in K_EVENTS)
    agg_bb = sum(1 for e in agg_events if e in BB_EVENTS)
    basic = {
        "whiff_pct": (100.0 * agg_whiffs / agg_swings) if agg_swings else None,
        "zone_pct": (100.0 * agg_in_zone / total) if total else None,
        "chase_pct": (100.0 * agg_ooz_swings / agg_ooz) if agg_ooz else None,
        "xwoba": _mean(agg_xw) if agg_xw else None,
        "k_pct": (100.0 * agg_k / agg_pa) if agg_pa else None,
        "bb_pct": (100.0 * agg_bb / agg_pa) if agg_pa else None,
    }

    pitches.sort(key=lambda x: x["n"], reverse=True)
    mix.sort(key=lambda x: x["n"], reverse=True)
    shapes.sort(key=lambda x: x["n"], reverse=True)

    usage_lr: List[Dict[str, Any]] = []
    ltot = int(side_totals.get("L") or 0)
    rtot = int(side_totals.get("R") or 0)
    for p in pitches:
        pt0 = (p.get("pitch_type") or "").strip() or "UNK"
        name0 = p.get("pitch_name") or pt0
        d = side_by_pt.get(pt0) or {"L": 0, "R": 0}
        lc = int(d.get("L") or 0)
        rc = int(d.get("R") or 0)
        usage_lr.append(
            {
                "pitch_type": pt0,
                "pitch_name": name0,
                "l_count": lc,
                "r_count": rc,
                "l_usage": (100.0 * lc / ltot) if ltot else 0.0,
                "r_usage": (100.0 * rc / rtot) if rtot else 0.0,
            }
        )

    return {
        "ok": True,
        "pitcher_id": int(pitcher_id),
        "season": int(season),
        "game_pk": int(game_pk) if game_pk else None,
        "total": total,
        "basic": basic,
        "pitches": pitches,  # used by your front-end dropdown builder
        "mix": mix,
        "shapes": shapes,
        "usage_lr": usage_lr,
        "side_totals": side_totals,
        "zone": {"left": ZONE_LEFT, "right": ZONE_RIGHT, "top": ZONE_TOP, "bot": ZONE_BOT},
    }


def pitching_heatmap(
    pitcher_id: int,
    season: int,
    pitch_type: str,
    stand_lr: str,
    metric: str = "density",
    game_pk: Optional[int] = None,
) -> Dict[str, Any]:
    arr = _fetch_pitches(pitcher_id, season, game_pk=game_pk)
    if not arr:
        return {"ok": False, "reason": "no_pitches"}

    pt = (pitch_type or "").strip() or ""
    side = (stand_lr or "").strip().upper()
    if side not in ("L", "R"):
        return {"ok": False, "reason": "bad_stand"}

    pts: List[Tuple[float, float]] = []
    xw_pts: List[Tuple[float, float, float]] = []

    for r in arr:
        if (r.get("pitch_type") or "").strip() != pt:
            continue
        s = _stand_lr(r.get("stand"), r.get("p_throws"))
        if s != side:
            continue

        x = _safe_float(r.get("plate_x"))
        z = _safe_float(r.get("plate_z"))
        if x is None or z is None:
            continue

        pts.append((x, z))

        xw = _safe_float(r.get("estimated_woba_using_speedangle"))
        if xw is not None:
            xw_pts.append((x, z, xw))

    if not pts:
        return {"ok": False, "reason": "no_points"}

    if metric == "density":
        H = _hist2d(pts)
        Hb = _blur_grid(H, GAUSS_SIGMA)
        out = _grid_payload(Hb)
        out.update({"ok": True, "metric": "density", "pitch_type": pt, "stand_lr": side})
        return out

    # xwOBA heatmap: average xwOBA per cell
    if not xw_pts:
        return {"ok": False, "reason": "no_xwoba"}

    sum_grid = _zeros_grid()
    cnt_grid = _zeros_grid()

    for x, z, xw in xw_pts:
        ix = _bin_index(x, X_MIN, X_MAX, GRID_NX)
        iz = _bin_index(z, Z_MIN, Z_MAX, GRID_NZ)
        if ix is None or iz is None:
            continue
        sum_grid[iz][ix] += float(xw)
        cnt_grid[iz][ix] += 1.0

    avg_grid = _zeros_grid()
    for iz in range(GRID_NZ):
        for ix in range(GRID_NX):
            c = cnt_grid[iz][ix]
            if c > 0:
                avg_grid[iz][ix] = sum_grid[iz][ix] / c
            else:
                avg_grid[iz][ix] = 0.0

    avg_blur = _blur_grid(avg_grid, GAUSS_SIGMA)
    out = _grid_payload(avg_blur)
    out.update({"ok": True, "metric": "xwoba", "pitch_type": pt, "stand_lr": side})
    return out


def pitching_scatter(
    pitcher_id: int,
    season: int,
    game_pk: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Return per-pitch location + movement data for scatter plots."""
    arr = _fetch_pitches(pitcher_id, season, game_pk=game_pk)
    out = []
    for r in arr:
        pt = (r.get("pitch_type") or "").strip()
        if not pt:
            continue
        hb = _safe_float(r.get("pfx_x"))
        vb = _safe_float(r.get("pfx_z"))
        out.append({
            "pitch_type": pt,
            "pitch_name": r.get("pitch_name") or pt,
            "stand": (r.get("stand") or "").upper(),
            "plate_x": _safe_float(r.get("plate_x")),
            "plate_z": _safe_float(r.get("plate_z")),
            "hb": (hb * 12.0) if hb is not None else None,
            "ivb": (vb * 12.0) if vb is not None else None,
        })
    return out


def pitching_gamelog(pitcher_id: int, season: int) -> List[Dict[str, Any]]:
    """Return game-by-game pitching stats for the given pitcher/season."""
    arr = _fetch_pitches(pitcher_id, season)
    if not arr:
        return []

    # Group pitches by game_pk
    games: Dict[int, List[Dict[str, Any]]] = {}
    for r in arr:
        gp = r.get("game_pk")
        if gp is not None:
            games.setdefault(int(gp), []).append(r)

    rows: List[Dict[str, Any]] = []
    for gp, pitches in sorted(games.items(), reverse=True):
        n = len(pitches)

        # Game date (take first non-null)
        game_date: Optional[str] = None
        for p in pitches:
            gd = p.get("game_date")
            if gd:
                game_date = str(gd)[:10]
                break

        # Velo
        velos = [_safe_float(p.get("release_speed")) for p in pitches]
        avg_velo = _mean(velos)

        # Stuff+ / Control+
        sp_vals = [_safe_float(p.get("stuff_plus")) for p in pitches]
        avg_stuff = _mean([v for v in sp_vals if v is not None])
        cp_vals = [_safe_float(p.get("control_plus")) for p in pitches]
        avg_control = _mean([v for v in cp_vals if v is not None])

        # Whiff / Chase / Zone
        swings = 0
        whiffs = 0
        in_zone = 0
        ooz = 0
        ooz_swings = 0

        for p in pitches:
            desc = (p.get("description") or "").lower().strip()
            zone_num = p.get("zone")

            if desc in SWING:
                swings += 1
            if desc in WHIFF:
                whiffs += 1

            if zone_num is not None:
                try:
                    zn = int(zone_num)
                    if zn in IN_ZONE:
                        in_zone += 1
                    elif zn in OUT_OF_ZONE:
                        ooz += 1
                        if desc in SWING:
                            ooz_swings += 1
                except (ValueError, TypeError):
                    pass

        # PA-level stats from events
        events = [str(p.get("events") or "").lower().strip() for p in pitches if p.get("events")]
        pa = len(events)
        k = sum(1 for e in events if e in K_EVENTS)
        bb = sum(1 for e in events if e in BB_EVENTS)

        # xwOBA
        xw_vals = [_safe_float(p.get("estimated_woba_using_speedangle")) for p in pitches]
        xwoba = _mean([v for v in xw_vals if v is not None])

        rows.append({
            "game_pk": gp,
            "game_date": game_date,
            "pitches": n,
            "avg_velo": avg_velo,
            "avg_stuff_plus": avg_stuff,
            "avg_control_plus": avg_control,
            "whiff_pct": (100.0 * whiffs / swings) if swings else None,
            "zone_pct": (100.0 * in_zone / n) if n else None,
            "chase_pct": (100.0 * ooz_swings / ooz) if ooz else None,
            "k_pct": (100.0 * k / pa) if pa else None,
            "bb_pct": (100.0 * bb / pa) if pa else None,
            "xwoba": xwoba,
        })

    return rows
