# services/pitching_report.py
from __future__ import annotations

import os
import math
from typing import Any, Dict, List, Optional, Tuple

import psycopg


# --- Fixed zone (matches your previous file constants) ---
ZONE_TOP = 3.3942716630565757
ZONE_BOT = 1.594602333802632
ZONE_LEFT = -0.83
ZONE_RIGHT = 0.83

# Heatmap domain (bigger than zone)
X_MIN, X_MAX = -2.0, 2.0
Z_MIN, Z_MAX = 0.0, 5.0

GRID_NX = 70
GRID_NZ = 70

GAUSS_SIGMA = 1.7  # smoothing

SWING_DESCRIPTIONS = {
    "swinging_strike",
    "swinging_strike_blocked",
    "foul",
    "foul_tip",
    "foul_bunt",
    "missed_bunt",
    "hit_into_play",
    "hit_into_play_no_out",
    "hit_into_play_score",
}
WHIFF_DESCRIPTIONS = {
    "swinging_strike",
    "swinging_strike_blocked",
    "missed_bunt",
}


# ----------------------------
# DB URL (match savant_profile style)
# ----------------------------
def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def _safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        f = float(x)
        if not math.isfinite(f):
            return None
        return f
    except Exception:
        return None


def _mean(nums: List[Optional[float]]) -> Optional[float]:
    vals = [x for x in nums if x is not None and math.isfinite(float(x))]
    if not vals:
        return None
    return float(sum(vals)) / float(len(vals))


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


def _grid_payload(grid: List[List[float]]) -> Dict[str, Any]:
    return {
        "nx": GRID_NX,
        "nz": GRID_NZ,
        "x_min": X_MIN,
        "x_max": X_MAX,
        "z_min": Z_MIN,
        "z_max": Z_MAX,
        "grid": grid,
        "zone": {
            "left": ZONE_LEFT,
            "right": ZONE_RIGHT,
            "top": ZONE_TOP,
            "bot": ZONE_BOT,
        },
    }


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
    IMPORTANT: These column names must exist in statcast_pitches.
    """
    sql = """
    SELECT
      game_pk,
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
      estimated_woba_using_speedangle
    FROM statcast_pitches
    WHERE pitcher = %s
      AND game_year = %s
    """
    params: List[Any] = [int(pitcher_id), int(season)]
    if game_pk:
        sql += " AND game_pk = %s"
        params.append(int(game_pk))

    with psycopg.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, r)) for r in rows]


# ----------------------------
# PUBLIC: matches app.py imports
# ----------------------------
def list_pitching_games(pitcher_id: int, season: int) -> List[Dict[str, Any]]:
    sql = """
    SELECT DISTINCT game_pk
    FROM statcast_pitches
    WHERE pitcher = %s
      AND game_year = %s
    ORDER BY game_pk DESC
    """
    with psycopg.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (int(pitcher_id), int(season)))
            rows = cur.fetchall()
            return [{"game_pk": int(r[0]), "label": str(r[0])} for r in rows if r and r[0] is not None]


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
    mix: List[Dict[str, Any]] = []
    shapes: List[Dict[str, Any]] = []
    pitches: List[Dict[str, Any]] = []

    for pt, rows in by_pt.items():
        n = len(rows)
        pitch_name = (rows[0].get("pitch_name") or pt) if rows else pt

        pitches.append({"pitch_type": pt, "pitch_name": pitch_name, "n": n})

        velo = [_safe_float(x.get("release_speed")) for x in rows]
        spin = [_safe_float(x.get("release_spin_rate")) for x in rows]
        hb = [_safe_float(x.get("pfx_x")) for x in rows]  # feet
        vb = [_safe_float(x.get("pfx_z")) for x in rows]  # feet

        swings = 0
        whiffs = 0
        xw_list: List[Optional[float]] = []

        for x in rows:
            desc = (x.get("description") or "").lower().strip()
            if desc in SWING_DESCRIPTIONS:
                swings += 1
            if desc in WHIFF_DESCRIPTIONS:
                whiffs += 1

            xw = _safe_float(x.get("estimated_woba_using_speedangle"))
            if xw is not None:
                xw_list.append(xw)

        whiff_pct = (whiffs / swings * 100.0) if swings else None

        mix.append(
            {
                "pitch_type": pt,
                "pitch_name": pitch_name,
                "n": n,
                "usage": (n / total * 100.0) if total else 0.0,
                "velo": _mean(velo),
                "spin": _mean(spin),
                "hb": _mean(hb),  # feet
                "vb": _mean(vb),  # feet
                "xwoba": _mean(xw_list),
                "whiff_pct": whiff_pct,
            }
        )

        shapes.append(
            {
                "pitch_type": pt,
                "pitch_name": pitch_name,
                "n": n,
                "pfx_x": _mean(hb),
                "pfx_z": _mean(vb),
                "velo": _mean(velo),
            }
        )

    pitches.sort(key=lambda x: x["n"], reverse=True)
    mix.sort(key=lambda x: x["n"], reverse=True)
    shapes.sort(key=lambda x: x["n"], reverse=True)

    return {
        "ok": True,
        "pitcher_id": int(pitcher_id),
        "season": int(season),
        "game_pk": int(game_pk) if game_pk else None,
        "total": total,
        "pitches": pitches,  # used by your front-end dropdown builder
        "mix": mix,
        "shapes": shapes,
        "zone": {"left": ZONE_LEFT, "right": ZONE_RIGHT, "top": ZONE_TOP, "bot": ZONE_BOT},
    }


def pitching_heatmap(
    pitcher_id: int,
    season: int,
    pitch_type: str,
    stand_lr: str = "L",
    metric: str = "density",
    game_pk: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Matches app.py call signature exactly:
      pitching_heatmap(pitcher_id=..., season=..., pitch_type=..., stand_lr=..., metric=..., game_pk=...)

    metric:
      - "density" => pitch location density
      - "xwoba"   => estimated_woba_using_speedangle mean per bin (blurred)
    """
    pt = (pitch_type or "").strip()
    side = (stand_lr or "L").upper().strip()
    m = (metric or "density").lower().strip()
    if not pt:
        return {"ok": False, "reason": "missing_pitch_type"}
    if side not in ("L", "R"):
        return {"ok": False, "reason": "bad_stand"}
    if m not in ("density", "xwoba"):
        return {"ok": False, "reason": "bad_metric"}

    arr = _fetch_pitches(pitcher_id, season, game_pk=game_pk)
    if not arr:
        return {"ok": False, "reason": "no_data"}

    if m == "density":
        pts: List[Tuple[float, float]] = []
        for r in arr:
            if (r.get("pitch_type") or "").strip() != pt:
                continue
            lr = _stand_lr(r.get("stand"), r.get("p_throws"))
            if lr != side:
                continue
            x = _safe_float(r.get("plate_x"))
            z = _safe_float(r.get("plate_z"))
            if x is None or z is None:
                continue
            pts.append((x, z))

        if not pts:
            return {"ok": False, "reason": "no_points"}

        H = _hist2d(pts)
        Hb = _blur_grid(H, GAUSS_SIGMA)

        mx = max((max(row) for row in Hb), default=0.0) or 1.0
        for iz in range(GRID_NZ):
            for ix in range(GRID_NX):
                Hb[iz][ix] = Hb[iz][ix] / mx

        out = _grid_payload(Hb)
        out.update({"ok": True, "metric": "density", "pitch_type": pt, "stand_lr": side})
        return out

    # m == "xwoba"
    sum_grid = _zeros_grid()
    cnt_grid = [[0 for _ in range(GRID_NX)] for __ in range(GRID_NZ)]

    for r in arr:
        if (r.get("pitch_type") or "").strip() != pt:
            continue
        lr = _stand_lr(r.get("stand"), r.get("p_throws"))
        if lr != side:
            continue

        x = _safe_float(r.get("plate_x"))
        z = _safe_float(r.get("plate_z"))
        xw = _safe_float(r.get("estimated_woba_using_speedangle"))
        if x is None or z is None or xw is None:
            continue

        ix = _bin_index(x, X_MIN, X_MAX, GRID_NX)
        iz = _bin_index(z, Z_MIN, Z_MAX, GRID_NZ)
        if ix is None or iz is None:
            continue

        sum_grid[iz][ix] += float(xw)
        cnt_grid[iz][ix] += 1

    avg_grid = _zeros_grid()
    any_cell = False
    for iz in range(GRID_NZ):
        for ix in range(GRID_NX):
            c = cnt_grid[iz][ix]
            if c:
                any_cell = True
                avg_grid[iz][ix] = sum_grid[iz][ix] / c
            else:
                avg_grid[iz][ix] = 0.0

    if not any_cell:
        return {"ok": False, "reason": "no_xwoba"}

    avg_blur = _blur_grid(avg_grid, GAUSS_SIGMA)
    out = _grid_payload(avg_blur)
    out.update({"ok": True, "metric": "xwoba", "pitch_type": pt, "stand_lr": side})
    return out
