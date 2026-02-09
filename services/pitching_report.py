# services/pitching_report.py
from __future__ import annotations

import os
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


# --- Fixed zone (user-specified) ---
ZONE_TOP = 3.3942716630565757
ZONE_BOT = 1.594602333802632
ZONE_LEFT = -0.83
ZONE_RIGHT = 0.83

# Canvas/grid domain (bigger than zone; tweak later if desired)
X_MIN, X_MAX = -2.0, 2.0
Z_MIN, Z_MAX = 0.0, 5.0

GRID_NX = 70
GRID_NZ = 70

# “KDE style”: blur histogram with a Gaussian kernel
GAUSS_SIGMA = 1.7

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


def _db_url() -> str:
    url = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or ""
    if not url:
        raise RuntimeError("DATABASE_URL not set")

    # force pg8000 driver to avoid psycopg binary issues on Render
    if url.startswith("postgres://"):
        url = "postgresql+pg8000://" + url[len("postgres://"):]
    elif url.startswith("postgresql://"):
        url = "postgresql+pg8000://" + url[len("postgresql://"):]
    return url


def _engine():
    from sqlalchemy import create_engine
    return create_engine(_db_url(), pool_pre_ping=True)


def _safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _stand_lr(stand: Optional[str], p_throws: Optional[str]) -> Optional[str]:
    """
    Split-only classification. NO coordinate flipping.
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


def _mean(nums: List[float]) -> Optional[float]:
    vals = [x for x in nums if x is not None and math.isfinite(x)]
    if not vals:
        return None
    return sum(vals) / len(vals)


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
    # map vmin..vmax onto 0..n-1
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
    """
    Returns grid shape (NZ rows, NX cols) where row is z-bin, col is x-bin.
    """
    H = _zeros_grid()
    for x, z in points:
        ix = _bin_index(x, X_MIN, X_MAX, GRID_NX)
        iz = _bin_index(z, Z_MIN, Z_MAX, GRID_NZ)
        if ix is None or iz is None:
            continue
        H[iz][ix] += 1.0
    return H


def _hist2d_weighted(points: List[Tuple[float, float, float]]) -> Tuple[List[List[float]], List[List[float]]]:
    """
    Returns (sum_grid, count_grid)
    """
    S = _zeros_grid()
    C = _zeros_grid()
    for x, z, w in points:
        ix = _bin_index(x, X_MIN, X_MAX, GRID_NX)
        iz = _bin_index(z, Z_MIN, Z_MAX, GRID_NZ)
        if ix is None or iz is None:
            continue
        if w is None or not math.isfinite(w):
            continue
        S[iz][ix] += float(w)
        C[iz][ix] += 1.0
    return S, C


def _gaussian_kernel_1d(sigma: float) -> List[float]:
    # radius ~ 3*sigma is standard
    radius = max(1, int(3.0 * sigma))
    kern = []
    for i in range(-radius, radius + 1):
        kern.append(math.exp(-(i * i) / (2.0 * sigma * sigma)))
    s = sum(kern) or 1.0
    return [k / s for k in kern]


def _convolve_1d_clamped(arr: List[float], kern: List[float]) -> List[float]:
    radius = len(kern) // 2
    n = len(arr)
    out = [0.0] * n
    for i in range(n):
        acc = 0.0
        for k, w in enumerate(kern):
            j = i + (k - radius)
            if j < 0:
                j = 0
            elif j >= n:
                j = n - 1
            acc += arr[j] * w
        out[i] = acc
    return out


def _blur(grid: List[List[float]], sigma: float) -> List[List[float]]:
    """
    Separable Gaussian blur: horizontal then vertical, clamped edges.
    """
    kern = _gaussian_kernel_1d(sigma)

    # horizontal pass
    tmp = []
    for row in grid:
        tmp.append(_convolve_1d_clamped(row, kern))

    # vertical pass
    out = _zeros_grid()
    radius = len(kern) // 2
    for z in range(GRID_NZ):
        for x in range(GRID_NX):
            acc = 0.0
            for k, w in enumerate(kern):
                zz = z + (k - radius)
                if zz < 0:
                    zz = 0
                elif zz >= GRID_NZ:
                    zz = GRID_NZ - 1
                acc += tmp[zz][x] * w
            out[z][x] = acc
    return out


def _normalize_max(grid: List[List[float]]) -> List[List[float]]:
    m = 0.0
    for r in grid:
        for v in r:
            if v > m:
                m = v
    if m <= 0:
        return grid
    return [[v / m for v in r] for r in grid]


def _divide_safe(num: List[List[float]], den: List[List[float]], den_eps: float = 1e-6) -> List[List[float]]:
    out = _zeros_grid()
    for z in range(GRID_NZ):
        for x in range(GRID_NX):
            d = den[z][x]
            out[z][x] = (num[z][x] / d) if d > den_eps else 0.0
    return out


def _clip(grid: List[List[float]], lo: float, hi: float) -> List[List[float]]:
    out = _zeros_grid()
    for z in range(GRID_NZ):
        for x in range(GRID_NX):
            v = grid[z][x]
            if v < lo:
                v = lo
            elif v > hi:
                v = hi
            out[z][x] = v
    return out


def _query_pitches(pitcher_id: int, season: int, game_pk: Optional[int] = None) -> List[Dict[str, Any]]:
    eng = _engine()
    where_game = ""
    params: Dict[str, Any] = {"pitcher": pitcher_id, "season": season}
    if game_pk:
        where_game = " AND game_pk = :game_pk"
        params["game_pk"] = game_pk

    sql = f"""
    SELECT
      pitch_type, pitch_name,
      plate_x, plate_z,
      pfx_x, pfx_z,
      release_speed, release_spin_rate,
      zone, description,
      stand, p_throws,
      estimated_woba_using_speedangle,
      game_pk, game_date, home_team, away_team, inning_topbot
    FROM statcast_pitches
    WHERE pitcher = :pitcher
      AND game_year = :season
      {where_game}
    """
    rows: List[Dict[str, Any]] = []
    from sqlalchemy import text
    with eng.connect() as conn:
        res = conn.execute(text(sql), params)
        for r in res.mappings():
            rows.append(dict(r))
    return rows


def list_pitching_games(pitcher_id: int, season: int) -> List[Dict[str, Any]]:
    rows = _query_pitches(pitcher_id, season, game_pk=None)
    by_game: Dict[int, Dict[str, Any]] = {}

    for r in rows:
        gpk = r.get("game_pk")
        if not gpk:
            continue
        if gpk in by_game:
            continue

        game_date = r.get("game_date")
        if isinstance(game_date, str):
            d = game_date[:10]
        else:
            try:
                d = game_date.strftime("%Y-%m-%d")
            except Exception:
                d = ""

        home = (r.get("home_team") or "").upper()
        away = (r.get("away_team") or "").upper()
        topbot = (r.get("inning_topbot") or "").lower()

        # if pitching in top, you're home team pitcher
        is_home_pitcher = (topbot == "top")
        opp = away if is_home_pitcher else home
        loc = "vs." if is_home_pitcher else "@"

        label = ""
        try:
            mmddyyyy = datetime.strptime(d, "%Y-%m-%d").strftime("%m-%d-%Y")
            label = f"{mmddyyyy} {loc} {opp}"
        except Exception:
            label = f"{d} {loc} {opp}".strip()

        by_game[int(gpk)] = {
            "game_pk": int(gpk),
            "label": label,
            "date": d,
            "home": home,
            "away": away,
        }

    return sorted(by_game.values(), key=lambda x: x.get("date") or "", reverse=True)


def pitching_report_summary(pitcher_id: int, season: int, game_pk: Optional[int]) -> Dict[str, Any]:
    rows = _query_pitches(pitcher_id, season, game_pk=game_pk)
    total = len(rows)
    if total == 0:
        return {"ok": False, "reason": "no_pitches", "pitcher_id": pitcher_id, "season": season, "game_pk": game_pk}

    by_pt: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        pt = (r.get("pitch_type") or "UNK") or "UNK"
        by_pt.setdefault(pt, []).append(r)

    pitch_list = []
    mix = []
    whiff_chase = []
    shapes = []

    for pt, arr in sorted(by_pt.items(), key=lambda kv: len(kv[1]), reverse=True):
        n = len(arr)
        pitch_name = None
        for x in arr:
            if x.get("pitch_name"):
                pitch_name = x.get("pitch_name")
                break
        pitch_name = pitch_name or pt

        velo = [_safe_float(x.get("release_speed")) for x in arr]
        spin = [_safe_float(x.get("release_spin_rate")) for x in arr]
        pfx_x = [_safe_float(x.get("pfx_x")) for x in arr]
        pfx_z = [_safe_float(x.get("pfx_z")) for x in arr]

        desc = [(x.get("description") or "").lower() for x in arr]
        zones = [x.get("zone") for x in arr]

        swings = 0
        whiffs = 0
        out_zone_p = 0
        chase_swings = 0

        for d, z in zip(desc, zones):
            is_swing = d in SWING_DESCRIPTIONS
            is_whiff = d in WHIFF_DESCRIPTIONS
            try:
                zi = int(z) if z is not None else None
            except Exception:
                zi = None
            in_zone = (zi is not None and 1 <= zi <= 9)
            if not in_zone:
                out_zone_p += 1
                if is_swing:
                    chase_swings += 1
            if is_swing:
                swings += 1
                if is_whiff:
                    whiffs += 1

        whiff_pct = (whiffs / swings * 100.0) if swings else 0.0
        chase_pct = (chase_swings / out_zone_p * 100.0) if out_zone_p else 0.0

        pitch_list.append({"pitch_type": pt, "pitch_name": pitch_name, "n": n})

        mix.append({
            "pitch_type": pt,
            "pitch_name": pitch_name,
            "n": n,
            "usage": (n / total * 100.0) if total else 0.0,
            "velo": _mean([v for v in velo if v is not None]),
            "spin": _mean([s for s in spin if s is not None]),
        })

        whiff_chase.append({
            "pitch_type": pt,
            "pitch_name": pitch_name,
            "n": n,
            "swings": swings,
            "whiffs": whiffs,
            "whiff_pct": whiff_pct,
            "out_zone_p": out_zone_p,
            "chase_swings": chase_swings,
            "chase_pct": chase_pct,
        })

        shapes.append({
            "pitch_type": pt,
            "pitch_name": pitch_name,
            "n": n,
            "pfx_x": _mean([v for v in pfx_x if v is not None]),
            "pfx_z": _mean([v for v in pfx_z if v is not None]),
            "velo": _mean([v for v in velo if v is not None]),
        })

    return {
        "ok": True,
        "pitcher_id": pitcher_id,
        "season": season,
        "game_pk": game_pk,
        "total_pitches": total,
        "pitches": pitch_list,
        "mix": mix,
        "whiff_chase": whiff_chase,
        "shapes": shapes,
        "zone": {"left": ZONE_LEFT, "right": ZONE_RIGHT, "top": ZONE_TOP, "bot": ZONE_BOT},
        "domain": {"x_min": X_MIN, "x_max": X_MAX, "z_min": Z_MIN, "z_max": Z_MAX},
    }


def pitching_heatmap(
    pitcher_id: int,
    season: int,
    pitch_type: str,
    stand_lr: str,   # "L" or "R"
    metric: str,     # "density" | "xwoba" | "whiff" | "chase"
    game_pk: Optional[int] = None
) -> Dict[str, Any]:
    rows = _query_pitches(pitcher_id, season, game_pk=game_pk)
    if not rows:
        return {"ok": False, "reason": "no_pitches"}

    # filter pitch type (no bucketing)
    rows = [r for r in rows if (r.get("pitch_type") or "UNK") == pitch_type]
    if not rows:
        return {"ok": False, "reason": "no_pitch_type"}

    # split vs L/R using stand->LR mapping (NO coordinate flipping)
    filtered: List[Dict[str, Any]] = []
    for r in rows:
        lr = _stand_lr(r.get("stand"), r.get("p_throws"))
        if lr == stand_lr:
            filtered.append(r)
    if not filtered:
        return {"ok": False, "reason": "no_split"}

    pts_xz: List[Tuple[float, float]] = []
    for r in filtered:
        x = _safe_float(r.get("plate_x"))
        z = _safe_float(r.get("plate_z"))
        if x is None or z is None:
            continue
        pts_xz.append((x, z))
    if not pts_xz:
        return {"ok": False, "reason": "no_locations"}

    if metric == "density":
        H = _hist2d(pts_xz)
        S = _blur(H, GAUSS_SIGMA)
        S = _normalize_max(S)
        return {"ok": True, "metric": metric, "stand": stand_lr, "pitch_type": pitch_type, **_grid_payload(S)}

    if metric == "xwoba":
        pts = []
        for r in filtered:
            x = _safe_float(r.get("plate_x"))
            z = _safe_float(r.get("plate_z"))
            w = _safe_float(r.get("estimated_woba_using_speedangle"))
            if x is None or z is None or w is None:
                continue
            pts.append((x, z, w))
        if not pts:
            return {"ok": False, "reason": "no_xwoba"}
        Ssum, Scnt = _hist2d_weighted(pts)
        Ssum_b = _blur(Ssum, GAUSS_SIGMA)
        Scnt_b = _blur(Scnt, GAUSS_SIGMA)
        avg = _divide_safe(Ssum_b, Scnt_b)
        avg = _clip(avg, 0.0, 1.2)
        return {"ok": True, "metric": metric, "stand": stand_lr, "pitch_type": pitch_type, **_grid_payload(avg)}

    # build swing/whiff arrays aligned with locations
    swing_pts: List[Tuple[float, float]] = []
    whiff_pts: List[Tuple[float, float]] = []
    out_pts: List[Tuple[float, float]] = []
    chase_pts: List[Tuple[float, float]] = []

    for r in filtered:
        x = _safe_float(r.get("plate_x"))
        z = _safe_float(r.get("plate_z"))
        if x is None or z is None:
            continue

        d = (r.get("description") or "").lower()
        is_swing = d in SWING_DESCRIPTIONS
        is_whiff = d in WHIFF_DESCRIPTIONS

        # in-zone by Statcast zone 1..9
        try:
            zi = int(r.get("zone")) if r.get("zone") is not None else None
        except Exception:
            zi = None
        in_zone = (zi is not None and 1 <= zi <= 9)
        out_zone = not in_zone

        if is_swing:
            swing_pts.append((x, z))
            if is_whiff:
                whiff_pts.append((x, z))

        if out_zone:
            out_pts.append((x, z))
            if is_swing:
                chase_pts.append((x, z))

    if metric == "whiff":
        Hsw = _hist2d(swing_pts) if swing_pts else _zeros_grid()
        Hwh = _hist2d(whiff_pts) if whiff_pts else _zeros_grid()
        Ssw = _blur(Hsw, GAUSS_SIGMA)
        Swh = _blur(Hwh, GAUSS_SIGMA)
        rate = _divide_safe(Swh, Ssw)
        rate = _clip(rate, 0.0, 1.0)
        return {"ok": True, "metric": metric, "stand": stand_lr, "pitch_type": pitch_type, **_grid_payload(rate)}

    if metric == "chase":
        Hout = _hist2d(out_pts) if out_pts else _zeros_grid()
        Hch = _hist2d(chase_pts) if chase_pts else _zeros_grid()
        Sout = _blur(Hout, GAUSS_SIGMA)
        Sch = _blur(Hch, GAUSS_SIGMA)
        rate = _divide_safe(Sch, Sout)
        rate = _clip(rate, 0.0, 1.0)
        return {"ok": True, "metric": metric, "stand": stand_lr, "pitch_type": pitch_type, **_grid_payload(rate)}

    return {"ok": False, "reason": "bad_metric"}
