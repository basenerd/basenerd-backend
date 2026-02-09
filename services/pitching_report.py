# services/pitching_report.py
import os
from typing import Any, Dict, List, Optional
from sqlalchemy import text as sql_text


# ----------------------------
# DB
# ----------------------------
def _get_engine():
    from sqlalchemy import create_engine
    db_url = 'postgresql://basenerd_user:d5LmELIOiEszYPBSLSDT1oIi79gkgDV6@dpg-d5i0tku3jp1c73f1d3gg-a.oregon-postgres.render.com/basenerd?sslmode=require'
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")
    return create_engine(db_url)

ENGINE = _get_engine()


# ----------------------------
# Small helpers
# ----------------------------
def _safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def _mean(vals: List[Optional[float]]) -> Optional[float]:
    vv = [v for v in vals if v is not None]
    if not vv:
        return None
    return sum(vv) / len(vv)

def _pct(n: int, d: int) -> Optional[float]:
    if not d:
        return None
    return (n / d) * 100.0


# ----------------------------
# Pure-Python Gaussian blur
# ----------------------------
_GAUSS5 = [
    [1,  4,  7,  4, 1],
    [4, 16, 26, 16, 4],
    [7, 26, 41, 26, 7],
    [4, 16, 26, 16, 4],
    [1,  4,  7,  4, 1],
]
_GAUSS5_SUM = float(sum(sum(r) for r in _GAUSS5))

def _blur5(grid: List[List[float]]) -> List[List[float]]:
    """
    5x5 gaussian blur, edge-padded, pure Python.
    grid is [nz][nx]
    """
    if not grid:
        return grid
    nz = len(grid)
    nx = len(grid[0]) if nz else 0
    if nx == 0:
        return grid

    def clamp(v: int, lo: int, hi: int) -> int:
        return lo if v < lo else hi if v > hi else v

    out = [[0.0 for _ in range(nx)] for _ in range(nz)]

    for z in range(nz):
        for x in range(nx):
            acc = 0.0
            for kz in range(-2, 3):
                zz = clamp(z + kz, 0, nz - 1)
                row = grid[zz]
                krow = _GAUSS5[kz + 2]
                for kx in range(-2, 3):
                    xx = clamp(x + kx, 0, nx - 1)
                    acc += row[xx] * krow[kx + 2]
            out[z][x] = acc / _GAUSS5_SUM
    return out


# ----------------------------
# Query helpers
# ----------------------------
def _query_pitches(player_id: int, season: int, game_pk: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Pull Statcast pitch-level rows from your statcast_pitches table.
    Expected columns: pitcher, game_year, game_pk, pitch_type, pitch_name, pfx_x, pfx_z,
    release_speed, release_spin_rate, stand, plate_x, plate_z,
    description, estimated_woba_using_speedangle
    """
    q = """
    SELECT
      pitch_type,
      pitch_name,
      release_speed,
      release_spin_rate,
      pfx_x,
      pfx_z,
      stand,
      plate_x,
      plate_z,
      description,
      estimated_woba_using_speedangle
    FROM statcast_pitches
    WHERE pitcher = :pid
      AND game_year = :season
    """
    params = {"pid": int(player_id), "season": int(season)}
    if game_pk:
        q += " AND game_pk = :gpk"
        params["gpk"] = int(game_pk)

    with ENGINE.connect() as conn:
        rows = conn.execute(sql_text(q), params).mappings().all()
        return [dict(r) for r in rows]


def pitching_games(player_id: int, season: int) -> List[Dict[str, Any]]:
    q = """
    SELECT DISTINCT game_pk
    FROM statcast_pitches
    WHERE pitcher = :pid
      AND game_year = :season
    ORDER BY game_pk DESC
    """
    params = {"pid": int(player_id), "season": int(season)}
    out: List[Dict[str, Any]] = []
    with ENGINE.connect() as conn:
        rows = conn.execute(sql_text(q), params).mappings().all()
        for r in rows:
            gpk = r.get("game_pk")
            if gpk:
                out.append({"game_pk": int(gpk), "label": str(gpk)})
    return out


# ----------------------------
# Main report summary
# ----------------------------
def pitching_report_summary(player_id: int, season: int, game_pk: Optional[int] = None) -> Dict[str, Any]:
    arr = _query_pitches(player_id, season, game_pk=game_pk)
    if not arr:
        return {"ok": False, "player_id": player_id, "season": season, "game_pk": game_pk}

    by_pt: Dict[str, List[Dict[str, Any]]] = {}
    for r in arr:
        pt = (r.get("pitch_type") or "").strip() or "UNK"
        by_pt.setdefault(pt, []).append(r)

    total = len(arr)

    pitches: List[Dict[str, Any]] = []
    shapes: List[Dict[str, Any]] = []
    mix: List[Dict[str, Any]] = []
    whiff_chase: List[Dict[str, Any]] = []

    for pt, rows in by_pt.items():
        n = len(rows)
        pitch_name = (rows[0].get("pitch_name") or pt) if rows else pt

        pitches.append({"pitch_type": pt, "pitch_name": pitch_name, "n": n})

        pfx_x = [_safe_float(x.get("pfx_x")) for x in rows]
        pfx_z = [_safe_float(x.get("pfx_z")) for x in rows]
        velo = [_safe_float(x.get("release_speed")) for x in rows]
        spin = [_safe_float(x.get("release_spin_rate")) for x in rows]

        shapes.append({
            "pitch_type": pt,
            "pitch_name": pitch_name,
            "n": n,
            "pfx_x": _mean(pfx_x),
            "pfx_z": _mean(pfx_z),
            "velo": _mean(velo),
        })

        whiffs = 0
        swings = 0
        chases = 0
        chase_opps = 0

        zone_left, zone_right = -0.83, 0.83
        zone_bot, zone_top = 1.5, 3.5

        for x in rows:
            desc = (x.get("description") or "").lower()

            is_swing = ("swing" in desc) or ("foul" in desc) or ("in_play" in desc)
            if is_swing:
                swings += 1
            if "swinging_strike" in desc:
                whiffs += 1

            px = _safe_float(x.get("plate_x"))
            pz = _safe_float(x.get("plate_z"))
            if px is not None and pz is not None:
                chase_opps += 1
                out_of_zone = (px < zone_left) or (px > zone_right) or (pz < zone_bot) or (pz > zone_top)
                if out_of_zone and is_swing:
                    chases += 1

        whiff_pct = _pct(whiffs, swings) if swings else None
        chase_pct = _pct(chases, chase_opps) if chase_opps else None

        whiff_chase.append({
            "pitch_type": pt,
            "pitch_name": pitch_name,
            "n": n,
            "whiff_pct": whiff_pct,
            "chase_pct": chase_pct,
        })

        # estimated_woba_using_speedangle as xwOBA proxy
        xw_list: List[Optional[float]] = []
        for x in rows:
            v = _safe_float(x.get("estimated_woba_using_speedangle"))
            if v is not None:
                xw_list.append(v)

        mix.append({
            "pitch_type": pt,
            "pitch_name": pitch_name,
            "n": n,
            "usage": (n / total * 100.0) if total else 0.0,
            "velo": _mean(velo),
            "spin": _mean(spin),
            "hb": _mean(pfx_x),   # feet; convert to inches client-side
            "vb": _mean(pfx_z),   # feet; convert to inches client-side
            "xwoba": _mean(xw_list),
            "whiff_pct": whiff_pct,
            "chase_pct": chase_pct,
        })

    pitches.sort(key=lambda x: x["n"], reverse=True)
    mix.sort(key=lambda x: x["n"], reverse=True)
    shapes.sort(key=lambda x: x["n"], reverse=True)

    return {
        "ok": True,
        "player_id": int(player_id),
        "season": int(season),
        "game_pk": int(game_pk) if game_pk else None,
        "total": total,
        "pitches": pitches,
        "mix": mix,
        "shapes": shapes,
        "whiff_chase": whiff_chase,
    }


# ----------------------------
# Heatmap endpoint helper
# ----------------------------
def pitching_heatmap(
    player_id: int,
    season: int,
    pitch_type: str,
    stand: str = "L",
    game_pk: Optional[int] = None,
    metric: str = "density",
    nx: int = 38,
    nz: int = 48,
    x_min: float = -1.9,
    x_max: float = 1.9,
    z_min: float = 0.0,
    z_max: float = 5.0,
) -> Dict[str, Any]:
    """
    Returns a smoothed 2D grid for density or xwOBA.
    metric: "density" | "xwoba"
    """
    arr = _query_pitches(player_id, season, game_pk=game_pk)
    if not arr:
        return {"ok": False}

    stand = (stand or "L").upper()
    pt = (pitch_type or "").strip()
    if not pt:
        return {"ok": False}

    filt: List[tuple] = []
    for r in arr:
        if (r.get("pitch_type") or "").strip() != pt:
            continue
        if (r.get("stand") or "").upper() != stand:
            continue
        px = _safe_float(r.get("plate_x"))
        pz = _safe_float(r.get("plate_z"))
        if px is None or pz is None:
            continue
        xw = _safe_float(r.get("estimated_woba_using_speedangle"))
        filt.append((px, pz, xw))

    if not filt:
        return {"ok": False}

    grid = [[0.0 for _ in range(nx)] for _ in range(nz)]
    counts = [[0 for _ in range(nx)] for _ in range(nz)]

    def ix(x): return int((x - x_min) / (x_max - x_min) * (nx - 1))
    def iz(z): return int((z - z_min) / (z_max - z_min) * (nz - 1))

    for px, pz, xw in filt:
        i = ix(px); k = iz(pz)
        if i < 0 or i >= nx or k < 0 or k >= nz:
            continue
        if metric == "xwoba":
            if xw is None:
                continue
            grid[k][i] += float(xw)
            counts[k][i] += 1
        else:
            grid[k][i] += 1.0
            counts[k][i] += 1

    if metric == "xwoba":
        for k in range(nz):
            for i in range(nx):
                if counts[k][i]:
                    grid[k][i] = grid[k][i] / counts[k][i]
                else:
                    grid[k][i] = 0.0
    else:
        mx = max(max(row) for row in grid) or 1.0
        for k in range(nz):
            for i in range(nx):
                grid[k][i] = grid[k][i] / mx

    # blur (pure python)
    blurred = _blur5(grid)

    zone = {"left": -0.83, "right": 0.83, "bot": 1.5, "top": 3.5}

    return {
        "ok": True,
        "metric": metric,
        "nx": nx,
        "nz": nz,
        "x_min": x_min,
        "x_max": x_max,
        "z_min": z_min,
        "z_max": z_max,
        "zone": zone,
        "grid": blurred,
    }
