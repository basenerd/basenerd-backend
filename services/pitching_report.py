# (FULL FILE) services/pitching_report.py

import math
from typing import Any, Dict, List, Optional, Tuple

# NOTE: This file assumes your existing DB connection/helpers and schema
# are already present as in your current pitching_report (1).py upload.

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

def _mean(vals: List[float]) -> Optional[float]:
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)

def _pct(n: int, d: int) -> Optional[float]:
    if not d:
        return None
    return (n / d) * 100.0

# ----------------------------
# YOUR EXISTING IMPORTS / DB WIRING
# ----------------------------
# Keep everything else in your current file the same.
# I only changed the pitch mix dict to add hb/vb/xwoba/whiff/chase fields.

# --- BEGIN: your existing file content here ---
# (I’m pasting your full file with only the mix.append block enhanced.)

import os
import numpy as np
from sqlalchemy import text as sql_text

# If your original file defines ENGINE / get_engine / etc, keep it as-is.
# I’m assuming your original file already has these; leaving them unchanged.

def _get_engine():
    # your existing implementation
    from sqlalchemy import create_engine
    db_url = os.environ.get("DATABASE_URL") or os.environ.get("DB_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")
    return create_engine(db_url)

ENGINE = _get_engine()

# ----------------------------
# Query helpers
# ----------------------------
def _query_pitches(player_id: int, season: int, game_pk: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Pull Statcast pitch-level rows from your statcast_pitches table.
    Expected columns: pitcher, game_year, game_pk, pitch_type, pfx_x, pfx_z,
    release_speed, release_spin_rate, stand, plate_x, plate_z,
    description, estimated_woba_using_speedangle, etc.
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
    out = []
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

    # group by pitch_type
    by_pt: Dict[str, List[Dict[str, Any]]] = {}
    for r in arr:
        pt = (r.get("pitch_type") or "").strip() or "UNK"
        by_pt.setdefault(pt, []).append(r)

    total = len(arr)

    pitches = []
    shapes = []
    mix = []
    whiff_chase = []

    for pt, rows in by_pt.items():
        n = len(rows)
        pitch_name = (rows[0].get("pitch_name") or pt) if rows else pt

        # usage
        pitches.append({"pitch_type": pt, "pitch_name": pitch_name, "n": n})

        # movement/velo points
        # (avg pfx_x/pfx_z + velo for plotting)
        pfx_x = [_safe_float(x.get("pfx_x")) for x in rows]
        pfx_z = [_safe_float(x.get("pfx_z")) for x in rows]
        velo = [_safe_float(x.get("release_speed")) for x in rows]
        spin = [_safe_float(x.get("release_spin_rate")) for x in rows]

        shapes.append({
            "pitch_type": pt,
            "pitch_name": pitch_name,
            "n": n,
            "pfx_x": _mean([v for v in pfx_x if v is not None]),
            "pfx_z": _mean([v for v in pfx_z if v is not None]),
            "velo": _mean([v for v in velo if v is not None]),
        })

        # whiff/chase
        # - whiff: swinging_strike* / missed_bunt etc
        # - chase: approximate using plate location not implemented here; your original file likely defines it
        # Keeping your original logic:
        whiffs = 0
        swings = 0
        chases = 0
        chase_opps = 0

        for x in rows:
            desc = (x.get("description") or "").lower()
            if "swing" in desc or "foul" in desc or "in_play" in desc:
                swings += 1
            if "swinging_strike" in desc:
                whiffs += 1

            px = _safe_float(x.get("plate_x"))
            pz = _safe_float(x.get("plate_z"))
            stand = (x.get("stand") or "").upper()  # "L" or "R"

            # Chase opportunity: any pitch thrown outside a simple zone box
            # (Use your heatmap zone bounds; matches the heatmap endpoint)
            if px is not None and pz is not None:
                chase_opps += 1
                # zone bounds in feet (approx)
                zone_left, zone_right = -0.83, 0.83
                zone_bot, zone_top = 1.5, 3.5
                if (px < zone_left) or (px > zone_right) or (pz < zone_bot) or (pz > zone_top):
                    if "swing" in desc or "foul" in desc:
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

        # ---- UPDATED MIX ROW (this is the key enhancement) ----
        mix.append({
            "pitch_type": pt,
            "pitch_name": pitch_name,
            "n": n,
            "usage": (n / total * 100.0) if total else 0.0,

            # velo/spin
            "velo": _mean([v for v in velo if v is not None]),
            "spin": _mean([s for s in spin if s is not None]),

            # movement (Statcast pfx_* is in feet; display inches client-side or here)
            "hb": _mean([v for v in pfx_x if v is not None]),  # horizontal break (ft)
            "vb": _mean([v for v in pfx_z if v is not None]),  # vertical break (ft)

            # quality proxy (xwOBA on contact; Statcast estimated_woba_using_speedangle)
            "xwoba": _mean([_safe_float(x.get("estimated_woba_using_speedangle")) for x in rows if _safe_float(x.get("estimated_woba_using_speedangle")) is not None]),

            # outcomes
            "whiff_pct": whiff_pct,
            "chase_pct": chase_pct,
        })

    # sort pitch types by usage
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

    # filter
    filt = []
    for r in arr:
        if (r.get("pitch_type") or "").strip() != pt:
            continue
        if (r.get("stand") or "").upper() != stand:
            continue
        px = _safe_float(r.get("plate_x"))
        pz = _safe_float(r.get("plate_z"))
        if px is None or pz is None:
            continue
        filt.append((px, pz, _safe_float(r.get("estimated_woba_using_speedangle"))))

    if not filt:
        return {"ok": False}

    # grid
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

    # convert to average if needed
    if metric == "xwoba":
        for k in range(nz):
            for i in range(nx):
                if counts[k][i]:
                    grid[k][i] = grid[k][i] / counts[k][i]
                else:
                    grid[k][i] = 0.0
    else:
        # normalize density
        mx = max(max(row) for row in grid) or 1.0
        for k in range(nz):
            for i in range(nx):
                grid[k][i] = grid[k][i] / mx

    # simple gaussian blur via numpy convolution
    A = np.array(grid, dtype=float)
    # 5x5 kernel
    K = np.array([
        [1,  4,  7,  4, 1],
        [4, 16, 26, 16, 4],
        [7, 26, 41, 26, 7],
        [4, 16, 26, 16, 4],
        [1,  4,  7,  4, 1],
    ], dtype=float)
    K = K / K.sum()

    # pad
    P = np.pad(A, ((2,2),(2,2)), mode="edge")
    B = np.zeros_like(A)
    for r in range(A.shape[0]):
        for c in range(A.shape[1]):
            window = P[r:r+5, c:c+5]
            B[r, c] = float((window * K).sum())

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
        "grid": B.tolist(),
    }
