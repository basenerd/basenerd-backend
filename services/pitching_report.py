# services/pitching_report.py
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from scipy.ndimage import gaussian_filter

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
GAUSS_SIGMA = 1.7  # "KDE-style smooth" via gaussian blur on histogram

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
    """
    Use Render-style DATABASE_URL if present.
    We try to be compatible with pg8000 (pure python).
    """
    url = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or ""
    if not url:
        raise RuntimeError("DATABASE_URL not set")

    # Render often uses postgres:// which SQLAlchemy deprecates
    if url.startswith("postgres://"):
        url = "postgresql+pg8000://" + url[len("postgres://"):]
    elif url.startswith("postgresql://"):
        # force pg8000 to avoid psycopg binary issues
        url = "postgresql+pg8000://" + url[len("postgresql://"):]
    return url

def _engine():
    # SQLAlchemy is already in your project (you have stack traces referencing it).
    from sqlalchemy import create_engine
    return create_engine(_db_url(), pool_pre_ping=True)

def _stand_lr(stand: Optional[str], p_throws: Optional[str]) -> Optional[str]:
    if not stand:
        return None
    s = stand.upper()
    if s in ("L", "R"):
        return s
    if s == "S":
        # Split-only classification (NO coordinate flipping)
        if (p_throws or "").upper() == "R":
            return "L"
        if (p_throws or "").upper() == "L":
            return "R"
    return None

def _safe_float(x):
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def _hist2d(x: np.ndarray, z: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Returns:
      H: shape (NZ, NX) (row = z, col = x)
      xedges, zedges
    """
    H, xedges, zedges = np.histogram2d(
        x, z,
        bins=[GRID_NX, GRID_NZ],
        range=[[X_MIN, X_MAX], [Z_MIN, Z_MAX]],
    )
    # histogram2d returns shape (NX, NZ) with first dim = x bins, second = z bins
    # We want (NZ, NX) for easier "image" mapping
    H = H.T
    return H, xedges, zedges

def _smooth(H: np.ndarray) -> np.ndarray:
    return gaussian_filter(H, sigma=GAUSS_SIGMA, mode="nearest")

def _grid_payload(grid: np.ndarray) -> Dict[str, Any]:
    # serialize as nested lists; keep it lightweight
    return {
        "nx": GRID_NX,
        "nz": GRID_NZ,
        "x_min": X_MIN,
        "x_max": X_MAX,
        "z_min": Z_MIN,
        "z_max": Z_MAX,
        "grid": grid.tolist(),
        "zone": {
            "left": ZONE_LEFT,
            "right": ZONE_RIGHT,
            "top": ZONE_TOP,
            "bot": ZONE_BOT,
        },
    }

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
    with eng.connect() as conn:
        res = conn.execute(__import__("sqlalchemy").text(sql), params)  # text() without import shadowing
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
        # DB sometimes stores as date or string
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

        # Infer whether pitcher is home team (pitching in top = home fields)
        is_home_pitcher = (topbot == "top")
        opp = away if is_home_pitcher else home
        loc = "vs." if is_home_pitcher else "@"

        # label: MM-DD-YYYY vs./@ OPP
        label = ""
        try:
            mmddyyyy = datetime.strptime(d, "%Y-%m-%d").strftime("%m-%d-%Y")
            label = f"{mmddyyyy} {loc} {opp}"
        except Exception:
            label = f"{d} {loc} {opp}".strip()

        by_game[gpk] = {
            "game_pk": int(gpk),
            "label": label,
            "date": d,
            "home": home,
            "away": away,
        }

    # sort desc by date string
    games = sorted(by_game.values(), key=lambda x: x.get("date") or "", reverse=True)
    return games

def pitching_report_summary(pitcher_id: int, season: int, game_pk: Optional[int]) -> Dict[str, Any]:
    rows = _query_pitches(pitcher_id, season, game_pk=game_pk)

    total = len(rows)
    if total == 0:
        return {"ok": False, "reason": "no_pitches", "pitcher_id": pitcher_id, "season": season, "game_pk": game_pk}

    # Group by pitch_type (no bucketing)
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
        pitch_name = next((x.get("pitch_name") for x in arr if x.get("pitch_name")), pt)

        # arrays for means
        velo = np.array([_safe_float(x.get("release_speed")) for x in arr], dtype=float)
        spin = np.array([_safe_float(x.get("release_spin_rate")) for x in arr], dtype=float)
        pfx_x = np.array([_safe_float(x.get("pfx_x")) for x in arr], dtype=float)
        pfx_z = np.array([_safe_float(x.get("pfx_z")) for x in arr], dtype=float)

        def nanmean(a):
            a = a[np.isfinite(a)]
            return float(a.mean()) if a.size else None

        # whiff/chase
        desc = [(x.get("description") or "").lower() for x in arr]
        zone = [x.get("zone") for x in arr]
        is_swing = np.array([d in SWING_DESCRIPTIONS for d in desc], dtype=bool)
        is_whiff = np.array([d in WHIFF_DESCRIPTIONS for d in desc], dtype=bool)
        # in-zone defined by Statcast zone 1-9
        in_zone = np.array([(z is not None and 1 <= int(z) <= 9) for z in zone], dtype=bool)
        out_zone = ~in_zone

        swings = int(is_swing.sum())
        whiffs = int((is_swing & is_whiff).sum())
        out_p = int(out_zone.sum())
        chase_sw = int((out_zone & is_swing).sum())

        whiff_pct = (whiffs / swings * 100.0) if swings else 0.0
        chase_pct = (chase_sw / out_p * 100.0) if out_p else 0.0

        pitch_list.append({"pitch_type": pt, "pitch_name": pitch_name, "n": n})

        mix.append({
            "pitch_type": pt,
            "pitch_name": pitch_name,
            "n": n,
            "usage": (n / total * 100.0) if total else 0.0,
            "velo": nanmean(velo),
            "spin": nanmean(spin),
        })

        whiff_chase.append({
            "pitch_type": pt,
            "pitch_name": pitch_name,
            "n": n,
            "swings": swings,
            "whiffs": whiffs,
            "whiff_pct": whiff_pct,
            "out_zone_p": out_p,
            "chase_swings": chase_sw,
            "chase_pct": chase_pct,
        })

        shapes.append({
            "pitch_type": pt,
            "pitch_name": pitch_name,
            "n": n,
            "pfx_x": nanmean(pfx_x),
            "pfx_z": nanmean(pfx_z),
            "velo": nanmean(velo),
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

    # Filter pitch type
    rows = [r for r in rows if (r.get("pitch_type") or "UNK") == pitch_type]
    if not rows:
        return {"ok": False, "reason": "no_pitch_type"}

    # Split vs L/R using stand (S mapped via p_throws) but do NOT flip coordinates
    filtered = []
    for r in rows:
        lr = _stand_lr(r.get("stand"), r.get("p_throws"))
        if lr == stand_lr:
            filtered.append(r)

    if not filtered:
        return {"ok": False, "reason": "no_split"}

    x = np.array([_safe_float(r.get("plate_x")) for r in filtered], dtype=float)
    z = np.array([_safe_float(r.get("plate_z")) for r in filtered], dtype=float)

    ok = np.isfinite(x) & np.isfinite(z)
    x = x[ok]
    z = z[ok]

    if x.size == 0:
        return {"ok": False, "reason": "no_locations"}

    # Base histogram
    H, _, _ = _hist2d(x, z)

    if metric == "density":
        S = _smooth(H)
        mx = float(S.max()) if S.size else 1.0
        if mx > 0:
            S = S / mx
        return {"ok": True, "metric": metric, "stand": stand_lr, "pitch_type": pitch_type, **_grid_payload(S)}

    # xwOBA
    if metric == "xwoba":
        w = np.array([_safe_float(r.get("estimated_woba_using_speedangle")) for r in filtered], dtype=float)
        w = w[ok]  # align with x/z filtering
        w_ok = np.isfinite(w)
        # per-bin sum and count
        Hsum, _, _ = np.histogram2d(
            x[w_ok], z[w_ok],
            bins=[GRID_NX, GRID_NZ],
            range=[[X_MIN, X_MAX], [Z_MIN, Z_MAX]],
            weights=w[w_ok],
        )
        Hsum = Hsum.T
        Hcnt, _, _ = np.histogram2d(
            x[w_ok], z[w_ok],
            bins=[GRID_NX, GRID_NZ],
            range=[[X_MIN, X_MAX], [Z_MIN, Z_MAX]],
        )
        Hcnt = Hcnt.T

        Ssum = _smooth(Hsum)
        Scnt = _smooth(Hcnt)
        with np.errstate(divide="ignore", invalid="ignore"):
            avg = np.where(Scnt > 1e-6, Ssum / Scnt, np.nan)

        # clip to reasonable range for display
        avg = np.clip(avg, 0.0, 1.2)
        # Replace nans with 0 for rendering; frontend will mask low-weight if desired later
        avg = np.nan_to_num(avg, nan=0.0)
        return {"ok": True, "metric": metric, "stand": stand_lr, "pitch_type": pitch_type, **_grid_payload(avg)}

    # whiff% by location
    desc = np.array([(r.get("description") or "").lower() for r in filtered])
    is_swing = np.array([d in SWING_DESCRIPTIONS for d in desc], dtype=bool)
    is_whiff = np.array([d in WHIFF_DESCRIPTIONS for d in desc], dtype=bool)

    if metric == "whiff":
        # whiffs / swings in each bin (smooth numerator and denom)
        xs = x[is_swing[ok]]
        zs = z[is_swing[ok]]
        xw = x[(is_swing & is_whiff)[ok]]
        zw = z[(is_swing & is_whiff)[ok]]

        Hsw, _, _ = np.histogram2d(xs, zs, bins=[GRID_NX, GRID_NZ], range=[[X_MIN, X_MAX], [Z_MIN, Z_MAX]])
        Hsw = Hsw.T
        Hwh, _, _ = np.histogram2d(xw, zw, bins=[GRID_NX, GRID_NZ], range=[[X_MIN, X_MAX], [Z_MIN, Z_MAX]])
        Hwh = Hwh.T

        Ssw = _smooth(Hsw)
        Swh = _smooth(Hwh)
        with np.errstate(divide="ignore", invalid="ignore"):
            rate = np.where(Ssw > 1e-6, Swh / Ssw, 0.0)
        rate = np.clip(rate, 0.0, 1.0)
        return {"ok": True, "metric": metric, "stand": stand_lr, "pitch_type": pitch_type, **_grid_payload(rate)}

    # chase% by location (out-of-zone swings / out-of-zone pitches)
    if metric == "chase":
        zone = np.array([r.get("zone") for r in filtered])
        in_zone = np.array([(zv is not None and 1 <= int(zv) <= 9) for zv in zone], dtype=bool)
        out_zone = ~in_zone

        x_out = x[out_zone[ok]]
        z_out = z[out_zone[ok]]
        x_ch = x[(out_zone & is_swing)[ok]]
        z_ch = z[(out_zone & is_swing)[ok]]

        Hout, _, _ = np.histogram2d(x_out, z_out, bins=[GRID_NX, GRID_NZ], range=[[X_MIN, X_MAX], [Z_MIN, Z_MAX]])
        Hout = Hout.T
        Hch, _, _ = np.histogram2d(x_ch, z_ch, bins=[GRID_NX, GRID_NZ], range=[[X_MIN, X_MAX], [Z_MIN, Z_MAX]])
        Hch = Hch.T

        Sout = _smooth(Hout)
        Sch = _smooth(Hch)
        with np.errstate(divide="ignore", invalid="ignore"):
            rate = np.where(Sout > 1e-6, Sch / Sout, 0.0)
        rate = np.clip(rate, 0.0, 1.0)
        return {"ok": True, "metric": metric, "stand": stand_lr, "pitch_type": pitch_type, **_grid_payload(rate)}

    return {"ok": False, "reason": "bad_metric"}
