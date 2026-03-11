#!/usr/bin/env python3
"""
Generate a post-game pitcher report PDF (Twitter-optimized 1080x1350 portrait).

Pulls pitch data directly from the MLB Stats API live feed — no database needed.
Scores BNStuff+ and BNControl+ on-the-fly using local model .pkl files.

Usage:
    python scripts/generate_pitcher_report_pdf.py --pitcher_id 684007 --game_pk 831781
    python scripts/generate_pitcher_report_pdf.py --date 2026-02-24
    python scripts/generate_pitcher_report_pdf.py --date yesterday

Output: reports/<date>/<PlayerName>_<game_pk>.pdf
"""
from __future__ import annotations

import argparse
import io
import json
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import numpy as np
import pandas as pd
import joblib
from PIL import Image as PILImage, ImageDraw

from reportlab.lib.colors import Color, HexColor, white
from reportlab.pdfgen import canvas as rl_canvas
from reportlab.lib.utils import ImageReader

# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

LOGO_PATH = ROOT / "static" / "basenerd-logo-official.png"
REPORTS_DIR = ROOT / "reports"
STUFF_MODEL = ROOT / "models" / "stuff_model.pkl"
STUFF_META = ROOT / "models" / "stuff_model_meta.json"
CTRL_MODEL = ROOT / "models" / "control_model.pkl"
CTRL_META = ROOT / "models" / "control_model_meta.json"

# ---------------------------------------------------------------------------
# Page dimensions — 1080x1350 portrait (4:5, optimal for Twitter/X mobile)
# ---------------------------------------------------------------------------
W = 1080
H = 1350
PAD = 28
CARD_R = 12
GAP = 12

# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------
BG = HexColor("#0b1220")
BG_CARD = HexColor("#131c2e")
BG_TABLE_ROW = HexColor("#182338")
ACCENT = HexColor("#38bdf8")
TEXT_PRIMARY = HexColor("#f1f5f9")
TEXT_SECONDARY = HexColor("#94a3b8")
TEXT_MUTED = HexColor("#64748b")
BORDER = HexColor("#1e3a5f")
WHITE = white
GREEN_GOOD = HexColor("#22c55e")
RED_BAD = HexColor("#ef4444")

PITCH_COLORS = {
    "FF": "#d9534f", "FT": "#f0ad4e", "SI": "#f0ad4e", "FC": "#5bc0de",
    "SL": "#ffd54f", "ST": "#ffd54f", "SV": "#9b59b6", "CU": "#5dade2",
    "KC": "#5dade2", "CH": "#5cb85c", "FS": "#5cb85c", "KN": "#95a5a6",
}

SWING_DESCS = {
    "swinging_strike", "swinging_strike_blocked", "foul", "foul_tip",
    "foul_bunt", "hit_into_play", "hit_into_play_no_out", "hit_into_play_score",
}
WHIFF_DESCS = {"swinging_strike", "swinging_strike_blocked"}
IN_ZONE = set(range(1, 10))
OUT_OF_ZONE = {11, 12, 13, 14}
K_EVENTS = {"strikeout", "strikeout_double_play"}
BB_EVENTS = {"walk", "intent_walk"}

MLB_API = "https://statsapi.mlb.com/api/v1"

# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
def _fmt(v, decimals=1, fallback="--"):
    if v is None or (isinstance(v, float) and not math.isfinite(v)):
        return fallback
    if decimals == 0:
        return str(int(round(v)))
    return f"{v:.{decimals}f}"

def _pct(v, fallback="--"):
    if v is None:
        return fallback
    return f"{v:.1f}%"

def _safe_float(x):
    try:
        if x is None:
            return None
        v = float(x)
        return v if math.isfinite(v) else None
    except Exception:
        return None

def _mean(vals):
    vv = [v for v in vals if v is not None and math.isfinite(v)]
    return float(sum(vv)) / len(vv) if vv else None


# ---------------------------------------------------------------------------
# MLB API data fetching
# ---------------------------------------------------------------------------
def _fetch_player(pid: int) -> dict:
    r = requests.get(f"{MLB_API}/people/{pid}", params={"hydrate": "currentTeam"}, timeout=15)
    r.raise_for_status()
    return (r.json().get("people") or [{}])[0]

def _fetch_headshot(pid: int, size: int = 360) -> Optional[PILImage.Image]:
    url = f"https://img.mlbstatic.com/mlb-photos/image/upload/w_{size},q_100/v1/people/{pid}/headshot/67/current"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200 and len(r.content) > 1000:
            return PILImage.open(io.BytesIO(r.content)).convert("RGBA")
    except Exception:
        pass
    return None

def _fetch_live_feed(game_pk: int) -> dict:
    url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def _extract_game_info(feed: dict) -> dict:
    gd = feed.get("gameData") or {}
    dt = gd.get("datetime") or {}
    teams = gd.get("teams") or {}
    live = feed.get("liveData") or {}
    ls = (live.get("linescore") or {}).get("teams") or {}
    away = teams.get("away") or {}
    home = teams.get("home") or {}

    # Boxscore pitcher stats
    boxscore = live.get("boxscore") or {}
    pitcher_stats = {}
    for side in ("away", "home"):
        side_box = (boxscore.get("teams") or {}).get(side) or {}
        players = side_box.get("players") or {}
        for key, pdata in players.items():
            pid = (pdata.get("person") or {}).get("id")
            try:
                stats = pdata.get("stats", {}).get("pitching", {}) or {}
            except Exception:
                stats = {}
            if pid and stats.get("pitchesThrown"):
                pitcher_stats[int(pid)] = {
                    "IP": stats.get("inningsPitched", "--"),
                    "H": stats.get("hits", "--"),
                    "R": stats.get("runs", "--"),
                    "ER": stats.get("earnedRuns", "--"),
                    "BB": stats.get("baseOnBalls", "--"),
                    "SO": stats.get("strikeOuts", "--"),
                    "HR": stats.get("homeRuns", "--"),
                    "pitches": stats.get("pitchesThrown", "--"),
                    "strikes": stats.get("strikes", "--"),
                }

    return {
        "date": (dt.get("officialDate") or "")[:10],
        "away_name": away.get("teamName") or away.get("name", ""),
        "home_name": home.get("teamName") or home.get("name", ""),
        "away_abbrev": away.get("abbreviation", ""),
        "home_abbrev": home.get("abbreviation", ""),
        "away_runs": (ls.get("away") or {}).get("runs"),
        "home_runs": (ls.get("home") or {}).get("runs"),
        "pitcher_stats": pitcher_stats,
    }


def _extract_pitches(feed: dict, pitcher_id: int) -> List[dict]:
    """Extract pitch-level data for a pitcher from the live feed."""
    plays = (feed.get("liveData") or {}).get("plays") or {}
    all_plays = plays.get("allPlays") or []
    pitches = []

    for play in all_plays:
        matchup = play.get("matchup") or {}
        pid = (matchup.get("pitcher") or {}).get("id")
        if pid != pitcher_id:
            continue

        stand = (matchup.get("batSide") or {}).get("code", "")
        p_throws = (matchup.get("pitchHand") or {}).get("code", "")

        # Get the event for this at-bat (only on the last pitch)
        ab_event = (play.get("result") or {}).get("event", "")

        events = play.get("playEvents") or []
        n_pitches_in_ab = sum(1 for e in events if e.get("isPitch"))
        pitch_idx = 0

        for ev in events:
            if not ev.get("isPitch"):
                continue
            pitch_idx += 1
            det = ev.get("details") or {}
            pd_ = ev.get("pitchData") or {}
            coords = pd_.get("coordinates") or {}
            breaks = pd_.get("breaks") or {}
            count = ev.get("count") or {}

            pitch_type = (det.get("type") or {}).get("code", "")
            pitch_name = (det.get("type") or {}).get("description", pitch_type)
            desc = (det.get("description") or "").lower().strip()

            # Map live feed description to statcast description format
            sc_desc = _map_description(det, ev)

            # pfxX/pfxZ from live feed are in inches; convert to feet for model
            pfx_x_in = _safe_float(coords.get("pfxX"))
            pfx_z_in = _safe_float(coords.get("pfxZ"))
            pfx_x_ft = pfx_x_in / 12.0 if pfx_x_in is not None else None
            pfx_z_ft = pfx_z_in / 12.0 if pfx_z_in is not None else None

            pitch = {
                "pitch_type": pitch_type,
                "pitch_name": pitch_name,
                "stand": stand,
                "p_throws": p_throws,
                "description": sc_desc,
                "zone": pd_.get("zone"),
                "events": ab_event.lower().strip() if pitch_idx == n_pitches_in_ab else "",
                # For model scoring
                "release_speed": _safe_float(pd_.get("startSpeed")),
                "release_spin_rate": _safe_float(breaks.get("spinRate")),
                "release_extension": _safe_float(pd_.get("extension")),
                "release_pos_x": _safe_float(coords.get("x0")),
                "release_pos_z": _safe_float(coords.get("z0")),
                "release_pos_y": _safe_float(coords.get("y0")),
                "pfx_x": pfx_x_ft,
                "pfx_z": pfx_z_ft,
                "vx0": _safe_float(coords.get("vX0")),
                "vy0": _safe_float(coords.get("vY0")),
                "vz0": _safe_float(coords.get("vZ0")),
                "ax": _safe_float(coords.get("aX")),
                "ay": _safe_float(coords.get("aY")),
                "az": _safe_float(coords.get("aZ")),
                "sz_top": _safe_float(pd_.get("strikeZoneTop")),
                "sz_bot": _safe_float(pd_.get("strikeZoneBottom")),
                "plate_x": _safe_float(coords.get("pX")),
                "plate_z": _safe_float(coords.get("pZ")),
                "estimated_woba_using_speedangle": None,  # not in live feed
                # Movement in inches for display
                "hb_in": pfx_x_in,
                "ivb_in": pfx_z_in,
            }
            pitches.append(pitch)

    return pitches


def _map_description(details: dict, event: dict) -> str:
    """Map MLB API pitch description to Statcast description format."""
    desc = (details.get("description") or "").lower().strip()
    if "swinging" in desc and "block" in desc:
        return "swinging_strike_blocked"
    if "swinging" in desc:
        return "swinging_strike"
    if "foul tip" in desc:
        return "foul_tip"
    if "foul bunt" in desc:
        return "foul_bunt"
    if "foul" in desc:
        return "foul"
    if "in play" in desc:
        return "hit_into_play"
    if "called strike" in desc:
        return "called_strike"
    if "ball" in desc:
        return "ball"
    return desc


# ---------------------------------------------------------------------------
# Model scoring
# ---------------------------------------------------------------------------
_STUFF_OHE_CATS = sorted([
    'CH', 'CS', 'CU', 'EP', 'FA', 'FC', 'FF', 'FO', 'FS', 'FT',
    'KC', 'KN', 'PO', 'SC', 'SI', 'SL', 'ST', 'SV',
])

def _predict_stuff(stuff_pipe, X_df, num_feats, cat_feats):
    """Predict with stuff model, bypassing broken sklearn pipeline deserialization.

    The pipeline's SimpleImputer and OneHotEncoder lost fitted state during
    cross-version unpickling. We manually impute NaN with column medians and
    one-hot encode pitch_type, then call the RandomForestRegressor directly.
    """
    model = stuff_pipe.named_steps["model"]
    # Fix missing attr on decision trees (added in newer sklearn)
    for tree in model.estimators_:
        if not hasattr(tree, "monotonic_cst"):
            tree.monotonic_cst = None

    num_data = X_df[num_feats].values.astype(np.float64)
    # Impute NaN with column medians (mimics SimpleImputer(strategy='median'))
    for col_i in range(num_data.shape[1]):
        col = num_data[:, col_i]
        nans = np.isnan(col)
        if nans.any():
            med = np.nanmedian(col) if not nans.all() else 0.0
            col[nans] = med

    # One-hot encode pitch_type (alphabetical, matching OHE(categories='auto'))
    cats = _STUFF_OHE_CATS
    cat_vals = X_df[cat_feats[0]].values
    ohe_data = np.zeros((len(cat_vals), len(cats)))
    for i, pt in enumerate(cat_vals):
        if pt in cats:
            ohe_data[i, cats.index(pt)] = 1.0

    X_combined = np.hstack([num_data, ohe_data])
    return model.predict(X_combined)


def _predict_control(ctrl_pipe, X_df, num_feats, cat_feats):
    """Predict with control model, bypassing broken sklearn pipeline deserialization.

    The control model's ColumnTransformer + OrdinalEncoder don't deserialize
    cleanly across sklearn versions. We manually pass numeric features through
    and ordinal-encode pitch_type, then call the model's _raw_predict directly.
    """
    model = ctrl_pipe.named_steps["model"]
    if not hasattr(model, "_preprocessor"):
        model._preprocessor = None
    if not hasattr(model._loss, "link"):
        try:
            from sklearn._loss.loss import HalfSquaredError
        except ImportError:
            from sklearn.ensemble._hist_gradient_boosting.loss import LeastSquares as HalfSquaredError
        model._loss = HalfSquaredError()

    num_data = X_df[num_feats].values.astype(np.float64)
    cat_vals = X_df[cat_feats[0]].values
    n_cat_bins = model._bin_mapper.n_bins_non_missing_[-1]
    all_types = sorted(['CH', 'CS', 'CU', 'EP', 'FA', 'FC', 'FF', 'FS', 'FT',
                        'KC', 'KN', 'SI', 'SL', 'ST', 'SV', 'SC', 'PO'])[:n_cat_bins]
    type_map = {pt: float(i) for i, pt in enumerate(all_types)}
    cat_encoded = np.array([type_map.get(v, 0.0) for v in cat_vals]).reshape(-1, 1)
    X_combined = np.hstack([num_data, cat_encoded])
    return model._raw_predict(X_combined).ravel()


def _score_models(pitches: List[dict]) -> List[dict]:
    """Score stuff+ and control+ on each pitch using local models."""
    if not pitches:
        return pitches

    df = pd.DataFrame(pitches)

    # Score Stuff+
    if STUFF_MODEL.exists() and STUFF_META.exists():
        try:
            import warnings as _w
            with _w.catch_warnings():
                _w.simplefilter("ignore")
                stuff_pipe = joblib.load(str(STUFF_MODEL))
            with open(str(STUFF_META)) as f:
                meta = json.load(f)
            num_feats = meta["num_features"]
            cat_feats = meta["cat_features"]
            feat_cols = num_feats + cat_feats
            goodness_std = float(meta.get("goodness_std", 0.01)) or 0.01
            center = float(meta.get("stuff_center", 100.0))
            scale = float(meta.get("stuff_scale", 15.0))
            clip_lo = float(meta.get("stuff_clip_min", 40.0))
            clip_hi = float(meta.get("stuff_clip_max", 160.0))

            X = df[feat_cols].copy()
            for c in num_feats:
                X[c] = pd.to_numeric(X[c], errors="coerce")
            preds = _predict_stuff(stuff_pipe, X, num_feats, cat_feats)
            goodness = -pd.Series(preds, index=X.index)
            sp = center + scale * (goodness / goodness_std)
            sp = sp.clip(clip_lo, clip_hi)
            df["stuff_plus"] = sp
        except Exception as e:
            print(f"  Warning: stuff+ scoring failed: {e}")

    # Score Control+
    if CTRL_MODEL.exists() and CTRL_META.exists():
        try:
            # Shim for sklearn version mismatch: control model (trained on 1.0.2)
            # references sklearn.ensemble._hist_gradient_boosting.loss.LeastSquares
            # which was removed in sklearn >=1.2. Patch the module so joblib.load
            # can find LeastSquares regardless of sklearn version.
            import importlib
            try:
                _loss_mod = importlib.import_module("sklearn.ensemble._hist_gradient_boosting.loss")
                if not hasattr(_loss_mod, "LeastSquares"):
                    # Module exists but LeastSquares was removed — patch it in
                    try:
                        from sklearn._loss.loss import HalfSquaredError as _Loss
                    except ImportError:
                        from sklearn._loss import HalfSquaredError as _Loss
                    _loss_mod.LeastSquares = _Loss
            except ModuleNotFoundError:
                # Module doesn't exist at all — create a shim
                import types
                try:
                    from sklearn._loss.loss import HalfSquaredError as _Loss
                except ImportError:
                    from sklearn._loss import HalfSquaredError as _Loss
                shim = types.ModuleType("sklearn.ensemble._hist_gradient_boosting.loss")
                shim.LeastSquares = _Loss
                sys.modules["sklearn.ensemble._hist_gradient_boosting.loss"] = shim

            import warnings as _w
            with _w.catch_warnings():
                _w.simplefilter("ignore")
                ctrl_pipe = joblib.load(str(CTRL_MODEL))
            with open(str(CTRL_META)) as f:
                meta = json.load(f)
            num_feats = meta["num_features"]
            cat_feats = meta["cat_features"]
            feat_cols = num_feats + cat_feats
            goodness_std = float(meta.get("goodness_std", 0.01)) or 0.01
            center = float(meta.get("control_center", 100.0))
            scale = float(meta.get("control_scale", 15.0))
            clip_lo = float(meta.get("control_clip_min", 40.0))
            clip_hi = float(meta.get("control_clip_max", 160.0))

            X = df[feat_cols].copy()
            for c in num_feats:
                X[c] = pd.to_numeric(X[c], errors="coerce")
            mask = X[num_feats].notna().all(axis=1)
            if mask.any():
                preds = _predict_control(ctrl_pipe, X.loc[mask], num_feats, cat_feats)
                goodness = -pd.Series(preds, index=X.loc[mask].index)
                cp = center + scale * (goodness / goodness_std)
                cp = cp.clip(clip_lo, clip_hi)
                df.loc[mask, "control_plus"] = cp
        except Exception as e:
            print(f"  Warning: control+ scoring failed: {e}")

    # Merge scores back
    for i, row in df.iterrows():
        pitches[i]["stuff_plus"] = _safe_float(row.get("stuff_plus"))
        pitches[i]["control_plus"] = _safe_float(row.get("control_plus"))

    return pitches


# ---------------------------------------------------------------------------
# Aggregate pitch data into report structure
# ---------------------------------------------------------------------------
def _build_report(pitches: List[dict]) -> dict:
    """Build report structure matching what the website uses."""
    if not pitches:
        return {"ok": False}

    by_pt = {}
    for p in pitches:
        pt = (p.get("pitch_type") or "").strip() or "UNK"
        by_pt.setdefault(pt, []).append(p)

    total = len(pitches)
    mix = []
    shapes = []

    # Side totals for usage_lr
    side_totals = {"L": 0, "R": 0}
    side_by_pt = {}

    # Aggregate counters
    agg_swings = agg_whiffs = agg_in_zone = agg_ooz = agg_ooz_swings = 0
    agg_events = []

    for pt, rows in by_pt.items():
        n = len(rows)
        pitch_name = rows[0].get("pitch_name") or pt

        velo = [_safe_float(r.get("release_speed")) for r in rows]
        spin = [_safe_float(r.get("release_spin_rate")) for r in rows]
        hb_vals = [_safe_float(r.get("hb_in")) for r in rows]
        ivb_vals = [_safe_float(r.get("ivb_in")) for r in rows]
        sp_vals = [_safe_float(r.get("stuff_plus")) for r in rows]
        cp_vals = [_safe_float(r.get("control_plus")) for r in rows]

        swings = whiffs = in_zone = ooz = ooz_swings = 0

        for r in rows:
            desc = (r.get("description") or "")
            zone_num = r.get("zone")
            ev = (r.get("events") or "")

            # Side tracking
            stand = (r.get("stand") or "").upper()
            if stand in ("L", "R"):
                side_totals[stand] += 1
                d = side_by_pt.setdefault(pt, {"L": 0, "R": 0})
                d[stand] += 1

            if desc in SWING_DESCS:
                swings += 1
            if desc in WHIFF_DESCS:
                whiffs += 1
            if zone_num is not None:
                try:
                    zn = int(zone_num)
                    if zn in IN_ZONE:
                        in_zone += 1
                    elif zn in OUT_OF_ZONE:
                        ooz += 1
                        if desc in SWING_DESCS:
                            ooz_swings += 1
                except (ValueError, TypeError):
                    pass
            if ev:
                agg_events.append(ev)

        agg_swings += swings
        agg_whiffs += whiffs
        agg_in_zone += in_zone
        agg_ooz += ooz
        agg_ooz_swings += ooz_swings

        # pfx in feet for shapes (movement chart)
        pfx_x_vals = [_safe_float(r.get("pfx_x")) for r in rows]
        pfx_z_vals = [_safe_float(r.get("pfx_z")) for r in rows]

        mix.append({
            "pitch_type": pt, "pitch_name": pitch_name, "n": n,
            "usage": 100.0 * n / total if total else 0,
            "velo": _mean(velo), "spin": _mean(spin),
            "hb": _mean(hb_vals),  # already inches
            "ivb": _mean(ivb_vals),  # already inches
            "whiff": (100.0 * whiffs / swings) if swings else None,
            "zone_pct": (100.0 * in_zone / n) if n else None,
            "chase_pct": (100.0 * ooz_swings / ooz) if ooz else None,
            "stuff_plus": _mean([v for v in sp_vals if v is not None]),
            "control_plus": _mean([v for v in cp_vals if v is not None]),
        })
        shapes.append({
            "pitch_type": pt, "pitch_name": pitch_name, "n": n,
            "pfx_x": _mean(pfx_x_vals), "pfx_z": _mean(pfx_z_vals),
        })

    agg_pa = len(agg_events)
    basic = {
        "whiff_pct": (100.0 * agg_whiffs / agg_swings) if agg_swings else None,
        "zone_pct": (100.0 * agg_in_zone / total) if total else None,
        "chase_pct": (100.0 * agg_ooz_swings / agg_ooz) if agg_ooz else None,
        "k_pct": (100.0 * sum(1 for e in agg_events if e in K_EVENTS) / agg_pa) if agg_pa else None,
        "bb_pct": (100.0 * sum(1 for e in agg_events if e in BB_EVENTS) / agg_pa) if agg_pa else None,
    }

    # Build usage_lr
    usage_lr = []
    ltot = side_totals.get("L", 0)
    rtot = side_totals.get("R", 0)
    for m in sorted(mix, key=lambda x: x["n"], reverse=True):
        pt = m["pitch_type"]
        d = side_by_pt.get(pt, {"L": 0, "R": 0})
        usage_lr.append({
            "pitch_type": pt, "pitch_name": m["pitch_name"],
            "l_count": d["L"], "r_count": d["R"],
            "l_usage": (100.0 * d["L"] / ltot) if ltot else 0,
            "r_usage": (100.0 * d["R"] / rtot) if rtot else 0,
        })

    mix.sort(key=lambda x: x["n"], reverse=True)
    shapes.sort(key=lambda x: x["n"], reverse=True)

    return {
        "ok": True, "total": total, "basic": basic,
        "mix": mix, "shapes": shapes,
        "usage_lr": usage_lr, "side_totals": side_totals,
    }


def _games_for_date(date_str: str) -> List[dict]:
    url = f"{MLB_API}/schedule"
    params = {"date": date_str, "sportId": 1, "hydrate": "probablePitcher,linescore"}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        out = []
        for d in r.json().get("dates") or []:
            for g in d.get("games") or []:
                gp = g.get("gamePk")
                if not gp:
                    continue
                state = ((g.get("status") or {}).get("abstractGameState") or "").lower()
                if state != "final":
                    continue
                teams = g.get("teams") or {}
                away_p = ((teams.get("away") or {}).get("probablePitcher") or {}).get("id")
                home_p = ((teams.get("home") or {}).get("probablePitcher") or {}).get("id")
                out.append({
                    "game_pk": int(gp),
                    "away_pitcher_id": int(away_p) if away_p else None,
                    "home_pitcher_id": int(home_p) if home_p else None,
                })
        return out
    except Exception as e:
        print(f"Error fetching schedule: {e}")
        return []


# ---------------------------------------------------------------------------
# Drawing helpers
# ---------------------------------------------------------------------------
def _rounded_rect(c, x, y, w, h, r, fill_color=None, stroke_color=None):
    c.saveState()
    if fill_color:
        c.setFillColor(fill_color)
    if stroke_color:
        c.setStrokeColor(stroke_color)
        c.setLineWidth(1)
    r = min(r, w / 2, h / 2)
    c.roundRect(x, y, w, h, r,
                fill=1 if fill_color else 0,
                stroke=1 if stroke_color else 0)
    c.restoreState()

def _color_for_metric(val, center=100.0):
    if val is None:
        return TEXT_MUTED
    diff = val - center
    if abs(diff) < 3:
        return TEXT_PRIMARY
    target = GREEN_GOOD if diff > 0 else RED_BAD
    intensity = min(1.0, abs(diff) / 30.0)
    return Color(
        target.red * intensity + TEXT_PRIMARY.red * (1 - intensity),
        target.green * intensity + TEXT_PRIMARY.green * (1 - intensity),
        target.blue * intensity + TEXT_PRIMARY.blue * (1 - intensity),
    )


def _draw_movement_chart(c, x, y, w, h, shapes, scatter_data, title="Pitch Movement"):
    _rounded_rect(c, x, y, w, h, CARD_R, fill_color=BG_CARD)
    c.setFillColor(TEXT_PRIMARY)
    c.setFont("Helvetica-Bold", 18)
    c.drawString(x + 14, y + h - 28, title)

    # Make chart area square within the card
    chart_top = y + h - 36
    chart_bot = y + 12
    avail_h = chart_top - chart_bot
    avail_w = w - 28
    side = min(avail_h, avail_w)
    cx = x + w / 2
    cy = chart_bot + avail_h / 2
    R = side / 2 - 8
    max_v = 24.0

    # Grid
    c.setStrokeColor(BORDER)
    c.setLineWidth(0.5)
    c.line(cx - R, cy, cx + R, cy)
    c.line(cx, cy - R, cx, cy + R)
    c.setDash(2, 3)
    for rv in [6, 12, 18]:
        rr = (rv / max_v) * R
        c.circle(cx, cy, rr, fill=0, stroke=1)
    c.setDash()
    c.setFillColor(TEXT_MUTED)
    c.setFont("Helvetica", 12)
    for rv in [6, 12, 18]:
        rr = (rv / max_v) * R
        c.drawString(cx + 3, cy + rr + 2, f'{rv}"')

    # Scatter dots
    if scatter_data:
        for p in scatter_data:
            hb = p.get("hb_in")
            ivb = p.get("ivb_in")
            if hb is None or ivb is None:
                continue
            px = cx - (hb / max_v) * R
            py = cy + (ivb / max_v) * R
            color = HexColor(PITCH_COLORS.get(p.get("pitch_type", ""), "#94a3b8"))
            c.setFillColor(color)
            c.setStrokeColor(Color(0, 0, 0, alpha=0.25))
            c.setLineWidth(0.3)
            c.circle(px, py, 4, fill=1, stroke=1)

    # Average dots
    if shapes:
        ordered = sorted(shapes, key=lambda d: d.get("n", 0), reverse=True)
        n_max = max(1, max(d.get("n", 1) for d in ordered))
        for d in ordered:
            pt = d.get("pitch_type", "UNK")
            pfx_x, pfx_z = d.get("pfx_x"), d.get("pfx_z")
            if pfx_x is None or pfx_z is None:
                continue
            hb = max(-max_v, min(max_v, pfx_x * 12.0))
            ivb = max(-max_v, min(max_v, pfx_z * 12.0))
            px = cx - (hb / max_v) * R
            py = cy + (ivb / max_v) * R
            t = d.get("n", 0) / n_max
            rad = 8 + 12 * t
            color = HexColor(PITCH_COLORS.get(pt, "#94a3b8"))
            c.setFillColor(color)
            c.setStrokeColor(white)
            c.setLineWidth(2)
            c.circle(px, py, rad, fill=1, stroke=1)
            c.setFillColor(HexColor("#0b1220"))
            c.setFont("Helvetica-Bold", 12)
            c.drawCentredString(px, py - 4, pt)


def _draw_location_chart(c, x, y, w, h, scatter_data, stand_filter, title):
    _rounded_rect(c, x, y, w, h, CARD_R, fill_color=BG_CARD)
    c.setFillColor(TEXT_PRIMARY)
    c.setFont("Helvetica-Bold", 18)
    c.drawString(x + 14, y + h - 28, title)

    chart_top = y + h - 36
    chart_bot = y + 12
    avail_h = chart_top - chart_bot
    avail_w = w - 28

    # Keep aspect ratio ~4:5 (x_range=4, z_range=4)
    side = min(avail_h, avail_w)
    chart_x = x + (w - side) / 2
    chart_y = chart_bot + (avail_h - side) / 2
    chart_w = chart_h = side

    x_min, x_max = -2.0, 2.0
    z_min, z_max = 0.5, 4.5
    zone_left, zone_right = -17/24, 17/24  # MLB plate = 17 inches = ±0.7083 ft
    zone_bot, zone_top = 1.59, 3.39

    def to_px(plate_x, plate_z):
        px = chart_x + ((plate_x - x_min) / (x_max - x_min)) * chart_w
        py = chart_y + ((plate_z - z_min) / (z_max - z_min)) * chart_h
        return px, py

    # Strike zone
    zl, zb = to_px(zone_left, zone_bot)
    zr, zt = to_px(zone_right, zone_top)
    c.setStrokeColor(HexColor("#ffffff"))
    c.setLineWidth(2)
    c.rect(zl, zb, zr - zl, zt - zb, fill=0, stroke=1)
    # Inner grid
    c.setStrokeColor(HexColor("#334155"))
    c.setLineWidth(0.5)
    zw = (zr - zl) / 3
    zh = (zt - zb) / 3
    for i in range(1, 3):
        c.line(zl + i * zw, zb, zl + i * zw, zt)
        c.line(zl, zb + i * zh, zr, zb + i * zh)

    filtered = [p for p in scatter_data
                if (p.get("stand") or "").upper() == stand_filter.upper()
                and p.get("plate_x") is not None and p.get("plate_z") is not None]

    for p in filtered:
        px, py = to_px(p["plate_x"], p["plate_z"])
        if chart_x <= px <= chart_x + chart_w and chart_y <= py <= chart_y + chart_h:
            pt = p.get("pitch_type", "")
            color = HexColor(PITCH_COLORS.get(pt, "#94a3b8"))
            c.setFillColor(color)
            c.setStrokeColor(Color(0, 0, 0, alpha=0.25))
            c.setLineWidth(0.3)
            c.circle(px, py, 5.5, fill=1, stroke=1)

    c.setFillColor(TEXT_MUTED)
    c.setFont("Helvetica", 13)
    c.drawRightString(x + w - 14, y + h - 28, f"n={len(filtered)}")


def _draw_tornado_chart(c, x, y, w, h, usage_lr):
    _rounded_rect(c, x, y, w, h, CARD_R, fill_color=BG_CARD)
    c.setFillColor(TEXT_PRIMARY)
    c.setFont("Helvetica-Bold", 13)
    c.drawCentredString(x + w / 2, y + h - 20, "Usage Split")
    c.setFillColor(TEXT_SECONDARY)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(x + 8, y + h - 36, "LHH")
    c.drawRightString(x + w - 8, y + h - 36, "RHH")

    if not usage_lr:
        return

    n = len(usage_lr)
    top_y = y + h - 44
    bot_y = y + 8
    avail = top_y - bot_y
    row_h = min(28, avail / max(n, 1))
    center_x = x + w / 2
    half_w = (w / 2) - 24
    max_pct = max(
        max((r.get("l_usage", 0) for r in usage_lr), default=1),
        max((r.get("r_usage", 0) for r in usage_lr), default=1),
        1,
    )

    for i, row in enumerate(usage_lr):
        ry = top_y - (i + 1) * row_h
        pt = row.get("pitch_type", "UNK")
        color = HexColor(PITCH_COLORS.get(pt, "#94a3b8"))

        c.setFillColor(TEXT_PRIMARY)
        c.setFont("Helvetica-Bold", 11)
        c.drawCentredString(center_x, ry + row_h / 2 - 4, pt)

        # LHH bar (left)
        l_pct = row.get("l_usage", 0)
        if l_pct > 0:
            bw = (l_pct / max_pct) * half_w
            bx = center_x - 14 - bw
            _rounded_rect(c, bx, ry + 2, bw, row_h - 6, 4, fill_color=color)
            if bw > 25:
                c.setFillColor(HexColor("#0b1220"))
                c.setFont("Helvetica-Bold", 10)
                c.drawCentredString(bx + bw / 2, ry + row_h / 2 - 4, f"{l_pct:.0f}%")

        # RHH bar (right)
        r_pct = row.get("r_usage", 0)
        if r_pct > 0:
            bw = (r_pct / max_pct) * half_w
            bx = center_x + 14
            _rounded_rect(c, bx, ry + 2, bw, row_h - 6, 4, fill_color=color)
            if bw > 25:
                c.setFillColor(HexColor("#0b1220"))
                c.setFont("Helvetica-Bold", 10)
                c.drawCentredString(bx + bw / 2, ry + row_h / 2 - 4, f"{r_pct:.0f}%")


def _draw_release_point_chart(c, x, y, w, h, scatter_data, shapes, title="Release Point"):
    _rounded_rect(c, x, y, w, h, CARD_R, fill_color=BG_CARD)
    c.setFillColor(TEXT_PRIMARY)
    c.setFont("Helvetica-Bold", 18)
    c.drawString(x + 14, y + h - 28, title)

    chart_top = y + h - 36
    chart_bot = y + 24
    avail_h = chart_top - chart_bot
    avail_w = w - 28
    chart_x0 = x + 16
    chart_y0 = chart_bot

    # Compute data bounds from actual release points
    rx_vals = []
    rz_vals = []
    for p in (scatter_data or []):
        rx = _safe_float(p.get("release_pos_x"))
        rz = _safe_float(p.get("release_pos_z"))
        if rx is not None and rz is not None:
            rx_vals.append(rx)
            rz_vals.append(rz)

    if not rx_vals:
        # No data — draw empty card
        c.setFillColor(TEXT_MUTED)
        c.setFont("Helvetica", 12)
        c.drawCentredString(x + w / 2, y + h / 2, "No data")
        return

    # Add padding around data extent (0.5 ft on each side, min 1 ft range)
    data_x_min, data_x_max = min(rx_vals), max(rx_vals)
    data_z_min, data_z_max = min(rz_vals), max(rz_vals)
    pad_ft = 0.5
    data_x_min -= pad_ft
    data_x_max += pad_ft
    data_z_min -= pad_ft
    data_z_max += pad_ft
    # Enforce minimum range of 1.5 ft
    x_range = max(data_x_max - data_x_min, 1.5)
    z_range = max(data_z_max - data_z_min, 1.5)
    # Center the range
    x_mid = (data_x_min + data_x_max) / 2
    z_mid = (data_z_min + data_z_max) / 2
    data_x_min = x_mid - x_range / 2
    data_x_max = x_mid + x_range / 2
    data_z_min = z_mid - z_range / 2
    data_z_max = z_mid + z_range / 2

    # Make chart square, fit within available space
    side = min(avail_h, avail_w)
    chart_cx = x + w / 2
    chart_cy = chart_bot + avail_h / 2
    half = side / 2 - 6

    def to_px(rx, rz):
        px = chart_cx - half + ((rx - data_x_min) / (data_x_max - data_x_min)) * (2 * half)
        py = chart_cy - half + ((rz - data_z_min) / (data_z_max - data_z_min)) * (2 * half)
        return px, py

    # Grid lines and axis labels
    c.setStrokeColor(BORDER)
    c.setLineWidth(0.5)
    c.setDash(2, 3)
    n_grid = 3
    c.setFillColor(TEXT_MUTED)
    c.setFont("Helvetica", 11)
    # Horizontal grid lines with z labels
    for i in range(n_grid + 1):
        frac = i / n_grid
        gz = data_z_min + frac * (data_z_max - data_z_min)
        _, py = to_px(data_x_min, gz)
        c.line(chart_cx - half, py, chart_cx + half, py)
        c.drawRightString(chart_cx - half - 4, py - 4, f"{gz:.1f}")
    # Vertical grid lines with x labels (skip edges to avoid crowding)
    for i in range(1, n_grid):
        frac = i / n_grid
        gx = data_x_min + frac * (data_x_max - data_x_min)
        px, _ = to_px(gx, data_z_min)
        c.line(px, chart_cy - half, px, chart_cy + half)
        c.drawCentredString(px, chart_cy - half - 14, f"{gx:.1f}")
    c.setDash()

    # Individual scatter dots
    for p in scatter_data:
        rx = _safe_float(p.get("release_pos_x"))
        rz = _safe_float(p.get("release_pos_z"))
        if rx is None or rz is None:
            continue
        px, py = to_px(rx, rz)
        color = HexColor(PITCH_COLORS.get(p.get("pitch_type", ""), "#94a3b8"))
        c.setFillColor(color)
        c.setStrokeColor(Color(0, 0, 0, alpha=0.3))
        c.setLineWidth(0.5)
        c.circle(px, py, 5, fill=1, stroke=1)


# ---------------------------------------------------------------------------
# Main PDF generation
# ---------------------------------------------------------------------------
def generate_report(
    pitcher_id: int,
    game_pk: int,
    out_dir: Optional[Path] = None,
) -> Optional[Path]:
    print(f"  Fetching live feed for game {game_pk}...")
    feed = _fetch_live_feed(game_pk)
    game_info = _extract_game_info(feed)

    print(f"  Extracting pitches for pitcher {pitcher_id}...")
    raw_pitches = _extract_pitches(feed, pitcher_id)
    if not raw_pitches:
        print(f"  No pitches found for pitcher {pitcher_id} in game {game_pk}")
        return None

    print(f"  Scoring {len(raw_pitches)} pitches with BNStuff+ and BNControl+ models...")
    pitches = _score_models(raw_pitches)
    report = _build_report(pitches)
    if not report.get("ok"):
        return None

    player = _fetch_player(pitcher_id)
    headshot_img = _fetch_headshot(pitcher_id, size=360)

    player_name = player.get("fullName", f"Player {pitcher_id}")
    team_name = ((player.get("currentTeam") or {}).get("name") or "")
    game_date = game_info.get("date") or ""

    if out_dir is None:
        out_dir = REPORTS_DIR / (game_date.replace("-", "") or "unknown")
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_name = player_name.replace(" ", "_").replace(".", "")
    out_path = out_dir / f"{safe_name}_{game_pk}.pdf"

    mix = report["mix"]
    basic = report["basic"]
    shapes = report["shapes"]
    usage_lr = report["usage_lr"]
    total_pitches = report["total"]
    box_stats = (game_info.get("pitcher_stats") or {}).get(pitcher_id) or {}

    # --- Create PDF ---
    c = rl_canvas.Canvas(str(out_path), pagesize=(W, H))
    c.setFillColor(BG)
    c.rect(0, 0, W, H, fill=1, stroke=0)

    cw_full = W - PAD * 2  # usable content width
    col_gap = GAP
    cur = H  # cursor — top edge of next section

    # ===== HEADER =====
    hdr_h = 100
    cur -= PAD + hdr_h
    _rounded_rect(c, PAD, cur, cw_full, hdr_h, CARD_R, fill_color=BG_CARD)

    # Headshot — circular crop (left side, next to name)
    hs_display = 72
    hs_x = PAD + 12
    hs_y = cur + (hdr_h - hs_display) / 2
    if headshot_img:
        try:
            img = headshot_img
            sz = min(img.size)
            left = (img.width - sz) // 2
            top = (img.height - sz) // 2
            img = img.crop((left, top, left + sz, top + sz))
            img = img.resize((256, 256), PILImage.LANCZOS if hasattr(PILImage, 'LANCZOS') else PILImage.ANTIALIAS)
            mask = PILImage.new("L", (256, 256), 0)
            ImageDraw.Draw(mask).ellipse([0, 0, 255, 255], fill=255)
            img.putalpha(mask)
            c.drawImage(ImageReader(img), hs_x, hs_y,
                        width=hs_display, height=hs_display, mask="auto")
        except Exception:
            pass

    # Logo — top right branding
    logo_sz = 60
    if LOGO_PATH.exists():
        try:
            logo = PILImage.open(str(LOGO_PATH)).convert("RGBA")
            c.drawImage(ImageReader(logo), PAD + cw_full - 12 - logo_sz, cur + (hdr_h - logo_sz) / 2,
                        width=logo_sz, height=logo_sz, mask="auto")
        except Exception:
            pass

    # Text
    tx = PAD + 12 + hs_display + 12
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 30)
    c.drawString(tx, cur + hdr_h - 34, player_name)

    away_ab = game_info.get("away_abbrev", "")
    home_ab = game_info.get("home_abbrev", "")
    ar = game_info.get("away_runs")
    hr_ = game_info.get("home_runs")
    score = f"  |  {away_ab} {ar} - {home_ab} {hr_}" if ar is not None else ""
    c.setFillColor(TEXT_SECONDARY)
    c.setFont("Helvetica", 15)
    c.drawString(tx, cur + hdr_h - 54, f"{team_name}  |  {game_date}{score}")

    c.setFillColor(ACCENT)
    c.setFont("Helvetica-Bold", 13)
    c.drawString(tx, cur + hdr_h - 74, "POST-GAME PITCHER REPORT")

    # ===== BOXSCORE BAR =====
    cur -= GAP
    bar_h = 52
    cur -= bar_h
    _rounded_rect(c, PAD, cur, cw_full, bar_h, CARD_R, fill_color=BG_CARD)
    box_items = [
        ("IP", str(box_stats.get("IP", "--"))),
        ("H", str(box_stats.get("H", "--"))),
        ("R", str(box_stats.get("R", "--"))),
        ("ER", str(box_stats.get("ER", "--"))),
        ("BB", str(box_stats.get("BB", "--"))),
        ("SO", str(box_stats.get("SO", "--"))),
        ("HR", str(box_stats.get("HR", "--"))),
        ("P-S", f"{box_stats.get('pitches', '--')}-{box_stats.get('strikes', '--')}"),
    ]
    bw = cw_full / len(box_items)
    for i, (lbl, val) in enumerate(box_items):
        bx = PAD + i * bw
        c.setFillColor(TEXT_MUTED)
        c.setFont("Helvetica", 12)
        c.drawCentredString(bx + bw / 2, cur + bar_h - 16, lbl)
        c.setFillColor(WHITE)
        c.setFont("Helvetica-Bold", 20)
        c.drawCentredString(bx + bw / 2, cur + 8, val)

    # ===== STATCAST BAR =====
    cur -= GAP
    cur -= bar_h
    _rounded_rect(c, PAD, cur, cw_full, bar_h, CARD_R, fill_color=BG_CARD)
    stat_items = [
        ("K%", _pct(basic.get("k_pct"))),
        ("BB%", _pct(basic.get("bb_pct"))),
        ("Whiff%", _pct(basic.get("whiff_pct"))),
        ("Zone%", _pct(basic.get("zone_pct"))),
        ("Chase%", _pct(basic.get("chase_pct"))),
        ("Pitches", str(total_pitches)),
    ]
    sw = cw_full / len(stat_items)
    for i, (lbl, val) in enumerate(stat_items):
        sx = PAD + i * sw
        c.setFillColor(TEXT_MUTED)
        c.setFont("Helvetica", 12)
        c.drawCentredString(sx + sw / 2, cur + bar_h - 16, lbl)
        c.setFillColor(WHITE)
        c.setFont("Helvetica-Bold", 20)
        c.drawCentredString(sx + sw / 2, cur + 8, val)

    # ===== ARSENAL TABLE (left) + TORNADO CHART (right) side by side =====
    cur -= GAP

    # Tornado width — enough for the bars but skinny
    tornado_w = 220
    table_w = cw_full - col_gap - tornado_w

    # Table columns — scaled to fit table_w
    cols = ["Pitch", "#", "Velo", "HB", "IVB", "Whiff%", "Zone%", "Chase%", "BNStuff+", "BNCtrl+"]
    col_w = [100, 34, 52, 46, 46, 60, 58, 62, 74, 68]
    ttw = sum(col_w)
    scale = (table_w - 12) / ttw
    col_w = [int(w * scale) for w in col_w]
    ttw = sum(col_w)
    col_w[0] += int(table_w - 12) - ttw

    rh = 30
    table_h = rh * (len(mix) + 1) + 6
    cur -= table_h

    # Draw table card
    tx0 = PAD + 6
    hdr_y = cur + table_h - rh - 3
    _rounded_rect(c, PAD, cur, table_w, table_h, CARD_R, fill_color=BG_CARD)
    _rounded_rect(c, tx0 - 6, hdr_y, table_w, rh + 3, 5, fill_color=BORDER)
    c.setFont("Helvetica-Bold", 12)
    cx_ = tx0
    for ci, cn in enumerate(cols):
        cw = col_w[ci]
        c.setFillColor(ACCENT if cn.startswith("BN") else TEXT_SECONDARY)
        if ci == 0:
            c.drawString(cx_ + 4, hdr_y + 9, cn)
        else:
            c.drawCentredString(cx_ + cw / 2, hdr_y + 9, cn)
        cx_ += cw

    for ri, row in enumerate(mix):
        ry = hdr_y - (ri + 1) * rh
        if ri % 2 == 0:
            _rounded_rect(c, tx0 - 6, ry - 1, table_w, rh, 3, fill_color=BG_TABLE_ROW)
        pt = row.get("pitch_type", "UNK")
        vals = [
            pt, _fmt(row.get("n"), 0),
            _fmt(row.get("velo"), 1), _fmt(row.get("hb"), 1), _fmt(row.get("ivb"), 1),
            _pct(row.get("whiff")),
            _pct(row.get("zone_pct")), _pct(row.get("chase_pct")),
            _fmt(row.get("stuff_plus"), 0), _fmt(row.get("control_plus"), 0),
        ]
        cx_ = tx0
        for ci, val in enumerate(vals):
            cw = col_w[ci]
            if ci == 0:
                pname = row.get("pitch_name") or pt
                color = HexColor(PITCH_COLORS.get(pt, "#94a3b8"))
                pill_w = min(cw - 6, max(52, len(pname) * 8 + 12))
                _rounded_rect(c, cx_ + 2, ry + 4, pill_w, 22, 11, fill_color=color)
                c.setFillColor(HexColor("#0b1220"))
                c.setFont("Helvetica-Bold", 11)
                c.drawCentredString(cx_ + 2 + pill_w / 2, ry + 8, pname)
            elif ci >= 8:
                raw = row.get("stuff_plus") if ci == 8 else row.get("control_plus")
                c.setFillColor(_color_for_metric(raw))
                c.setFont("Helvetica-Bold", 14)
                c.drawCentredString(cx_ + cw / 2, ry + 8, val)
            else:
                c.setFillColor(TEXT_PRIMARY)
                c.setFont("Helvetica", 13)
                c.drawCentredString(cx_ + cw / 2, ry + 8, val)
            cx_ += cw

    # Draw tornado chart beside the table
    _draw_tornado_chart(c, PAD + table_w + col_gap, cur, tornado_w, table_h, usage_lr)

    # ===== CHART ROW 1: Location LHH + Location RHH (2 side by side) =====
    cur -= GAP
    footer_h = 28
    remaining = cur - PAD - footer_h
    chart_row_h = (remaining - GAP) / 2  # split remaining space into 2 rows

    chart_half_w = (cw_full - col_gap) / 2
    chart_y1 = cur - chart_row_h
    _draw_location_chart(c, PAD, chart_y1, chart_half_w, chart_row_h, pitches, "L", "Locations vs LHH")
    _draw_location_chart(c, PAD + chart_half_w + col_gap, chart_y1, chart_half_w, chart_row_h, pitches, "R", "Locations vs RHH")

    # ===== CHART ROW 2: Movement + Release Point (2 side by side) =====
    cur = chart_y1 - GAP
    chart_y2 = cur - chart_row_h
    _draw_movement_chart(c, PAD, chart_y2, chart_half_w, chart_row_h, shapes, pitches)
    _draw_release_point_chart(c, PAD + chart_half_w + col_gap, chart_y2, chart_half_w, chart_row_h, pitches, shapes)

    # ===== FOOTER =====
    c.setFillColor(TEXT_MUTED)
    c.setFont("Helvetica", 12)
    c.drawString(PAD, 10, f"basenerd.com  |  Data: MLB Stats API  |  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    c.setFillColor(ACCENT)
    c.setFont("Helvetica-Bold", 13)
    c.drawRightString(W - PAD, 10, "@basenerd")

    c.save()
    print(f"  Saved: {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate post-game pitcher report PDFs")
    parser.add_argument("--pitcher_id", type=int)
    parser.add_argument("--game_pk", type=int)
    parser.add_argument("--date", type=str, help="YYYY-MM-DD or 'yesterday'")
    parser.add_argument("--outdir", type=str, default=None)
    args = parser.parse_args()

    out_dir = Path(args.outdir) if args.outdir else None

    if args.date:
        if args.date.lower() == "yesterday":
            d = datetime.now(timezone.utc) - timedelta(days=1)
            date_str = d.strftime("%Y-%m-%d")
        elif args.date.lower() == "today":
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        else:
            date_str = args.date

        print(f"Generating reports for {date_str}...")
        games = _games_for_date(date_str)
        if not games:
            print("No final games found.")
            return

        count = 0
        for g in games:
            gp = g["game_pk"]
            # Fetch live feed once per game, find ALL pitchers
            try:
                print(f"\n  Fetching live feed for game {gp}...")
                feed = _fetch_live_feed(gp)
                game_info = _extract_game_info(feed)
                all_pitcher_ids = list((game_info.get("pitcher_stats") or {}).keys())
                if not all_pitcher_ids:
                    print(f"  No pitchers found in game {gp}")
                    continue
                print(f"  Found {len(all_pitcher_ids)} pitchers in game {gp}")
                for pid in all_pitcher_ids:
                    try:
                        result = generate_report(pid, gp, out_dir=out_dir)
                        if result:
                            count += 1
                    except Exception as e:
                        print(f"  Error: pitcher {pid}, game {gp}: {e}")
            except Exception as e:
                print(f"  Error fetching game {gp}: {e}")
        print(f"\nDone. Generated {count} reports.")

    elif args.pitcher_id and args.game_pk:
        generate_report(args.pitcher_id, args.game_pk, out_dir=out_dir)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
