"""Score stuff+ and control+ from MLB live feed pitch data.

Loads the trained models once and exposes score_pitches_from_feed()
which takes a game_pk and pitcher_id, fetches the MLB live feed,
extracts pitch-level features, and returns mix data with model scores.
"""

import json
import sys
import warnings
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

try:
    import joblib
except ImportError:
    joblib = None

# ---------------------------------------------------------------------------
# Model paths
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).resolve().parent.parent
_STUFF_MODEL = _ROOT / "models" / "stuff_model.pkl"
_STUFF_META = _ROOT / "models" / "stuff_model_meta.json"
_CTRL_MODEL = _ROOT / "models" / "control_model.pkl"
_CTRL_META = _ROOT / "models" / "control_model_meta.json"

# ---------------------------------------------------------------------------
# Lazy-loaded model singletons
# ---------------------------------------------------------------------------
_stuff_pipe = None
_stuff_meta = None
_ctrl_pipe = None
_ctrl_meta = None
_models_loaded = False

_STUFF_OHE_CATS = sorted([
    'CH', 'CS', 'CU', 'EP', 'FA', 'FC', 'FF', 'FO', 'FS', 'FT',
    'KC', 'KN', 'PO', 'SC', 'SI', 'SL', 'ST', 'SV',
])


def _load_models():
    """Load models and metadata once."""
    global _stuff_pipe, _stuff_meta, _ctrl_pipe, _ctrl_meta, _models_loaded
    if _models_loaded:
        return
    _models_loaded = True

    if joblib is None:
        return

    # --- Stuff+ model ---
    if _STUFF_MODEL.exists() and _STUFF_META.exists():
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                _stuff_pipe = joblib.load(str(_STUFF_MODEL))
            with open(str(_STUFF_META)) as f:
                _stuff_meta = json.load(f)
        except Exception as e:
            print(f"[score_live] Failed to load stuff+ model: {e}")

    # --- Control+ model (needs sklearn shim for LeastSquares) ---
    if _CTRL_MODEL.exists() and _CTRL_META.exists():
        try:
            import importlib
            try:
                _loss_mod = importlib.import_module(
                    "sklearn.ensemble._hist_gradient_boosting.loss"
                )
                if not hasattr(_loss_mod, "LeastSquares"):
                    try:
                        from sklearn._loss.loss import HalfSquaredError as _Loss
                    except ImportError:
                        from sklearn._loss import HalfSquaredError as _Loss
                    _loss_mod.LeastSquares = _Loss
            except ModuleNotFoundError:
                import types
                try:
                    from sklearn._loss.loss import HalfSquaredError as _Loss
                except ImportError:
                    from sklearn._loss import HalfSquaredError as _Loss
                shim = types.ModuleType(
                    "sklearn.ensemble._hist_gradient_boosting.loss"
                )
                shim.LeastSquares = _Loss
                sys.modules[
                    "sklearn.ensemble._hist_gradient_boosting.loss"
                ] = shim

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                _ctrl_pipe = joblib.load(str(_CTRL_MODEL))
            with open(str(_CTRL_META)) as f:
                _ctrl_meta = json.load(f)
        except Exception as e:
            print(f"[score_live] Failed to load control+ model: {e}")


# ---------------------------------------------------------------------------
# Model prediction helpers (bypass broken sklearn pipelines)
# ---------------------------------------------------------------------------

def _predict_stuff(pipe, X_df, num_feats, cat_feats):
    model = pipe.named_steps["model"]
    for tree in model.estimators_:
        if not hasattr(tree, "monotonic_cst"):
            tree.monotonic_cst = None

    num_data = X_df[num_feats].values.astype(np.float64)
    for col_i in range(num_data.shape[1]):
        col = num_data[:, col_i]
        nans = np.isnan(col)
        if nans.any():
            med = np.nanmedian(col) if not nans.all() else 0.0
            col[nans] = med

    cats = _STUFF_OHE_CATS
    cat_vals = X_df[cat_feats[0]].values
    ohe_data = np.zeros((len(cat_vals), len(cats)))
    for i, pt in enumerate(cat_vals):
        if pt in cats:
            ohe_data[i, cats.index(pt)] = 1.0

    X_combined = np.hstack([num_data, ohe_data])
    return model.predict(X_combined)


def _predict_control(pipe, X_df, num_feats, cat_feats):
    model = pipe.named_steps["model"]
    if not hasattr(model, "_preprocessor"):
        model._preprocessor = None
    if not hasattr(model._loss, "link"):
        try:
            from sklearn._loss.loss import HalfSquaredError
        except ImportError:
            from sklearn.ensemble._hist_gradient_boosting.loss import (
                LeastSquares as HalfSquaredError,
            )
        model._loss = HalfSquaredError()

    num_data = X_df[num_feats].values.astype(np.float64)
    cat_vals = X_df[cat_feats[0]].values
    n_cat_bins = model._bin_mapper.n_bins_non_missing_[-1]
    all_types = sorted([
        'CH', 'CS', 'CU', 'EP', 'FA', 'FC', 'FF', 'FS', 'FT',
        'KC', 'KN', 'SI', 'SL', 'ST', 'SV', 'SC', 'PO',
    ])[:n_cat_bins]
    type_map = {pt: float(i) for i, pt in enumerate(all_types)}
    cat_encoded = np.array(
        [type_map.get(v, 0.0) for v in cat_vals]
    ).reshape(-1, 1)
    X_combined = np.hstack([num_data, cat_encoded])
    return model._raw_predict(X_combined).ravel()


# ---------------------------------------------------------------------------
# Extract pitch data from MLB live feed
# ---------------------------------------------------------------------------

def _safe_float(v):
    if v is None:
        return None
    try:
        f = float(v)
        return f if np.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def _extract_pitches(feed, pitcher_id: int) -> List[dict]:
    """Extract pitch-level data for a specific pitcher from the live feed."""
    all_plays = (feed.get("liveData") or {}).get("plays", {}).get("allPlays", [])
    pitches = []

    for play in all_plays:
        matchup = play.get("matchup") or {}
        pid = (matchup.get("pitcher") or {}).get("id")
        if pid != pitcher_id:
            continue

        stand = (matchup.get("batSide") or {}).get("code", "")

        for ev in play.get("playEvents") or []:
            if not ev.get("isPitch"):
                continue

            det = ev.get("details") or {}
            pd_ = ev.get("pitchData") or {}
            coords = pd_.get("coordinates") or {}
            breaks = pd_.get("breaks") or {}

            pitch_type = (det.get("type") or {}).get("code", "")
            pitch_name = (det.get("type") or {}).get("description", pitch_type)

            # pfxX/pfxZ from live feed are in inches; convert to feet for model
            pfx_x_in = _safe_float(coords.get("pfxX"))
            pfx_z_in = _safe_float(coords.get("pfxZ"))
            pfx_x_ft = pfx_x_in / 12.0 if pfx_x_in is not None else None
            pfx_z_ft = pfx_z_in / 12.0 if pfx_z_in is not None else None

            call_desc = (det.get("call") or {}).get("description", "")

            pitches.append({
                "pitch_type": pitch_type,
                "pitch_name": pitch_name,
                "stand": stand,
                "call": call_desc,
                # Stuff+ model features
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
                # Control+ model features
                "plate_x": _safe_float(coords.get("pX")),
                "plate_z": _safe_float(coords.get("pZ")),
                # Display
                "mph": _safe_float(pd_.get("startSpeed")),
                "spin": _safe_float(breaks.get("spinRate")),
                "hb_in": pfx_x_in,
                "ivb_in": pfx_z_in,
                "px": _safe_float(coords.get("pX")),
                "pz": _safe_float(coords.get("pZ")),
            })

    return pitches


# ---------------------------------------------------------------------------
# Score pitches with models
# ---------------------------------------------------------------------------

def _score_pitches(pitches: List[dict]) -> List[dict]:
    """Score stuff+ and control+ on each pitch."""
    if not pitches:
        return pitches

    _load_models()

    df = pd.DataFrame(pitches)

    # --- Stuff+ ---
    if _stuff_pipe is not None and _stuff_meta is not None:
        try:
            meta = _stuff_meta
            num_feats = meta["num_features"]
            cat_feats = meta["cat_features"]
            goodness_std = float(meta.get("goodness_std", 0.01)) or 0.01
            center = float(meta.get("stuff_center", 100.0))
            scale = float(meta.get("stuff_scale", 15.0))
            clip_lo = float(meta.get("stuff_clip_min", 40.0))
            clip_hi = float(meta.get("stuff_clip_max", 160.0))

            X = df[num_feats + cat_feats].copy()
            for c in num_feats:
                X[c] = pd.to_numeric(X[c], errors="coerce")
            preds = _predict_stuff(_stuff_pipe, X, num_feats, cat_feats)
            goodness = -pd.Series(preds, index=X.index)
            sp = center + scale * (goodness / goodness_std)
            sp = sp.clip(clip_lo, clip_hi)
            df["stuff_plus"] = sp
        except Exception as e:
            print(f"[score_live] stuff+ scoring failed: {e}")

    # --- Control+ ---
    if _ctrl_pipe is not None and _ctrl_meta is not None:
        try:
            meta = _ctrl_meta
            num_feats = meta["num_features"]
            cat_feats = meta["cat_features"]
            goodness_std = float(meta.get("goodness_std", 0.01)) or 0.01
            center = float(meta.get("control_center", 100.0))
            scale = float(meta.get("control_scale", 15.0))
            clip_lo = float(meta.get("control_clip_min", 40.0))
            clip_hi = float(meta.get("control_clip_max", 160.0))

            X = df[num_feats + cat_feats].copy()
            for c in num_feats:
                X[c] = pd.to_numeric(X[c], errors="coerce")
            mask = X[num_feats].notna().all(axis=1)
            if mask.any():
                preds = _predict_control(_ctrl_pipe, X.loc[mask], num_feats, cat_feats)
                goodness = -pd.Series(preds, index=X.loc[mask].index)
                cp = center + scale * (goodness / goodness_std)
                cp = cp.clip(clip_lo, clip_hi)
                df.loc[mask, "control_plus"] = cp
        except Exception as e:
            print(f"[score_live] control+ scoring failed: {e}")

    # Merge scores back
    for i, row in df.iterrows():
        pitches[i]["stuff_plus"] = _safe_float(row.get("stuff_plus"))
        pitches[i]["control_plus"] = _safe_float(row.get("control_plus"))

    return pitches


# ---------------------------------------------------------------------------
# Aggregate scored pitches into mix array
# ---------------------------------------------------------------------------

SWING_CALLS = {
    "Swinging Strike", "Swinging Strike (Blocked)", "Foul", "Foul Tip",
    "Foul Bunt", "In play, no out", "In play, out(s)", "In play, run(s)",
}
WHIFF_CALLS = {"Swinging Strike", "Swinging Strike (Blocked)"}


def _build_mix(pitches: List[dict]) -> List[dict]:
    """Build mix array from scored pitches."""
    by_type = {}
    for p in pitches:
        pt = (p.get("pitch_type") or "").strip() or "UNK"
        by_type.setdefault(pt, []).append(p)

    total = len(pitches)
    mix = []

    for tc, pts in by_type.items():
        n = len(pts)
        velos = [p["mph"] for p in pts if p.get("mph") is not None]
        spins = [p["spin"] for p in pts if p.get("spin") is not None]

        swings = sum(1 for p in pts if p.get("call") in SWING_CALLS)
        whiffs = sum(1 for p in pts if p.get("call") in WHIFF_CALLS)

        # Zone/chase
        in_zone = 0
        ooz = 0
        ooz_swings = 0
        plate_half = 17 / 24
        for p in pts:
            px, pz = p.get("px"), p.get("pz")
            if px is not None and pz is not None:
                iz = (-plate_half <= px <= plate_half and 1.5 <= pz <= 3.5)
                if iz:
                    in_zone += 1
                else:
                    ooz += 1
                    if p.get("call") in SWING_CALLS:
                        ooz_swings += 1

        stuffs = [p["stuff_plus"] for p in pts if p.get("stuff_plus") is not None]
        ctrls = [p["control_plus"] for p in pts if p.get("control_plus") is not None]
        hbs = [p["hb_in"] for p in pts if p.get("hb_in") is not None]
        ivbs = [p["ivb_in"] for p in pts if p.get("ivb_in") is not None]

        mix.append({
            "pitch_type": tc,
            "pitch_name": pts[0].get("pitch_name", tc),
            "n": n,
            "usage": round(100 * n / total, 1) if total else 0,
            "velo": round(sum(velos) / len(velos), 1) if velos else None,
            "spin": round(sum(spins) / len(spins), 0) if spins else None,
            "whiff": round(100 * whiffs / swings, 1) if swings else None,
            "zone_pct": round(100 * in_zone / n, 1) if n else None,
            "chase_pct": round(100 * ooz_swings / ooz, 1) if ooz else None,
            "stuff_plus": round(sum(stuffs) / len(stuffs), 1) if stuffs else None,
            "control_plus": round(sum(ctrls) / len(ctrls), 1) if ctrls else None,
            "hb": round(sum(hbs) / len(hbs), 1) if hbs else None,
            "ivb": round(sum(ivbs) / len(ivbs), 1) if ivbs else None,
        })

    mix.sort(key=lambda r: r["n"], reverse=True)
    return mix


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def score_pitcher_live(feed: dict, pitcher_id: int) -> dict:
    """Score stuff+/control+ for a pitcher from an MLB live feed dict.

    Returns {"ok": True, "mix": [...]} or {"ok": False}.
    """
    try:
        pitches = _extract_pitches(feed, pitcher_id)
        if not pitches:
            return {"ok": False, "error": "No pitches found"}

        pitches = _score_pitches(pitches)
        mix = _build_mix(pitches)
        return {"ok": True, "mix": mix}
    except Exception as e:
        print(f"[score_live] Error: {e}")
        return {"ok": False, "error": str(e)}
