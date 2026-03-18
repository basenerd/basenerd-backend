"""
Umpire strike zone service.

Loads per-umpire strike zone models and provides:
- P(called strike) predictions for a given umpire + pitch location
- Zone heatmap grids (per-umpire vs league average)
- Umpire profile data (tendencies from umpire_metrics.parquet)
- Umpire list with key metrics

Each umpire has their own individually trained model.
Umpires below the training threshold fall back to the league-average model.
"""

import os
import json
import logging
import math

import joblib
import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MODEL_DIR = os.path.join(_ROOT, "models", "umpire_zone_models")
_REGISTRY_PATH = os.path.join(_MODEL_DIR, "registry.json")
_DATA_DIR = os.path.join(_ROOT, "data")

# Lazy-loaded globals
_registry = None
_league_model = None
_umpire_models = {}  # cache: umpire_id -> model
_umpire_metrics = None
_game_outcomes = None

# Heatmap grid bounds
GRID_X_MIN, GRID_X_MAX = -1.5, 1.5
GRID_Z_MIN, GRID_Z_MAX = 1.0, 4.2
GRID_SIZE = 50

# Average strike zone for heatmap generation
AVG_SZ_TOP = 3.4
AVG_SZ_BOT = 1.6
ZONE_X_HALF = 0.83


def _safe_float(val):
    """Convert to float, return None if NaN/None/invalid."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if math.isfinite(f) else None
    except (ValueError, TypeError):
        return None


def _load():
    """Lazy-load registry, league-average model, and data files."""
    global _registry, _league_model, _umpire_metrics, _game_outcomes

    if _registry is not None:
        return

    try:
        with open(_REGISTRY_PATH) as f:
            _registry = json.load(f)
        log.info("Umpire zone registry loaded: %d umpires",
                 len(_registry.get("umpires", {})))
    except Exception as e:
        log.warning("Could not load umpire zone registry: %s", e)
        _registry = {"umpires": {}, "features": [], "cat_features": [],
                      "cat_categories": {}}
        return

    try:
        _league_model = joblib.load(os.path.join(_MODEL_DIR, "_league_avg.joblib"))
        log.info("League-average zone model loaded")
    except Exception as e:
        log.warning("Could not load league-average zone model: %s", e)

    # Load umpire metrics (for profile data)
    metrics_path = os.path.join(_DATA_DIR, "umpire_metrics.parquet")
    try:
        _umpire_metrics = pd.read_parquet(metrics_path)
        log.info("Umpire metrics: %d rows", len(_umpire_metrics))
    except Exception as e:
        log.warning("Could not load umpire metrics: %s", e)
        _umpire_metrics = pd.DataFrame()

    # Load game outcomes (for umpire-game mapping and name lookup)
    outcomes_path = os.path.join(_DATA_DIR, "game_outcomes.parquet")
    try:
        _game_outcomes = pd.read_parquet(outcomes_path)
        log.info("Game outcomes: %d rows", len(_game_outcomes))
    except Exception as e:
        log.warning("Could not load game outcomes: %s", e)
        _game_outcomes = pd.DataFrame()


def _get_model(umpire_id):
    """Get the model for a specific umpire, or league-avg fallback."""
    _load()

    ump_key = str(umpire_id)

    # Check cache
    if umpire_id in _umpire_models:
        return _umpire_models[umpire_id]

    # Try to load per-umpire model
    if ump_key in _registry.get("umpires", {}):
        model_path = os.path.join(_MODEL_DIR, f"{umpire_id}.joblib")
        try:
            model = joblib.load(model_path)
            _umpire_models[umpire_id] = model
            return model
        except Exception as e:
            log.warning("Could not load model for umpire %s: %s", umpire_id, e)

    # Fallback to league average
    return _league_model


def _build_features(plate_x, plate_z, sz_top, sz_bot, pitch_type, stand,
                    balls, strikes):
    """Build a single-row feature DataFrame for prediction."""
    _load()

    sz_range = sz_top - sz_bot
    plate_z_norm = (plate_z - sz_bot) / sz_range if sz_range > 0 else 0.5

    row = {
        "plate_x": plate_x,
        "plate_z_norm": plate_z_norm,
        "dist_from_edge_x": abs(plate_x) - ZONE_X_HALF,
        "dist_from_edge_z_top": plate_z - sz_top,
        "dist_from_edge_z_bot": sz_bot - plate_z,
        "pitch_type": pitch_type,
        "stand": stand,
        "balls": balls,
        "strikes": strikes,
    }

    df = pd.DataFrame([row])

    # Apply categorical encoding consistent with training
    cat_categories = _registry.get("cat_categories", {})
    for col in _registry.get("cat_features", []):
        if col in cat_categories:
            df[col] = pd.Categorical(df[col], categories=cat_categories[col])

    features = _registry.get("features", list(row.keys()))
    return df[features]


def predict_called_strike(hp_umpire_id, plate_x, plate_z, sz_top, sz_bot,
                          pitch_type, stand, balls, strikes):
    """Predict P(called strike) for a single pitch using the umpire's model.

    Returns dict with "ok", "p_called_strike", "model_type".
    """
    _load()

    model = _get_model(hp_umpire_id)
    if model is None:
        return {"ok": False, "error": "No model available"}

    try:
        X = _build_features(plate_x, plate_z, sz_top, sz_bot,
                            pitch_type, stand, balls, strikes)
        prob = float(model.predict_proba(X)[:, 1][0])

        is_individual = str(hp_umpire_id) in _registry.get("umpires", {})
        return {
            "ok": True,
            "p_called_strike": round(prob, 4),
            "model_type": "individual" if is_individual else "league_avg",
        }
    except Exception as e:
        log.error("Prediction error for umpire %s: %s", hp_umpire_id, e)
        return {"ok": False, "error": str(e)}


def umpire_zone_heatmap(hp_umpire_id, stand="R", pitch_type=None,
                        balls=None, strikes=None):
    """Generate a heatmap grid of P(called strike) for an umpire.

    Returns the umpire's grid and the league-average grid for comparison.
    Grid spans plate_x × plate_z with GRID_SIZE resolution.
    """
    _load()

    ump_model = _get_model(hp_umpire_id)
    if ump_model is None:
        return {"ok": False, "error": "No model available"}

    # Build meshgrid
    xs = np.linspace(GRID_X_MIN, GRID_X_MAX, GRID_SIZE)
    zs = np.linspace(GRID_Z_MIN, GRID_Z_MAX, GRID_SIZE)
    xx, zz = np.meshgrid(xs, zs)
    n_points = xx.size

    # Default values
    pt = pitch_type or "FF"
    b = balls if balls is not None else 0
    s = strikes if strikes is not None else 0

    # Build feature grid
    sz_range = AVG_SZ_TOP - AVG_SZ_BOT
    grid_data = {
        "plate_x": xx.ravel(),
        "plate_z_norm": (zz.ravel() - AVG_SZ_BOT) / sz_range,
        "dist_from_edge_x": np.abs(xx.ravel()) - ZONE_X_HALF,
        "dist_from_edge_z_top": zz.ravel() - AVG_SZ_TOP,
        "dist_from_edge_z_bot": AVG_SZ_BOT - zz.ravel(),
        "pitch_type": [pt] * n_points,
        "stand": [stand] * n_points,
        "balls": [b] * n_points,
        "strikes": [s] * n_points,
    }
    grid_df = pd.DataFrame(grid_data)

    # Apply categorical encoding
    cat_categories = _registry.get("cat_categories", {})
    for col in _registry.get("cat_features", []):
        if col in cat_categories:
            grid_df[col] = pd.Categorical(
                grid_df[col], categories=cat_categories[col]
            )

    features = _registry.get("features", list(grid_data.keys()))
    X_grid = grid_df[features]

    try:
        # Umpire-specific predictions
        ump_probs = ump_model.predict_proba(X_grid)[:, 1].reshape(GRID_SIZE, GRID_SIZE)

        # League-average predictions
        league_probs = None
        if _league_model is not None:
            league_probs = _league_model.predict_proba(X_grid)[:, 1].reshape(
                GRID_SIZE, GRID_SIZE
            )

        result = {
            "ok": True,
            "grid_x": xs.tolist(),
            "grid_z": zs.tolist(),
            "p_strike": [[round(float(v), 4) for v in row] for row in ump_probs],
            "stand": stand,
            "pitch_type": pt,
            "balls": b,
            "strikes": s,
            "sz_top": AVG_SZ_TOP,
            "sz_bot": AVG_SZ_BOT,
            "model_type": ("individual"
                           if str(hp_umpire_id) in _registry.get("umpires", {})
                           else "league_avg"),
        }

        if league_probs is not None:
            result["p_strike_league_avg"] = [
                [round(float(v), 4) for v in row] for row in league_probs
            ]
            diff = ump_probs - league_probs
            result["p_strike_diff"] = [
                [round(float(v), 4) for v in row] for row in diff
            ]

        return result

    except Exception as e:
        log.error("Heatmap error for umpire %s: %s", hp_umpire_id, e)
        return {"ok": False, "error": str(e)}


def umpire_profile(hp_umpire_id, season=None):
    """Return umpire profile data including tendencies and model info.

    Returns dict with "ok", tendencies, career stats, and model eval metrics.
    """
    _load()

    if _umpire_metrics is None or _umpire_metrics.empty:
        return {"ok": False, "error": "Umpire metrics not available"}

    um = _umpire_metrics
    ump_data = um[um["hp_umpire_id"] == hp_umpire_id]

    if ump_data.empty:
        return {"ok": False, "error": f"No data for umpire {hp_umpire_id}"}

    # Get name from game_outcomes if available
    name = None
    if _game_outcomes is not None and not _game_outcomes.empty:
        if "hp_umpire_name" in _game_outcomes.columns:
            name_rows = _game_outcomes[
                _game_outcomes["hp_umpire_id"] == hp_umpire_id
            ]["hp_umpire_name"].dropna()
            if not name_rows.empty:
                name = str(name_rows.iloc[0])

    # Season-specific or most recent
    if season:
        row = ump_data[ump_data["season"] == season]
    else:
        row = ump_data.sort_values("season", ascending=False)

    if row.empty:
        row = ump_data.sort_values("season", ascending=False)

    latest = row.iloc[0]

    # Model info
    ump_key = str(hp_umpire_id)
    has_model = ump_key in _registry.get("umpires", {})
    model_info = _registry["umpires"].get(ump_key, {}) if has_model else {}

    result = {
        "ok": True,
        "umpire_id": int(hp_umpire_id),
        "name": name,
        "seasons": sorted(int(s) for s in ump_data["season"].unique()),
        "season": int(latest["season"]),
        "tendencies": {
            "overall_cs_rate": _safe_float(latest.get("overall_cs_rate")),
            "ooz_cs_rate": _safe_float(latest.get("ooz_cs_rate")),
            "iz_ball_rate": _safe_float(latest.get("iz_ball_rate")),
            "zone_size_factor": _safe_float(latest.get("zone_size_factor")),
            "shadow_high_cs_rate": _safe_float(latest.get("shadow_high_cs_rate")),
            "shadow_low_cs_rate": _safe_float(latest.get("shadow_low_cs_rate")),
            "games": int(latest.get("games", 0)),
            "total_called": int(latest.get("total_called", 0)),
            "run_env_factor": _safe_float(latest.get("run_env_factor")),
        },
        "has_individual_model": has_model,
        "model_eval": {
            "brier_score": model_info.get("brier_score"),
            "auc": model_info.get("auc"),
            "total_pitches": model_info.get("total_pitches"),
        } if has_model else None,
    }

    return result


def umpire_list(season=None):
    """Return list of all umpires with key tendency metrics.

    Returns dict with "ok" and "umpires" array.
    """
    _load()

    if _umpire_metrics is None or _umpire_metrics.empty:
        return {"ok": False, "error": "Umpire metrics not available"}

    um = _umpire_metrics.copy()

    if season:
        um = um[um["season"] == season]

    if um.empty:
        return {"ok": True, "umpires": [], "season": season}

    # Get name lookup
    name_map = {}
    if (_game_outcomes is not None and not _game_outcomes.empty
            and "hp_umpire_name" in _game_outcomes.columns):
        names = _game_outcomes[["hp_umpire_id", "hp_umpire_name"]].dropna()
        names = names.drop_duplicates("hp_umpire_id")
        name_map = dict(zip(names["hp_umpire_id"], names["hp_umpire_name"]))

    umpires = []
    for _, row in um.iterrows():
        ump_id = int(row["hp_umpire_id"])
        has_model = str(ump_id) in _registry.get("umpires", {})

        umpires.append({
            "umpire_id": ump_id,
            "name": name_map.get(ump_id),
            "season": int(row["season"]),
            "games": int(row.get("games", 0)),
            "total_called": int(row.get("total_called", 0)),
            "overall_cs_rate": _safe_float(row.get("overall_cs_rate")),
            "ooz_cs_rate": _safe_float(row.get("ooz_cs_rate")),
            "zone_size_factor": _safe_float(row.get("zone_size_factor")),
            "has_individual_model": has_model,
        })

    # Sort by games worked descending
    umpires.sort(key=lambda u: u.get("games", 0), reverse=True)

    return {
        "ok": True,
        "umpires": umpires,
        "season": season,
        "total": len(umpires),
    }
