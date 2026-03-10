"""
Matchup prediction service.

Loads the XGBoost matchup model + precomputed profiles (batter, pitcher, park)
and predicts plate appearance outcome probabilities for a batter-vs-pitcher matchup.

Returns 9-class probabilities: 1B, 2B, 3B, BB, HBP, HR, IBB, K, OUT
plus derived summary stats (hit%, k%, bb%, etc.).
"""

import os
import json
import logging

import joblib
import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MODEL_PATH = os.path.join(_ROOT, "models", "matchup_model.joblib")
_META_PATH = os.path.join(_ROOT, "models", "matchup_model_meta.json")
_DATA_DIR = os.path.join(_ROOT, "data")

# Lazy-loaded globals
_model = None
_meta = None
_batter_profiles = None   # DataFrame
_pitcher_arsenal = None    # DataFrame
_park_factors = None       # DataFrame

CLASSES = ["1B", "2B", "3B", "BB", "HBP", "HR", "IBB", "K", "OUT"]

# League-average fallbacks (2021-2025 approx)
_LEAGUE_AVG_BATTER = {
    "bat_k_pct": 0.225, "bat_bb_pct": 0.082, "bat_whiff_rate": 0.245,
    "bat_chase_rate": 0.295, "bat_zone_swing_rate": 0.68, "bat_zone_contact_rate": 0.82,
    "bat_avg_ev": 88.5, "bat_avg_la": 12.0, "bat_barrel_rate": 0.07,
    "bat_hard_hit_rate": 0.35, "bat_sweet_spot_rate": 0.33,
    "bat_gb_rate": 0.43, "bat_fb_rate": 0.36, "bat_hr_per_fb": 0.12,
    "bat_iso": 0.155, "bat_babip": 0.295, "bat_xwoba": 0.315,
}

_LEAGUE_AVG_BATTER_PLAT = {
    "bat_plat_k_pct": 0.225, "bat_plat_bb_pct": 0.082,
    "bat_plat_whiff_rate": 0.245, "bat_plat_chase_rate": 0.295,
    "bat_plat_avg_ev": 88.5, "bat_plat_barrel_rate": 0.07,
    "bat_plat_xwoba": 0.315,
}

_LEAGUE_AVG_PITCHER = {
    "p_avg_stuff_plus": 100.0, "p_avg_control_plus": 100.0,
    "p_avg_velo": 93.5, "p_whiff_rate": 0.245, "p_chase_rate": 0.295,
    "p_zone_rate": 0.45, "p_xwoba": 0.315,
    "p_num_pitches": 5, "p_total_thrown": 2500,
}


def _load():
    """Lazy-load model, metadata, and profile DataFrames."""
    global _model, _meta, _batter_profiles, _pitcher_arsenal, _park_factors

    if _model is not None:
        return

    try:
        _model = joblib.load(_MODEL_PATH)
        with open(_META_PATH) as f:
            _meta = json.load(f)
        log.info("Matchup model loaded (%d features)", _model.n_features_in_)
    except Exception as e:
        log.warning("Could not load matchup model: %s", e)
        _model = None
        return

    try:
        bp = os.path.join(_DATA_DIR, "batter_profiles.parquet")
        _batter_profiles = pd.read_parquet(bp)
        log.info("Batter profiles: %d rows", len(_batter_profiles))
    except Exception as e:
        log.warning("Could not load batter profiles: %s", e)
        _batter_profiles = pd.DataFrame()

    try:
        pa = os.path.join(_DATA_DIR, "pitcher_arsenal.parquet")
        _pitcher_arsenal = pd.read_parquet(pa)
        log.info("Pitcher arsenal: %d rows", len(_pitcher_arsenal))
    except Exception as e:
        log.warning("Could not load pitcher arsenal: %s", e)
        _pitcher_arsenal = pd.DataFrame()

    try:
        pf = os.path.join(_DATA_DIR, "park_factors.parquet")
        _park_factors = pd.read_parquet(pf)
        log.info("Park factors: %d rows", len(_park_factors))
    except Exception as e:
        log.warning("Could not load park factors: %s", e)
        _park_factors = pd.DataFrame()


def _get_batter_features(batter_id, season, p_throws):
    """Look up batter profile features. Falls back to league average."""
    if _batter_profiles is None or _batter_profiles.empty:
        return {**_LEAGUE_AVG_BATTER, **_LEAGUE_AVG_BATTER_PLAT}

    # Try exact season, then fall back to most recent
    bp = _batter_profiles
    mask_all = (bp["batter"] == batter_id) & (bp["vs_hand"] == "ALL")

    row_all = bp[mask_all & (bp["season"] == season)]
    if row_all.empty:
        row_all = bp[mask_all].sort_values("season", ascending=False).head(1)

    if row_all.empty:
        overall = dict(_LEAGUE_AVG_BATTER)
    else:
        r = row_all.iloc[0]
        overall = {}
        for k, default in _LEAGUE_AVG_BATTER.items():
            col = k.replace("bat_", "", 1)
            val = r.get(col)
            overall[k] = float(val) if pd.notna(val) else default

    # Platoon split (vs pitcher's throwing hand)
    plat_hand = p_throws if p_throws in ("L", "R") else "R"
    mask_plat = (bp["batter"] == batter_id) & (bp["vs_hand"] == plat_hand)
    row_plat = bp[mask_plat & (bp["season"] == season)]
    if row_plat.empty:
        row_plat = bp[mask_plat].sort_values("season", ascending=False).head(1)

    if row_plat.empty:
        plat = dict(_LEAGUE_AVG_BATTER_PLAT)
    else:
        r = row_plat.iloc[0]
        plat = {}
        for k, default in _LEAGUE_AVG_BATTER_PLAT.items():
            col = k.replace("bat_plat_", "", 1)
            val = r.get(col)
            plat[k] = float(val) if pd.notna(val) else default

    return {**overall, **plat}


def _get_pitcher_features(pitcher_id, season):
    """Look up pitcher aggregate features. Falls back to league average."""
    if _pitcher_arsenal is None or _pitcher_arsenal.empty:
        return dict(_LEAGUE_AVG_PITCHER)

    pa = _pitcher_arsenal
    mask = (pa["pitcher"] == pitcher_id) & (pa["stand"] == "ALL")

    df_season = pa[mask & (pa["season"] == season)]
    if df_season.empty:
        df_season = pa[mask].sort_values("season", ascending=False)
        if df_season.empty:
            return dict(_LEAGUE_AVG_PITCHER)
        # Use most recent season
        latest = df_season["season"].iloc[0]
        df_season = df_season[df_season["season"] == latest]

    # Weighted aggregation across pitch types
    n = df_season["n"].values.astype(float)
    total = n.sum()
    if total == 0:
        return dict(_LEAGUE_AVG_PITCHER)

    def _wavg(col):
        vals = pd.to_numeric(df_season[col], errors="coerce")
        valid = vals.notna()
        if not valid.any():
            return None
        return float(np.average(vals[valid], weights=n[valid]))

    result = {}
    for feat, col, default in [
        ("p_avg_stuff_plus", "avg_stuff_plus", 100.0),
        ("p_avg_control_plus", "avg_control_plus", 100.0),
        ("p_avg_velo", "avg_velo", 93.5),
        ("p_whiff_rate", "whiff_rate", 0.245),
        ("p_chase_rate", "chase_rate", 0.295),
        ("p_zone_rate", "zone_rate", 0.45),
        ("p_xwoba", "xwoba", 0.315),
    ]:
        val = _wavg(col) if col in df_season.columns else None
        result[feat] = val if val is not None else default

    result["p_num_pitches"] = int(df_season["pitch_type"].nunique()) if "pitch_type" in df_season.columns else 5
    result["p_total_thrown"] = int(total)

    return result


def _get_park_features(venue, season):
    """Look up park factors. Falls back to neutral."""
    if _park_factors is None or _park_factors.empty:
        return {"park_run_factor": 1.0, "park_hr_factor": 1.0}

    pf = _park_factors
    mask = (pf["venue"] == venue) & (pf["stand"] == "ALL")
    row = pf[mask & (pf["season"] == season)]
    if row.empty:
        row = pf[mask].sort_values("season", ascending=False).head(1)
    if row.empty:
        return {"park_run_factor": 1.0, "park_hr_factor": 1.0}

    r = row.iloc[0]
    return {
        "park_run_factor": float(r["run_factor"]) if pd.notna(r.get("run_factor")) else 1.0,
        "park_hr_factor": float(r["hr_factor"]) if pd.notna(r.get("hr_factor")) else 1.0,
    }


def predict_matchup(
    batter_id,
    pitcher_id,
    stand="R",
    p_throws="R",
    venue=None,
    season=2025,
    inning=1,
    outs=0,
    runner_1b=0,
    runner_2b=0,
    runner_3b=0,
    n_thru_order=1,
):
    """
    Predict PA outcome probabilities for a batter-vs-pitcher matchup.

    Returns dict with:
      - probs: {1B, 2B, 3B, BB, HBP, HR, IBB, K, OUT} probabilities
      - summary: {k_pct, bb_pct, hit_pct, hr_pct, obp, xba_approx}
      - ok: True
    """
    _load()

    if _model is None:
        return {"ok": False, "reason": "model_not_loaded"}

    # Assemble feature vector
    features = {}

    # Batter features
    features.update(_get_batter_features(batter_id, season, p_throws))

    # Pitcher features
    features.update(_get_pitcher_features(pitcher_id, season))

    # Park factors
    if venue:
        features.update(_get_park_features(venue, season))
    else:
        features.update({"park_run_factor": 1.0, "park_hr_factor": 1.0})

    # Context
    features["inning"] = inning
    features["outs_when_up"] = outs
    features["n_thruorder_pitcher"] = n_thru_order
    features["runner_on_1b"] = runner_1b
    features["runner_on_2b"] = runner_2b
    features["runner_on_3b"] = runner_3b

    # Categorical encoding (same as training: L=0, R=1)
    features["stand"] = 0 if stand == "L" else 1
    features["p_throws"] = 0 if p_throws == "L" else 1

    # Build feature array in model's expected order
    feature_names = _meta["numeric_features"] + _meta["categorical_features"]
    X = np.array([[features.get(f, np.nan) for f in feature_names]], dtype=np.float64)

    # Predict
    probs = _model.predict_proba(X)[0]

    # Map to class names
    prob_dict = {}
    for i, cls in enumerate(CLASSES):
        prob_dict[cls] = round(float(probs[i]), 4)

    # Derived summary stats
    hit_pct = prob_dict["1B"] + prob_dict["2B"] + prob_dict["3B"] + prob_dict["HR"]
    on_base_pct = hit_pct + prob_dict["BB"] + prob_dict["HBP"] + prob_dict["IBB"]
    xba = prob_dict["1B"] + prob_dict["2B"] + prob_dict["3B"] + prob_dict["HR"]
    # Approximate at-bats exclude BB, HBP, IBB
    ab_pct = 1.0 - prob_dict["BB"] - prob_dict["HBP"] - prob_dict["IBB"]
    xba_adj = hit_pct / ab_pct if ab_pct > 0 else 0

    # Expected total bases
    tb = (prob_dict["1B"] * 1 + prob_dict["2B"] * 2 +
          prob_dict["3B"] * 3 + prob_dict["HR"] * 4)
    xslg = tb / ab_pct if ab_pct > 0 else 0

    summary = {
        "k_pct": round(prob_dict["K"], 3),
        "bb_pct": round(prob_dict["BB"] + prob_dict["IBB"], 3),
        "hit_pct": round(hit_pct, 3),
        "hr_pct": round(prob_dict["HR"], 3),
        "obp": round(on_base_pct, 3),
        "xba": round(xba_adj, 3),
        "xslg": round(xslg, 3),
    }

    return {
        "ok": True,
        "probs": prob_dict,
        "summary": summary,
    }
