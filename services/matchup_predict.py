"""
Matchup prediction service.

Loads the XGBoost matchup model + pitch selection model + precomputed profiles
and predicts:
1. PA outcome probabilities (K, OUT, BB, 1B, 2B, 3B, HR, etc.)
2. Expected pitch usage from the pitcher

Uses pitcher's arsenal + batter's per-pitch-type performance for richer predictions.
"""

import os
import json
import logging
from datetime import date

import joblib
import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MODEL_PATH = os.path.join(_ROOT, "models", "matchup_model.joblib")
_META_PATH = os.path.join(_ROOT, "models", "matchup_model_meta.json")
_PITCH_SEL_PATH = os.path.join(_ROOT, "models", "pitch_selection_model.joblib")
_PITCH_SEL_META_PATH = os.path.join(_ROOT, "models", "pitch_selection_meta.json")
_DATA_DIR = os.path.join(_ROOT, "data")

# Lazy-loaded globals
_model = None
_meta = None
_pitch_sel = None      # dict with model, label_encoder, etc.
_pitch_sel_meta = None
_batter_profiles = None
_batter_pitch_types = None
_pitcher_arsenal = None
_park_factors = None

CLASSES = ["1B", "2B", "3B", "BB", "HBP", "HR", "IBB", "K", "OUT"]

PITCH_CATEGORY = {
    "FF": "fastball", "SI": "fastball", "FC": "fastball",
    "SL": "breaking", "CU": "breaking", "KC": "breaking", "ST": "breaking", "SV": "breaking",
    "CH": "offspeed", "FS": "offspeed", "KN": "offspeed",
}

# Friendly pitch type names
PITCH_NAMES = {
    "FF": "4-Seam", "SI": "Sinker", "FC": "Cutter",
    "SL": "Slider", "CU": "Curveball", "CH": "Changeup",
    "FS": "Splitter", "KC": "Knuckle Curve", "ST": "Sweeper",
    "SV": "Slurve", "KN": "Knuckleball",
}

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

_LEAGUE_AVG_BVPT = {
    "bvpt_whiff_rate": 0.245, "bvpt_chase_rate": 0.295,
    "bvpt_zone_contact_rate": 0.82, "bvpt_hard_hit_rate": 0.35,
    "bvpt_xwoba": 0.315,
}

_LEAGUE_AVG_PITCHER = {
    "p_avg_stuff_plus": 100.0, "p_avg_control_plus": 100.0,
    "p_avg_velo": 93.5, "p_whiff_rate": 0.245, "p_chase_rate": 0.295,
    "p_zone_rate": 0.45, "p_xwoba": 0.315,
    "p_num_pitches": 5, "p_total_thrown": 2500,
}

_LEAGUE_AVG_BATTER_R14 = {
    "bat_r14_k_pct": 0.225, "bat_r14_bb_pct": 0.082,
    "bat_r14_xwoba": 0.315, "bat_r14_barrel_rate": 0.07,
    "bat_r14_whiff_rate": 0.245, "bat_r14_chase_rate": 0.295,
}

_LEAGUE_AVG_PITCHER_R14 = {
    "p_r14_k_pct": 0.225, "p_r14_bb_pct": 0.082,
    "p_r14_xwoba": 0.315, "p_r14_whiff_rate": 0.245,
    "p_r14_chase_rate": 0.295,
}


def _load():
    """Lazy-load model, metadata, and profile DataFrames."""
    global _model, _meta, _pitch_sel, _pitch_sel_meta
    global _batter_profiles, _batter_pitch_types, _pitcher_arsenal, _park_factors

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

    # Pitch selection model (optional — enhances output but not required)
    try:
        _pitch_sel = joblib.load(_PITCH_SEL_PATH)
        with open(_PITCH_SEL_META_PATH) as f:
            _pitch_sel_meta = json.load(f)
        log.info("Pitch selection model loaded")
    except Exception as e:
        log.info("Pitch selection model not available: %s", e)
        _pitch_sel = None

    for name, path, attr in [
        ("Batter profiles", "batter_profiles.parquet", "_batter_profiles"),
        ("Batter pitch-type profiles", "batter_pitch_type_profiles.parquet", "_batter_pitch_types"),
        ("Pitcher arsenal", "pitcher_arsenal.parquet", "_pitcher_arsenal"),
        ("Park factors", "park_factors.parquet", "_park_factors"),
    ]:
        try:
            df = pd.read_parquet(os.path.join(_DATA_DIR, path))
            globals()[attr] = df
            log.info("%s: %d rows", name, len(df))
        except Exception as e:
            log.warning("Could not load %s: %s", name, e)
            globals()[attr] = pd.DataFrame()


def _get_batter_features(batter_id, season, p_throws):
    """Look up batter profile features. Falls back to league average."""
    if _batter_profiles is None or _batter_profiles.empty:
        return {**_LEAGUE_AVG_BATTER, **_LEAGUE_AVG_BATTER_PLAT}

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

    # Platoon split
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


def _get_batter_pitch_type_features(batter_id, season, pitcher_usage):
    """
    Look up batter-vs-pitch-type category stats and compute pitch-weighted composites.

    pitcher_usage: dict like {"fastball": 0.55, "breaking": 0.30, "offspeed": 0.15}
    """
    features = {}
    stats = ["whiff_rate", "chase_rate", "zone_contact_rate", "hard_hit_rate", "xwoba"]

    if _batter_pitch_types is None or _batter_pitch_types.empty:
        # Fill all with league average
        for cat in ["fastball", "breaking", "offspeed"]:
            for stat in stats:
                features[f"bvpt_{stat}_{cat}"] = _LEAGUE_AVG_BVPT[f"bvpt_{stat}"]
        for stat in stats:
            features[f"bvpt_w_{stat}"] = _LEAGUE_AVG_BVPT[f"bvpt_{stat}"]
        return features

    bpt = _batter_pitch_types
    for cat in ["fastball", "breaking", "offspeed"]:
        cat_key = f"CAT_{cat}"
        mask = (bpt["batter"] == batter_id) & (bpt["pitch_type"] == cat_key)
        row = bpt[mask & (bpt["season"] == season)]
        if row.empty:
            row = bpt[mask].sort_values("season", ascending=False).head(1)

        for stat in stats:
            if row.empty:
                features[f"bvpt_{stat}_{cat}"] = _LEAGUE_AVG_BVPT[f"bvpt_{stat}"]
            else:
                val = row.iloc[0].get(stat)
                features[f"bvpt_{stat}_{cat}"] = float(val) if pd.notna(val) else _LEAGUE_AVG_BVPT[f"bvpt_{stat}"]

    # Pitch-weighted composites
    for stat in stats:
        weighted = 0.0
        total_w = 0.0
        for cat in ["fastball", "breaking", "offspeed"]:
            w = pitcher_usage.get(cat, 0.33)
            v = features[f"bvpt_{stat}_{cat}"]
            weighted += v * w
            total_w += w
        features[f"bvpt_w_{stat}"] = weighted / total_w if total_w > 0 else _LEAGUE_AVG_BVPT[f"bvpt_{stat}"]

    return features


def _get_pitcher_features(pitcher_id, season):
    """Look up pitcher aggregate + arsenal features."""
    if _pitcher_arsenal is None or _pitcher_arsenal.empty:
        result = dict(_LEAGUE_AVG_PITCHER)
        result.update({
            "p_usage_fastball": 0.55, "p_usage_breaking": 0.30, "p_usage_offspeed": 0.15,
            "p_pitch1_usage": 0.40, "p_pitch1_velo": 93.5, "p_pitch1_whiff": 0.20, "p_pitch1_stuff": 100.0,
            "p_pitch2_usage": 0.25, "p_pitch2_velo": 85.0, "p_pitch2_whiff": 0.30, "p_pitch2_stuff": 100.0,
            "p_pitch3_usage": 0.15, "p_pitch3_velo": 84.0, "p_pitch3_whiff": 0.25, "p_pitch3_stuff": 100.0,
        })
        return result, {"fastball": 0.55, "breaking": 0.30, "offspeed": 0.15}, []

    pa = _pitcher_arsenal
    mask = (pa["pitcher"] == pitcher_id) & (pa["stand"] == "ALL")

    df_season = pa[mask & (pa["season"] == season)]
    if df_season.empty:
        df_season = pa[mask].sort_values("season", ascending=False)
        if df_season.empty:
            result = dict(_LEAGUE_AVG_PITCHER)
            result.update({
                "p_usage_fastball": 0.55, "p_usage_breaking": 0.30, "p_usage_offspeed": 0.15,
                "p_pitch1_usage": 0.40, "p_pitch1_velo": 93.5, "p_pitch1_whiff": 0.20, "p_pitch1_stuff": 100.0,
                "p_pitch2_usage": 0.25, "p_pitch2_velo": 85.0, "p_pitch2_whiff": 0.30, "p_pitch2_stuff": 100.0,
                "p_pitch3_usage": 0.15, "p_pitch3_velo": 84.0, "p_pitch3_whiff": 0.25, "p_pitch3_stuff": 100.0,
            })
            return result, {"fastball": 0.55, "breaking": 0.30, "offspeed": 0.15}, []
        latest = df_season["season"].iloc[0]
        df_season = df_season[df_season["season"] == latest]

    n = df_season["n"].values.astype(float)
    total = n.sum()
    if total == 0:
        result = dict(_LEAGUE_AVG_PITCHER)
        result.update({"p_usage_fastball": 0.55, "p_usage_breaking": 0.30, "p_usage_offspeed": 0.15})
        return result, {"fastball": 0.55, "breaking": 0.30, "offspeed": 0.15}, []

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

    # Category usage
    cat_usage = {"fastball": 0.0, "breaking": 0.0, "offspeed": 0.0}
    for _, row in df_season.iterrows():
        pt = row.get("pitch_type", "")
        cat = PITCH_CATEGORY.get(pt)
        if cat:
            cat_usage[cat] += float(row["n"]) / total
    result["p_usage_fastball"] = cat_usage["fastball"]
    result["p_usage_breaking"] = cat_usage["breaking"]
    result["p_usage_offspeed"] = cat_usage["offspeed"]

    # Top 3 pitches by usage
    top3 = df_season.nlargest(3, "n")
    arsenal_list = []
    for rank, (_, row) in enumerate(top3.iterrows(), 1):
        usage = float(row["n"]) / total
        velo = float(row["avg_velo"]) if pd.notna(row.get("avg_velo")) else 93.5
        whiff = float(row["whiff_rate"]) if pd.notna(row.get("whiff_rate")) else 0.245
        stuff = float(row["avg_stuff_plus"]) if pd.notna(row.get("avg_stuff_plus")) else 100.0
        result[f"p_pitch{rank}_usage"] = usage
        result[f"p_pitch{rank}_velo"] = velo
        result[f"p_pitch{rank}_whiff"] = whiff
        result[f"p_pitch{rank}_stuff"] = stuff
        arsenal_list.append({
            "pitch_type": row.get("pitch_type", "??"),
            "name": PITCH_NAMES.get(row.get("pitch_type", ""), row.get("pitch_type", "")),
            "usage": round(usage, 3),
            "velo": round(velo, 1),
            "whiff": round(whiff, 3),
            "stuff": round(stuff, 0),
        })

    # Fill missing ranks
    for rank in range(len(top3) + 1, 4):
        result[f"p_pitch{rank}_usage"] = 0.0
        result[f"p_pitch{rank}_velo"] = 0.0
        result[f"p_pitch{rank}_whiff"] = 0.0
        result[f"p_pitch{rank}_stuff"] = 0.0

    return result, cat_usage, arsenal_list


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


def _get_pitch_usage(pitcher_id, season, stand, arsenal_list):
    """
    Get expected pitch usage from the pitcher's arsenal.
    Returns list of {pitch_type, name, usage, velo, whiff, stuff} sorted by usage.
    """
    if not arsenal_list:
        return []

    # If we have the pitch selection model, we could refine usage by count/context.
    # For now, return the baseline arsenal usage (already context-free).
    return arsenal_list


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
      - summary: {k_pct, bb_pct, hit_pct, hr_pct, obp, xba, xslg}
      - arsenal: [{pitch_type, name, usage, velo, whiff, stuff}, ...]
      - ok: True
    """
    _load()

    if _model is None:
        return {"ok": False, "reason": "model_not_loaded"}

    # Assemble feature vector
    features = {}

    # Batter overall + platoon features
    features.update(_get_batter_features(batter_id, season, p_throws))

    # Pitcher features (also returns category usage and arsenal list)
    pitcher_feats, cat_usage, arsenal_list = _get_pitcher_features(pitcher_id, season)
    features.update(pitcher_feats)

    # Batter vs pitch-type features (weighted by this pitcher's usage)
    features.update(_get_batter_pitch_type_features(batter_id, season, cat_usage))

    # Park factors
    if venue:
        features.update(_get_park_features(venue, season))
    else:
        features.update({"park_run_factor": 1.0, "park_hr_factor": 1.0})

    # Recent form (rolling 14-day)
    try:
        from services.recent_form import get_batter_recent_form, get_pitcher_recent_form
        features.update(get_batter_recent_form(batter_id))
        features.update(get_pitcher_recent_form(pitcher_id))
    except Exception as e:
        log.info("Recent form unavailable: %s — using league avg", e)
        features.update(_LEAGUE_AVG_BATTER_R14)
        features.update(_LEAGUE_AVG_PITCHER_R14)

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
    ab_pct = 1.0 - prob_dict["BB"] - prob_dict["HBP"] - prob_dict["IBB"]
    xba_adj = hit_pct / ab_pct if ab_pct > 0 else 0

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

    # Get arsenal/pitch usage for this matchup
    arsenal = _get_pitch_usage(pitcher_id, season, stand, arsenal_list)

    return {
        "ok": True,
        "probs": prob_dict,
        "summary": summary,
        "arsenal": arsenal,
    }


def predict_matchup_live(
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
    pitcher_velo_tonight=None,
    pitcher_pitch_count=None,
):
    """
    Predict PA outcome with Bayesian in-game adjustments.

    Gets base prediction from XGBoost, then applies multiplicative adjustments
    based on observed pitcher performance tonight (velo delta, fatigue).

    Returns same structure as predict_matchup plus:
      - adjustments: dict showing what shifted and by how much
      - pregame_probs: baseline prediction (inning 1, no runners, 1st TTO)
    """
    # Get live prediction (already uses real context: inning, runners, TTO)
    result = predict_matchup(
        batter_id, pitcher_id, stand, p_throws, venue, season,
        inning, outs, runner_1b, runner_2b, runner_3b, n_thru_order,
    )

    if not result.get("ok"):
        return result

    # Get pregame baseline for comparison
    pregame = predict_matchup(
        batter_id, pitcher_id, stand, p_throws, venue, season,
        inning=1, outs=0, runner_1b=0, runner_2b=0, runner_3b=0, n_thru_order=1,
    )
    result["pregame_probs"] = pregame.get("probs", {})

    # Apply Bayesian adjustments
    adjustments = {}
    adj_factors = {cls: 1.0 for cls in CLASSES}

    # Velocity adjustment
    _load()
    if pitcher_velo_tonight is not None and _pitcher_arsenal is not None and not _pitcher_arsenal.empty:
        pa = _pitcher_arsenal
        mask = (pa["pitcher"] == pitcher_id) & (pa["stand"] == "ALL")
        season_data = pa[mask & (pa["season"] == season)]
        if season_data.empty:
            season_data = pa[mask].sort_values("season", ascending=False)

        if not season_data.empty:
            n = season_data["n"].values.astype(float)
            total = n.sum()
            if total > 0:
                velos = pd.to_numeric(season_data["avg_velo"], errors="coerce")
                valid = velos.notna()
                if valid.any():
                    expected_velo = float(np.average(velos[valid], weights=n[valid]))
                    velo_delta = pitcher_velo_tonight - expected_velo

                    if abs(velo_delta) > 0.3:  # Only adjust if meaningful difference
                        # Each 1 mph above expected -> K prob +2.5%, HR prob -1.5%
                        adj_factors["K"] *= (1.0 + velo_delta * 0.025)
                        adj_factors["HR"] *= (1.0 - velo_delta * 0.015)
                        adj_factors["BB"] *= (1.0 - velo_delta * 0.01)
                        adjustments["velo"] = {
                            "tonight": round(pitcher_velo_tonight, 1),
                            "expected": round(expected_velo, 1),
                            "delta": round(velo_delta, 1),
                        }

    # Pitch count / fatigue adjustment
    if pitcher_pitch_count is not None and pitcher_pitch_count > 75:
        fatigue_factor = min((pitcher_pitch_count - 75) / 25.0, 1.0)
        adj_factors["K"] *= (1.0 - fatigue_factor * 0.08)
        adj_factors["BB"] *= (1.0 + fatigue_factor * 0.06)
        adj_factors["HR"] *= (1.0 + fatigue_factor * 0.04)
        adj_factors["1B"] *= (1.0 + fatigue_factor * 0.02)
        adjustments["fatigue"] = {
            "pitch_count": pitcher_pitch_count,
            "factor": round(fatigue_factor, 2),
        }

    # Apply adjustments and renormalize
    if adjustments:
        probs = result["probs"]
        adjusted = {cls: probs[cls] * adj_factors.get(cls, 1.0) for cls in CLASSES}
        total = sum(adjusted.values())
        if total > 0:
            adjusted = {cls: round(v / total, 4) for cls, v in adjusted.items()}
        result["probs"] = adjusted

        # Recompute summary with adjusted probs
        hit_pct = adjusted["1B"] + adjusted["2B"] + adjusted["3B"] + adjusted["HR"]
        on_base_pct = hit_pct + adjusted["BB"] + adjusted["HBP"] + adjusted["IBB"]
        ab_pct = 1.0 - adjusted["BB"] - adjusted["HBP"] - adjusted["IBB"]
        xba = hit_pct / ab_pct if ab_pct > 0 else 0
        tb = adjusted["1B"] + adjusted["2B"] * 2 + adjusted["3B"] * 3 + adjusted["HR"] * 4
        xslg = tb / ab_pct if ab_pct > 0 else 0

        result["summary"] = {
            "k_pct": round(adjusted["K"], 3),
            "bb_pct": round(adjusted["BB"] + adjusted["IBB"], 3),
            "hit_pct": round(hit_pct, 3),
            "hr_pct": round(adjusted["HR"], 3),
            "obp": round(on_base_pct, 3),
            "xba": round(xba, 3),
            "xslg": round(xslg, 3),
        }

    result["adjustments"] = adjustments
    return result
