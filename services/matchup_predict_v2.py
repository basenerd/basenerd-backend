"""
Matchup prediction service v2.

Drop-in replacement for matchup_predict.py that uses the v2 model
with enhanced features (count, score, prior-year, blended stats,
interactions, deltas, sample size awareness).

Key differences from v1:
- Uses prior-year stats as stabilized baseline for early season
- Blends current + prior year based on sample size
- Includes count (balls/strikes) for much better K/BB prediction
- Interaction and delta features for better matchup modeling
- Sample size features so model knows reliability of inputs
"""

import os
import json
import logging
from datetime import date
from functools import lru_cache

import joblib
import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MODEL_PATH = os.path.join(_ROOT, "models", "matchup_model_v2.joblib")
_META_PATH = os.path.join(_ROOT, "models", "matchup_model_v2_meta.json")
_DATA_DIR = os.path.join(_ROOT, "data")

# Lazy globals
_model = None
_meta = None
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

PITCH_NAMES = {
    "FF": "4-Seam", "SI": "Sinker", "FC": "Cutter",
    "SL": "Slider", "CU": "Curveball", "CH": "Changeup",
    "FS": "Splitter", "KC": "Knuckle Curve", "ST": "Sweeper",
    "SV": "Slurve", "KN": "Knuckleball",
}

# League averages (2021-2025)
LEAGUE_AVG = {
    "k_pct": 0.223, "bb_pct": 0.083, "whiff_rate": 0.245,
    "chase_rate": 0.295, "zone_swing_rate": 0.68, "zone_contact_rate": 0.82,
    "avg_ev": 88.5, "avg_la": 12.0, "barrel_rate": 0.068,
    "hard_hit_rate": 0.35, "sweet_spot_rate": 0.33,
    "gb_rate": 0.43, "fb_rate": 0.36, "hr_per_fb": 0.12,
    "iso": 0.155, "babip": 0.295, "xwoba": 0.315,
}

LG_PITCHER = {
    "avg_stuff_plus": 100.0, "avg_control_plus": 100.0,
    "avg_velo": 93.5, "whiff_rate": 0.245, "chase_rate": 0.295,
    "zone_rate": 0.45, "xwoba": 0.315,
}


def _load():
    """Lazy-load model and data."""
    global _model, _meta, _batter_profiles, _batter_pitch_types, _pitcher_arsenal, _park_factors

    if _model is not None:
        return

    try:
        _model = joblib.load(_MODEL_PATH)
        with open(_META_PATH) as f:
            _meta = json.load(f)
        log.info("Matchup v2 model loaded (%d features)", _model.n_features_in_)
    except Exception as e:
        log.warning("Could not load matchup v2 model: %s", e)
        # Fall back to v1 model
        v1_path = os.path.join(_ROOT, "models", "matchup_model.joblib")
        v1_meta = os.path.join(_ROOT, "models", "matchup_model_meta.json")
        try:
            _model = joblib.load(v1_path)
            with open(v1_meta) as f:
                _meta = json.load(f)
            log.warning("Fell back to v1 model")
        except Exception:
            _model = None
            return

    for name, path, attr in [
        ("Batter profiles", "batter_profiles.parquet", "_batter_profiles"),
        ("Batter pitch-type", "batter_pitch_type_profiles.parquet", "_batter_pitch_types"),
        ("Pitcher arsenal", "pitcher_arsenal.parquet", "_pitcher_arsenal"),
        ("Park factors", "park_factors.parquet", "_park_factors"),
    ]:
        try:
            df = pd.read_parquet(os.path.join(_DATA_DIR, path))
            globals()[attr] = df
        except Exception as e:
            log.warning("Could not load %s: %s", name, e)
            globals()[attr] = pd.DataFrame()


def _get_batter_features(batter_id, season, p_throws):
    """
    Get batter features including current year, prior year, and blended.
    Returns dict with bat_*, bat_prev_*, bat_blend_*, bat_plat_*, bat_season_pa.
    """
    features = {}

    if _batter_profiles is None or _batter_profiles.empty:
        # Fill all with league average
        for prefix in ["bat_", "bat_prev_", "bat_blend_"]:
            for k, v in LEAGUE_AVG.items():
                features[f"{prefix}{k}"] = v
        for k, v in LEAGUE_AVG.items():
            if k in ["k_pct", "bb_pct", "whiff_rate", "chase_rate", "avg_ev", "barrel_rate", "xwoba"]:
                features[f"bat_plat_{k}"] = v
        features["bat_season_pa"] = 0
        return features

    bp = _batter_profiles

    # Current season "ALL" hand split
    def _extract_profile(batter, target_season, prefix):
        mask = (bp["batter"] == batter) & (bp["vs_hand"] == "ALL")
        row = bp[mask & (bp["season"] == target_season)]
        if row.empty:
            row = bp[mask].sort_values("season", ascending=False).head(1)
        if row.empty:
            for k, v in LEAGUE_AVG.items():
                features[f"{prefix}{k}"] = v
            return 0
        r = row.iloc[0]
        pa = int(r.get("pa", 0))
        for k, v in LEAGUE_AVG.items():
            col = k
            val = r.get(col)
            features[f"{prefix}{k}"] = float(val) if pd.notna(val) else v
        return pa

    curr_pa = _extract_profile(batter_id, season, "bat_")
    features["bat_season_pa"] = curr_pa

    prev_pa = _extract_profile(batter_id, season - 1, "bat_prev_")

    # Blended: weight current season by PA, fill with prior year
    blend_threshold = 100
    w = min(curr_pa / blend_threshold, 1.0)
    for k in LEAGUE_AVG:
        curr_val = features.get(f"bat_{k}")
        prev_val = features.get(f"bat_prev_{k}")
        if curr_val is not None and prev_val is not None:
            features[f"bat_blend_{k}"] = w * curr_val + (1 - w) * prev_val
        elif curr_val is not None:
            features[f"bat_blend_{k}"] = curr_val
        elif prev_val is not None:
            features[f"bat_blend_{k}"] = prev_val
        else:
            features[f"bat_blend_{k}"] = LEAGUE_AVG[k]

    # Platoon split
    plat_hand = p_throws if p_throws in ("L", "R") else "R"
    mask_plat = (bp["batter"] == batter_id) & (bp["vs_hand"] == plat_hand)
    row_plat = bp[mask_plat & (bp["season"] == season)]
    if row_plat.empty:
        row_plat = bp[mask_plat].sort_values("season", ascending=False).head(1)

    plat_cols = ["k_pct", "bb_pct", "whiff_rate", "chase_rate", "avg_ev", "barrel_rate", "xwoba"]
    if row_plat.empty:
        for k in plat_cols:
            features[f"bat_plat_{k}"] = LEAGUE_AVG.get(k, 0.0)
    else:
        r = row_plat.iloc[0]
        for k in plat_cols:
            val = r.get(k)
            features[f"bat_plat_{k}"] = float(val) if pd.notna(val) else LEAGUE_AVG.get(k, 0.0)

    return features


def _get_pitcher_features(pitcher_id, season):
    """Get pitcher features including current, prior year, arsenal entropy."""
    features = {}
    cat_usage = {"fastball": 0.55, "breaking": 0.30, "offspeed": 0.15}
    arsenal_list = []

    if _pitcher_arsenal is None or _pitcher_arsenal.empty:
        for k, v in LG_PITCHER.items():
            features[f"p_{k}"] = v
            features[f"p_prev_{k}"] = v
        features["p_num_pitches"] = 5
        features["p_total_thrown"] = 2500
        features["p_season_pitches"] = 0
        features["p_arsenal_entropy"] = 2.0
        features["p_pitch_type_count"] = 5
        features.update({
            "p_usage_fastball": 0.55, "p_usage_breaking": 0.30, "p_usage_offspeed": 0.15,
        })
        for rank in range(1, 4):
            features[f"p_pitch{rank}_usage"] = [0.40, 0.25, 0.15][rank-1]
            features[f"p_pitch{rank}_velo"] = 93.5
            features[f"p_pitch{rank}_whiff"] = 0.245
            features[f"p_pitch{rank}_stuff"] = 100.0
        return features, cat_usage, arsenal_list

    pa = _pitcher_arsenal

    def _extract_pitcher(target_season, prefix):
        mask = (pa["pitcher"] == pitcher_id) & (pa["stand"] == "ALL")
        df_s = pa[mask & (pa["season"] == target_season)]
        if df_s.empty:
            df_s = pa[mask].sort_values("season", ascending=False)
            if not df_s.empty:
                latest = df_s["season"].iloc[0]
                df_s = df_s[df_s["season"] == latest]
        if df_s.empty:
            for k, v in LG_PITCHER.items():
                features[f"{prefix}{k}"] = v
            return None

        n = pd.to_numeric(df_s["n"], errors="coerce").fillna(0).values.astype(float)
        total = n.sum()
        if total == 0:
            for k, v in LG_PITCHER.items():
                features[f"{prefix}{k}"] = v
            return None

        def _wavg(col):
            vals = pd.to_numeric(df_s[col], errors="coerce")
            valid = vals.notna() & (n > 0)
            if not valid.any():
                return None
            return float(np.average(vals[valid], weights=n[valid]))

        for feat, col, default in [
            ("avg_stuff_plus", "avg_stuff_plus", 100.0),
            ("avg_control_plus", "avg_control_plus", 100.0),
            ("avg_velo", "avg_velo", 93.5),
            ("whiff_rate", "whiff_rate", 0.245),
            ("chase_rate", "chase_rate", 0.295),
            ("zone_rate", "zone_rate", 0.45),
            ("xwoba", "xwoba", 0.315),
        ]:
            val = _wavg(col) if col in df_s.columns else None
            features[f"{prefix}{feat}"] = val if val is not None else default

        return df_s

    df_curr = _extract_pitcher(season, "p_")
    _extract_pitcher(season - 1, "p_prev_")

    if df_curr is not None:
        n = pd.to_numeric(df_curr["n"], errors="coerce").fillna(0).values.astype(float)
        total = n.sum()
        features["p_num_pitches"] = int(df_curr["pitch_type"].nunique()) if "pitch_type" in df_curr.columns else 5
        features["p_total_thrown"] = int(total)
        features["p_season_pitches"] = int(total)

        # Arsenal entropy
        probs = n / total if total > 0 else n
        probs = probs[probs > 0]
        features["p_arsenal_entropy"] = float(-np.sum(probs * np.log2(probs))) if len(probs) > 0 else 0.0
        features["p_pitch_type_count"] = len(probs)

        # Category usage
        cat_n = {"fastball": 0, "breaking": 0, "offspeed": 0}
        for _, row in df_curr.iterrows():
            cat = PITCH_CATEGORY.get(row.get("pitch_type", ""))
            if cat:
                cat_n[cat] += float(row["n"])
        for cat in ["fastball", "breaking", "offspeed"]:
            cat_usage[cat] = cat_n[cat] / total if total > 0 else 0.33
            features[f"p_usage_{cat}"] = cat_usage[cat]

        # Top 3 pitches
        top3 = df_curr.nlargest(3, "n")
        for rank, (_, row) in enumerate(top3.iterrows(), 1):
            usage = float(row["n"]) / total
            velo = float(row["avg_velo"]) if pd.notna(row.get("avg_velo")) else 93.5
            whiff = float(row["whiff_rate"]) if pd.notna(row.get("whiff_rate")) else 0.245
            stuff = float(row["avg_stuff_plus"]) if pd.notna(row.get("avg_stuff_plus")) else 100.0
            features[f"p_pitch{rank}_usage"] = usage
            features[f"p_pitch{rank}_velo"] = velo
            features[f"p_pitch{rank}_whiff"] = whiff
            features[f"p_pitch{rank}_stuff"] = stuff
            arsenal_list.append({
                "pitch_type": row.get("pitch_type", "??"),
                "name": PITCH_NAMES.get(row.get("pitch_type", ""), row.get("pitch_type", "??")),
                "usage": round(usage, 3),
                "velo": round(velo, 1),
                "whiff": round(whiff, 3),
                "stuff": round(stuff, 0),
            })

        for rank in range(len(top3) + 1, 4):
            features[f"p_pitch{rank}_usage"] = 0.0
            features[f"p_pitch{rank}_velo"] = 0.0
            features[f"p_pitch{rank}_whiff"] = 0.0
            features[f"p_pitch{rank}_stuff"] = 0.0
    else:
        features["p_num_pitches"] = 5
        features["p_total_thrown"] = 2500
        features["p_season_pitches"] = 0
        features["p_arsenal_entropy"] = 2.0
        features["p_pitch_type_count"] = 5
        features.update({
            "p_usage_fastball": 0.55, "p_usage_breaking": 0.30, "p_usage_offspeed": 0.15,
        })
        for rank in range(1, 4):
            features[f"p_pitch{rank}_usage"] = [0.40, 0.25, 0.15][rank-1]
            features[f"p_pitch{rank}_velo"] = 93.5
            features[f"p_pitch{rank}_whiff"] = 0.245
            features[f"p_pitch{rank}_stuff"] = 100.0

    return features, cat_usage, arsenal_list


def _get_bvpt_features(batter_id, season, cat_usage):
    """Get batter-vs-pitch-type features."""
    features = {}
    stats = ["whiff_rate", "chase_rate", "zone_contact_rate", "hard_hit_rate", "xwoba"]
    defaults = {"whiff_rate": 0.245, "chase_rate": 0.295, "zone_contact_rate": 0.82,
                "hard_hit_rate": 0.35, "xwoba": 0.315}

    if _batter_pitch_types is None or _batter_pitch_types.empty:
        for cat in ["fastball", "breaking", "offspeed"]:
            for stat in stats:
                features[f"bvpt_{stat}_{cat}"] = defaults[stat]
        for stat in stats:
            features[f"bvpt_w_{stat}"] = defaults[stat]
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
                features[f"bvpt_{stat}_{cat}"] = defaults[stat]
            else:
                val = row.iloc[0].get(stat)
                features[f"bvpt_{stat}_{cat}"] = float(val) if pd.notna(val) else defaults[stat]

    # Weighted composites
    for stat in stats:
        weighted = sum(
            features[f"bvpt_{stat}_{cat}"] * cat_usage.get(cat, 0.33)
            for cat in ["fastball", "breaking", "offspeed"]
        )
        total_w = sum(cat_usage.get(cat, 0.33) for cat in ["fastball", "breaking", "offspeed"])
        features[f"bvpt_w_{stat}"] = weighted / total_w if total_w > 0 else defaults[stat]

    return features


def _get_park_features(venue, season):
    """Look up park factors."""
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


def _compute_engineered_features(features):
    """Compute all interaction, delta, and matchup features from base features."""

    # Interactions
    pairs = [
        ("bat_blend_k_pct", "p_whiff_rate", "interact_k_whiff"),
        ("bat_blend_k_pct", "p_chase_rate", "interact_k_chase"),
        ("bat_blend_bb_pct", "p_zone_rate", "interact_bb_zone"),
        ("bat_blend_whiff_rate", "p_whiff_rate", "interact_whiff_whiff"),
        ("bat_blend_chase_rate", "p_chase_rate", "interact_chase_chase"),
        ("bat_blend_xwoba", "p_xwoba", "interact_xwoba"),
        ("bat_blend_barrel_rate", "p_avg_stuff_plus", "interact_barrel_stuff"),
        ("bat_blend_hard_hit_rate", "p_avg_velo", "interact_hard_velo"),
        ("bat_blend_iso", "park_hr_factor", "interact_iso_park"),
    ]
    for a, b, name in pairs:
        va = features.get(a)
        vb = features.get(b)
        if va is not None and vb is not None:
            features[name] = va * vb

    # Deltas from league average
    batter_deltas = [
        ("bat_blend_k_pct", LEAGUE_AVG["k_pct"], "bat_delta_k"),
        ("bat_blend_bb_pct", LEAGUE_AVG["bb_pct"], "bat_delta_bb"),
        ("bat_blend_whiff_rate", LEAGUE_AVG["whiff_rate"], "bat_delta_whiff"),
        ("bat_blend_chase_rate", LEAGUE_AVG["chase_rate"], "bat_delta_chase"),
        ("bat_blend_xwoba", LEAGUE_AVG["xwoba"], "bat_delta_xwoba"),
        ("bat_blend_barrel_rate", LEAGUE_AVG["barrel_rate"], "bat_delta_barrel"),
        ("bat_blend_iso", LEAGUE_AVG["iso"], "bat_delta_iso"),
    ]
    pitcher_deltas = [
        ("p_whiff_rate", 0.245, "p_delta_whiff"),
        ("p_chase_rate", 0.295, "p_delta_chase"),
        ("p_xwoba", 0.315, "p_delta_xwoba"),
        ("p_zone_rate", 0.45, "p_delta_zone"),
    ]
    for col, avg, name in batter_deltas + pitcher_deltas:
        val = features.get(col)
        features[name] = (val - avg) if val is not None else 0.0

    # Matchup advantages
    features["matchup_k_advantage"] = features.get("bat_delta_k", 0) + features.get("p_delta_whiff", 0)
    features["matchup_bb_advantage"] = features.get("bat_delta_bb", 0) - features.get("p_delta_zone", 0)
    features["matchup_xwoba_advantage"] = features.get("bat_delta_xwoba", 0) - features.get("p_delta_xwoba", 0)
    features["matchup_power_advantage"] = features.get("bat_delta_barrel", 0) + features.get("bat_delta_iso", 0)

    # Platoon splits
    bat_plat_k = features.get("bat_plat_k_pct")
    bat_blend_k = features.get("bat_blend_k_pct")
    if bat_plat_k is not None and bat_blend_k is not None:
        features["platoon_k_split"] = bat_plat_k - bat_blend_k
    bat_plat_xwoba = features.get("bat_plat_xwoba")
    bat_blend_xwoba = features.get("bat_blend_xwoba")
    if bat_plat_xwoba is not None and bat_blend_xwoba is not None:
        features["platoon_xwoba_split"] = bat_plat_xwoba - bat_blend_xwoba

    # Recent form deltas (if recent form was provided)
    r14_k = features.get("bat_r14_k_pct")
    if r14_k is not None and bat_blend_k is not None:
        features["bat_r14_k_delta"] = r14_k - bat_blend_k
    r14_xwoba = features.get("bat_r14_xwoba")
    if r14_xwoba is not None and bat_blend_xwoba is not None:
        features["bat_r14_xwoba_delta"] = r14_xwoba - bat_blend_xwoba
    p_r14_k = features.get("p_r14_k_pct")
    p_whiff = features.get("p_whiff_rate")
    if p_r14_k is not None and p_whiff is not None:
        features["p_r14_k_delta"] = p_r14_k - p_whiff

    # Stuff vs contact
    stuff = features.get("p_avg_stuff_plus", 100)
    zc = features.get("bat_blend_zone_contact_rate", LEAGUE_AVG["zone_contact_rate"])
    features["stuff_vs_contact"] = (stuff - 100) / 20 - (zc - LEAGUE_AVG["zone_contact_rate"]) * 5

    # Derived
    n_tto = features.get("n_thruorder_pitcher", 1)
    features["is_third_time_thru"] = 1 if n_tto >= 3 else 0
    features["tto_squared"] = n_tto ** 2
    features["est_pitch_count"] = max(0, min(130, (n_tto - 1) * 36 + features.get("inning", 1) * 4))

    return features


def predict_matchup(
    batter_id, pitcher_id, stand="R", p_throws="R",
    venue=None, season=None, inning=1, outs=0,
    runner_1b=0, runner_2b=0, runner_3b=0, n_thru_order=1,
    balls=0, strikes=0, bat_score=0, fld_score=0,
    pitcher_days_rest=None,
    weather_temp_f=None, weather_wind_mph=None, weather_hr_factor=None,
    weather_xbh_factor=None, weather_density_factor=None, weather_is_dome=0,
    seq_n_pitches=1, seq_prev_pitch_cat=-1, seq_prev_was_whiff=0,
    seq_prev_was_strike=0, seq_prev_was_ball=0, seq_prev_same_cat=0,
    seq_fb_pct_in_ab=0.5, seq_whiffs_in_ab=0, seq_chases_in_ab=0,
    seq_zone_pct_in_ab=0.5, seq_velo_trend=0.0,
):
    """
    Predict PA outcome probabilities (v2).

    New parameters vs v1: balls, strikes, bat_score, fld_score.
    """
    _load()
    if _model is None:
        return {"ok": False, "reason": "model_not_loaded"}

    if season is None:
        season = date.today().year

    features = {}

    # Batter features (current + prior + blend + platoon)
    features.update(_get_batter_features(batter_id, season, p_throws))

    # Pitcher features (current + prior + arsenal)
    pitcher_feats, cat_usage, arsenal_list = _get_pitcher_features(pitcher_id, season)
    features.update(pitcher_feats)

    # Batter vs pitch-type
    features.update(_get_bvpt_features(batter_id, season, cat_usage))

    # Park factors
    if venue:
        features.update(_get_park_features(venue, season))
    else:
        features.update({"park_run_factor": 1.0, "park_hr_factor": 1.0})

    # Recent form with Bayesian shrinkage
    try:
        from services.recent_form import get_batter_recent_form, get_pitcher_recent_form, RELIABLE_PA
        bat_r14 = get_batter_recent_form(batter_id)
        pit_r14 = get_pitcher_recent_form(pitcher_id)

        bat_pa = bat_r14.pop("_pa", RELIABLE_PA)
        if bat_pa < RELIABLE_PA:
            w = bat_pa / RELIABLE_PA
            mapping = {
                "bat_r14_k_pct": "bat_blend_k_pct",
                "bat_r14_bb_pct": "bat_blend_bb_pct",
                "bat_r14_xwoba": "bat_blend_xwoba",
                "bat_r14_barrel_rate": "bat_blend_barrel_rate",
                "bat_r14_whiff_rate": "bat_blend_whiff_rate",
                "bat_r14_chase_rate": "bat_blend_chase_rate",
            }
            for r14_key, profile_key in mapping.items():
                observed = bat_r14.get(r14_key)
                prior = features.get(profile_key, LEAGUE_AVG.get(profile_key.replace("bat_blend_", ""), 0))
                if observed is not None and prior is not None:
                    bat_r14[r14_key] = w * observed + (1 - w) * prior

        pit_pa = pit_r14.pop("_pa", RELIABLE_PA)
        if pit_pa < RELIABLE_PA:
            w = pit_pa / RELIABLE_PA
            for r14_key, profile_key in [
                ("p_r14_xwoba", "p_xwoba"),
                ("p_r14_whiff_rate", "p_whiff_rate"),
                ("p_r14_chase_rate", "p_chase_rate"),
            ]:
                observed = pit_r14.get(r14_key)
                prior = features.get(profile_key, LG_PITCHER.get(profile_key.replace("p_", ""), 0))
                if observed is not None and prior is not None:
                    pit_r14[r14_key] = w * observed + (1 - w) * prior
            for r14_key in ("p_r14_k_pct", "p_r14_bb_pct"):
                observed = pit_r14.get(r14_key)
                lg_val = {"p_r14_k_pct": 0.225, "p_r14_bb_pct": 0.082}.get(r14_key, 0)
                if observed is not None:
                    pit_r14[r14_key] = w * observed + (1 - w) * lg_val

        features.update(bat_r14)
        features.update(pit_r14)
    except Exception as e:
        log.info("Recent form unavailable: %s", e)
        features.update({
            "bat_r14_k_pct": features.get("bat_blend_k_pct", LEAGUE_AVG["k_pct"]),
            "bat_r14_bb_pct": features.get("bat_blend_bb_pct", LEAGUE_AVG["bb_pct"]),
            "bat_r14_xwoba": features.get("bat_blend_xwoba", LEAGUE_AVG["xwoba"]),
            "bat_r14_barrel_rate": features.get("bat_blend_barrel_rate", LEAGUE_AVG["barrel_rate"]),
            "bat_r14_whiff_rate": features.get("bat_blend_whiff_rate", LEAGUE_AVG["whiff_rate"]),
            "bat_r14_chase_rate": features.get("bat_blend_chase_rate", LEAGUE_AVG["chase_rate"]),
            "p_r14_k_pct": 0.225, "p_r14_bb_pct": 0.082,
            "p_r14_xwoba": features.get("p_xwoba", 0.315),
            "p_r14_whiff_rate": features.get("p_whiff_rate", 0.245),
            "p_r14_chase_rate": features.get("p_chase_rate", 0.295),
        })

    # Context features
    features["inning"] = inning
    features["outs_when_up"] = outs
    features["n_thruorder_pitcher"] = n_thru_order
    features["runner_on_1b"] = runner_1b
    features["runner_on_2b"] = runner_2b
    features["runner_on_3b"] = runner_3b

    # NEW: Count features
    features["balls"] = balls
    features["strikes"] = strikes
    features["count_leverage"] = strikes - balls

    # NEW: Game state features
    score_diff = max(-8, min(8, bat_score - fld_score))
    features["score_diff"] = score_diff
    features["runners_on"] = runner_1b + runner_2b + runner_3b
    features["base_out_state"] = features["runners_on"] * 3 + outs
    features["is_home_batting"] = 0  # Will be set by caller if known
    features["late_and_close"] = 1 if inning >= 7 and abs(score_diff) <= 2 else 0

    # Season day
    today = date.today()
    season_start = date(season, 3, 20)
    features["day_of_season"] = max(0, min(200, (today - season_start).days))

    # Pitcher days rest
    features["pitcher_days_rest"] = pitcher_days_rest if pitcher_days_rest is not None else 4.0
    features["wl_days_rest"] = features["pitcher_days_rest"]
    features["wl_is_starter"] = 1 if n_thru_order >= 1 and inning <= 2 else 0

    # Weather features
    features["weather_temp_f"] = weather_temp_f if weather_temp_f is not None else 72
    features["weather_wind_mph"] = weather_wind_mph if weather_wind_mph is not None else 0
    features["weather_hr_factor"] = weather_hr_factor if weather_hr_factor is not None else 1.0
    features["weather_xbh_factor"] = weather_xbh_factor if weather_xbh_factor is not None else 1.0
    features["weather_density_factor"] = weather_density_factor if weather_density_factor is not None else 1.0
    features["weather_is_dome"] = weather_is_dome

    # Pitch sequence context
    features["seq_n_pitches"] = seq_n_pitches
    features["seq_prev_pitch_cat"] = seq_prev_pitch_cat
    features["seq_prev_was_whiff"] = seq_prev_was_whiff
    features["seq_prev_was_strike"] = seq_prev_was_strike
    features["seq_prev_was_ball"] = seq_prev_was_ball
    features["seq_prev_same_cat"] = seq_prev_same_cat
    features["seq_fb_pct_in_ab"] = seq_fb_pct_in_ab
    features["seq_whiffs_in_ab"] = seq_whiffs_in_ab
    features["seq_chases_in_ab"] = seq_chases_in_ab
    features["seq_zone_pct_in_ab"] = seq_zone_pct_in_ab
    features["seq_velo_trend"] = seq_velo_trend

    # BvP history — look up from precomputed data or use defaults
    # At runtime, these could be populated from a DB query
    for bvp_feat in ["bvp_pa", "bvp_k_pct", "bvp_bb_pct",
                      "bvp_hit_pct", "bvp_hr_pct", "bvp_xwoba"]:
        if bvp_feat not in features:
            defaults = {"bvp_pa": 0, "bvp_k_pct": 0.223, "bvp_bb_pct": 0.083,
                        "bvp_hit_pct": 0.235, "bvp_hr_pct": 0.033, "bvp_xwoba": 0.315}
            features[bvp_feat] = defaults.get(bvp_feat, 0)

    # Spray features — look up from precomputed data or use league average
    spray_defaults = {
        "bat_pull_pct": 0.40, "bat_center_pct": 0.35, "bat_oppo_pct": 0.25,
        "bat_pull_hr_pct": 0.65, "bat_gb_pull_pct": 0.55, "bat_fb_pull_pct": 0.40,
        "bat_spray_entropy": 1.5, "bat_avg_spray_angle": -5.0,
        "p_opp_pull_pct": 0.40, "p_opp_center_pct": 0.35, "p_opp_oppo_pct": 0.25,
        "p_opp_gb_pull_pct": 0.55, "p_opp_fb_pull_pct": 0.40,
    }
    for k, v in spray_defaults.items():
        if k not in features:
            features[k] = v

    # Categorical
    features["stand"] = 0 if stand == "L" else 1
    features["p_throws"] = 0 if p_throws == "L" else 1

    # Compute all engineered features
    features = _compute_engineered_features(features)

    # Build feature array in model's expected order
    feature_names = _meta["numeric_features"] + _meta["categorical_features"]
    X = np.array([[features.get(f, 0.0) for f in feature_names]], dtype=np.float64)

    # Replace NaN with 0
    X = np.nan_to_num(X, nan=0.0)

    # Predict
    probs = _model.predict_proba(X)[0]

    prob_dict = {}
    for i, cls in enumerate(CLASSES):
        prob_dict[cls] = round(float(probs[i]), 4)

    # Derived summary
    hit_pct = prob_dict["1B"] + prob_dict["2B"] + prob_dict["3B"] + prob_dict["HR"]
    on_base_pct = hit_pct + prob_dict["BB"] + prob_dict["HBP"] + prob_dict["IBB"]
    ab_pct = 1.0 - prob_dict["BB"] - prob_dict["HBP"] - prob_dict["IBB"]
    xba = hit_pct / ab_pct if ab_pct > 0 else 0
    tb = (prob_dict["1B"] + prob_dict["2B"] * 2 + prob_dict["3B"] * 3 + prob_dict["HR"] * 4)
    xslg = tb / ab_pct if ab_pct > 0 else 0

    summary = {
        "k_pct": round(prob_dict["K"], 3),
        "bb_pct": round(prob_dict["BB"] + prob_dict["IBB"], 3),
        "hit_pct": round(hit_pct, 3),
        "hr_pct": round(prob_dict["HR"], 3),
        "obp": round(on_base_pct, 3),
        "xba": round(xba, 3),
        "xslg": round(xslg, 3),
    }

    return {
        "ok": True,
        "probs": prob_dict,
        "summary": summary,
        "arsenal": arsenal_list,
    }
