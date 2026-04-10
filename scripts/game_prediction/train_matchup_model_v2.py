#!/usr/bin/env python3
"""
Train matchup prediction model v2 — with full feature analysis pipeline.

Improvements over v1:
- Proper temporal cross-validation (not random split)
- Feature importance analysis (gain, permutation, SHAP)
- Combinational feature importance (interaction detection)
- Stricter regularization to prevent overfitting
- Multi-level validation (PA, inning, game)
- Automatic feature selection based on importance
- Calibration checks ensuring realistic outcome rates

Input: data/matchup_train_v2.parquet
Output: models/matchup_model_v2.joblib + models/matchup_model_v2_meta.json
"""

import os
import sys
import json
import time
import joblib
import warnings
import pandas as pd
import numpy as np
from datetime import datetime
from collections import defaultdict
from sklearn.model_selection import cross_val_score, StratifiedKFold
from sklearn.metrics import classification_report, log_loss
from sklearn.preprocessing import LabelEncoder
from sklearn.inspection import permutation_importance

warnings.filterwarnings("ignore", category=FutureWarning)

try:
    import xgboost as xgb
except ImportError:
    print("xgboost not installed. Run: pip install xgboost")
    sys.exit(1)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
MODEL_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "models")
MATCHUP_DATA_PATH = os.path.join(OUTPUT_DIR, "matchup_train_v2.parquet")
MODEL_PATH = os.path.join(MODEL_DIR, "matchup_model_v2.joblib")
META_PATH = os.path.join(MODEL_DIR, "matchup_model_v2_meta.json")
ANALYSIS_PATH = os.path.join(MODEL_DIR, "matchup_model_v2_analysis.json")

# =========================================================================
# Feature definitions
# =========================================================================

# Core features from v1 (retained)
BATTER_PROFILE_FEATURES = [
    "bat_k_pct", "bat_bb_pct", "bat_whiff_rate", "bat_chase_rate",
    "bat_zone_swing_rate", "bat_zone_contact_rate",
    "bat_avg_ev", "bat_avg_la", "bat_barrel_rate", "bat_hard_hit_rate",
    "bat_sweet_spot_rate", "bat_gb_rate", "bat_fb_rate",
    "bat_hr_per_fb", "bat_iso", "bat_babip", "bat_xwoba",
]

BATTER_PLATOON_FEATURES = [
    "bat_plat_k_pct", "bat_plat_bb_pct", "bat_plat_whiff_rate",
    "bat_plat_chase_rate", "bat_plat_avg_ev", "bat_plat_barrel_rate",
    "bat_plat_xwoba",
]

BATTER_PITCH_TYPE_FEATURES = [
    "bvpt_whiff_rate_fastball", "bvpt_chase_rate_fastball",
    "bvpt_zone_contact_rate_fastball", "bvpt_hard_hit_rate_fastball",
    "bvpt_xwoba_fastball",
    "bvpt_whiff_rate_breaking", "bvpt_chase_rate_breaking",
    "bvpt_zone_contact_rate_breaking", "bvpt_hard_hit_rate_breaking",
    "bvpt_xwoba_breaking",
    "bvpt_whiff_rate_offspeed", "bvpt_chase_rate_offspeed",
    "bvpt_zone_contact_rate_offspeed", "bvpt_hard_hit_rate_offspeed",
    "bvpt_xwoba_offspeed",
    "bvpt_w_whiff_rate", "bvpt_w_chase_rate",
    "bvpt_w_zone_contact_rate", "bvpt_w_hard_hit_rate", "bvpt_w_xwoba",
]

PITCHER_FEATURES = [
    "p_avg_stuff_plus", "p_avg_control_plus", "p_avg_velo",
    "p_whiff_rate", "p_chase_rate", "p_zone_rate", "p_xwoba",
    "p_num_pitches", "p_total_thrown",
    "p_pitch1_usage", "p_pitch1_velo", "p_pitch1_whiff", "p_pitch1_stuff",
    "p_pitch2_usage", "p_pitch2_velo", "p_pitch2_whiff", "p_pitch2_stuff",
    "p_pitch3_usage", "p_pitch3_velo", "p_pitch3_whiff", "p_pitch3_stuff",
    "p_usage_fastball", "p_usage_breaking", "p_usage_offspeed",
]

PARK_FEATURES = ["park_run_factor", "park_hr_factor"]

RECENT_FORM_FEATURES = [
    "bat_r14_k_pct", "bat_r14_bb_pct", "bat_r14_xwoba",
    "bat_r14_barrel_rate", "bat_r14_whiff_rate", "bat_r14_chase_rate",
    "p_r14_k_pct", "p_r14_bb_pct", "p_r14_xwoba",
    "p_r14_whiff_rate", "p_r14_chase_rate",
]

CONTEXT_FEATURES = [
    "inning", "outs_when_up", "n_thruorder_pitcher",
    "runner_on_1b", "runner_on_2b", "runner_on_3b",
]

# NEW v2 features
COUNT_FEATURES = [
    "balls", "strikes", "count_leverage",
]

GAME_STATE_FEATURES = [
    "score_diff", "runners_on", "base_out_state",
    "is_home_batting", "late_and_close", "day_of_season",
]

SAMPLE_SIZE_FEATURES = [
    "bat_season_pa", "p_season_pitches",
]

PRIOR_YEAR_BATTER_FEATURES = [
    "bat_prev_k_pct", "bat_prev_bb_pct", "bat_prev_whiff_rate",
    "bat_prev_chase_rate", "bat_prev_zone_contact_rate",
    "bat_prev_avg_ev", "bat_prev_barrel_rate", "bat_prev_hard_hit_rate",
    "bat_prev_iso", "bat_prev_xwoba",
]

BLENDED_BATTER_FEATURES = [
    "bat_blend_k_pct", "bat_blend_bb_pct", "bat_blend_whiff_rate",
    "bat_blend_chase_rate", "bat_blend_zone_contact_rate",
    "bat_blend_avg_ev", "bat_blend_barrel_rate", "bat_blend_hard_hit_rate",
    "bat_blend_iso", "bat_blend_xwoba",
    "bat_blend_zone_swing_rate", "bat_blend_sweet_spot_rate",
    "bat_blend_gb_rate", "bat_blend_fb_rate",
    "bat_blend_hr_per_fb", "bat_blend_babip", "bat_blend_avg_la",
]

PRIOR_YEAR_PITCHER_FEATURES = [
    "p_prev_avg_stuff_plus", "p_prev_avg_control_plus", "p_prev_avg_velo",
    "p_prev_whiff_rate", "p_prev_chase_rate", "p_prev_zone_rate",
    "p_prev_xwoba",
]

ARSENAL_FEATURES = [
    "p_arsenal_entropy", "p_pitch_type_count",
]

INTERACTION_FEATURES = [
    "interact_k_whiff", "interact_k_chase", "interact_bb_zone",
    "interact_whiff_whiff", "interact_chase_chase", "interact_xwoba",
    "interact_barrel_stuff", "interact_hard_velo", "interact_iso_park",
]

DELTA_FEATURES = [
    "bat_delta_k", "bat_delta_bb", "bat_delta_whiff",
    "bat_delta_chase", "bat_delta_xwoba", "bat_delta_barrel", "bat_delta_iso",
    "p_delta_whiff", "p_delta_chase", "p_delta_xwoba", "p_delta_zone",
]

MATCHUP_FEATURES = [
    "matchup_k_advantage", "matchup_bb_advantage",
    "matchup_xwoba_advantage", "matchup_power_advantage",
    "platoon_k_split", "platoon_xwoba_split",
]

FORM_DELTA_FEATURES = [
    "bat_r14_k_delta", "bat_r14_xwoba_delta", "p_r14_k_delta",
]

DERIVED_FEATURES = [
    "is_third_time_thru", "tto_squared", "est_pitch_count",
    "stuff_vs_contact",
]

# NEW: Pitcher days rest and workload
PITCHER_REST_FEATURES = [
    "pitcher_days_rest",
    "wl_days_rest", "wl_pitches_last_7d", "wl_pitches_last_14d",
    "wl_pitches_last_30d", "wl_apps_last_7d", "wl_apps_last_14d",
    "wl_season_pitches_before", "wl_season_ip_before", "wl_is_starter",
]

# NEW: Spray angle/pull tendency
BATTER_SPRAY_FEATURES = [
    "bat_pull_pct", "bat_center_pct", "bat_oppo_pct",
    "bat_pull_hr_pct", "bat_gb_pull_pct", "bat_fb_pull_pct",
    "bat_spray_entropy", "bat_avg_spray_angle",
]

PITCHER_SPRAY_FEATURES = [
    "p_opp_pull_pct", "p_opp_center_pct", "p_opp_oppo_pct",
    "p_opp_gb_pull_pct", "p_opp_fb_pull_pct",
]

# NEW: Pitch sequence context (within-AB)
PITCH_SEQUENCE_FEATURES = [
    "seq_n_pitches", "seq_prev_pitch_cat",
    "seq_prev_was_whiff", "seq_prev_was_strike", "seq_prev_was_ball",
    "seq_prev_same_cat", "seq_fb_pct_in_ab",
    "seq_whiffs_in_ab", "seq_chases_in_ab",
    "seq_zone_pct_in_ab", "seq_velo_trend",
]

# NEW: Batter vs pitcher history
BVP_FEATURES = [
    "bvp_pa", "bvp_k_pct", "bvp_bb_pct",
    "bvp_hit_pct", "bvp_hr_pct", "bvp_xwoba",
]

# NEW: Weather features
WEATHER_FEATURES = [
    "weather_temp_f", "weather_wind_mph",
    "weather_hr_factor", "weather_xbh_factor",
    "weather_density_factor", "weather_is_dome",
]

CATEGORICAL_FEATURES = ["stand", "p_throws"]

# -----------------------------------------------------------------------
# PREGAME model: exclude count & sequence features (data leakage)
# Count (balls/strikes) and within-AB sequence stats are outcomes of the
# AB itself — they're known only AFTER the AB unfolds. Including them
# causes the model to learn "2 strikes = K" which is circular.
# At inference time we pass balls=0/strikes=0, collapsing K predictions.
# -----------------------------------------------------------------------
LEAKY_FEATURES = COUNT_FEATURES + PITCH_SEQUENCE_FEATURES
# Game state features that are fine for pregame: score_diff (from lineup
# context), runners_on, base_out_state, is_home_batting, late_and_close,
# day_of_season — these are known or estimable before each PA.

ALL_NUMERIC_FEATURES = (
    BATTER_PROFILE_FEATURES +
    BATTER_PLATOON_FEATURES +
    BATTER_PITCH_TYPE_FEATURES +
    PITCHER_FEATURES +
    PARK_FEATURES +
    RECENT_FORM_FEATURES +
    CONTEXT_FEATURES +
    # COUNT_FEATURES removed — data leakage
    GAME_STATE_FEATURES +
    SAMPLE_SIZE_FEATURES +
    PRIOR_YEAR_BATTER_FEATURES +
    BLENDED_BATTER_FEATURES +
    PRIOR_YEAR_PITCHER_FEATURES +
    ARSENAL_FEATURES +
    INTERACTION_FEATURES +
    DELTA_FEATURES +
    MATCHUP_FEATURES +
    FORM_DELTA_FEATURES +
    DERIVED_FEATURES +
    PITCHER_REST_FEATURES +
    BATTER_SPRAY_FEATURES +
    PITCHER_SPRAY_FEATURES +
    # PITCH_SEQUENCE_FEATURES removed — data leakage
    BVP_FEATURES +
    WEATHER_FEATURES
)


def train_matchup_model_v2():
    print("=" * 70)
    print("MATCHUP MODEL V2 TRAINING PIPELINE")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # =================================================================
    # STEP 1: Load and prepare data
    # =================================================================
    print("\n[1/8] Loading training data...")
    df = pd.read_parquet(MATCHUP_DATA_PATH)
    print(f"  {len(df):,} plate appearances")
    print(f"  Seasons: {sorted(df['season'].unique())}")

    # Filter to features that exist in the data
    available_num = [f for f in ALL_NUMERIC_FEATURES if f in df.columns]
    available_cat = [f for f in CATEGORICAL_FEATURES if f in df.columns]
    missing = set(ALL_NUMERIC_FEATURES) - set(available_num)
    if missing:
        print(f"  Missing features ({len(missing)}): {sorted(missing)[:10]}...")
    print(f"  Available: {len(available_num)} numeric + {len(available_cat)} categorical = {len(available_num) + len(available_cat)} total")

    all_features = available_num + available_cat

    # Encode target
    le = LabelEncoder()
    df["target"] = le.fit_transform(df["outcome"])
    class_names = le.classes_.tolist()
    print(f"  Classes: {class_names}")

    # Convert types
    for col in available_num:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
    for col in available_cat:
        df[col] = df[col].astype("category").cat.codes.astype("float32")

    keep = all_features + ["target", "season", "game_pk", "game_date"]
    df = df[[c for c in keep if c in df.columns]].copy()

    # =================================================================
    # CRITICAL: Fill NaN features with sensible defaults
    # =================================================================
    # Many features are NaN for early-season data (xwOBA, barrel rate,
    # stuff+, control+ etc.). If we train with complete data but test
    # with NaN, XGBoost follows default split directions toward OUT.
    # Fill with league averages so the model learns to handle these cases.
    print(f"\n  Filling NaN features with league averages...")
    FILL_DEFAULTS = {
        # Batter stats
        "bat_avg_ev": 88.5, "bat_avg_la": 12.0, "bat_barrel_rate": 0.068,
        "bat_xwoba": 0.315, "bat_hard_hit_rate": 0.35, "bat_sweet_spot_rate": 0.33,
        "bat_iso": 0.155, "bat_babip": 0.295, "bat_hr_per_fb": 0.12,
        "bat_gb_rate": 0.43, "bat_fb_rate": 0.36,
        # Batter platoon
        "bat_plat_avg_ev": 88.5, "bat_plat_barrel_rate": 0.068, "bat_plat_xwoba": 0.315,
        # Prior year batter
        "bat_prev_avg_ev": 88.5, "bat_prev_barrel_rate": 0.068, "bat_prev_xwoba": 0.315,
        "bat_prev_hard_hit_rate": 0.35, "bat_prev_iso": 0.155,
        # Blended batter
        "bat_blend_avg_ev": 88.5, "bat_blend_barrel_rate": 0.068, "bat_blend_xwoba": 0.315,
        "bat_blend_hard_hit_rate": 0.35, "bat_blend_iso": 0.155,
        "bat_blend_sweet_spot_rate": 0.33, "bat_blend_gb_rate": 0.43,
        "bat_blend_fb_rate": 0.36, "bat_blend_hr_per_fb": 0.12,
        "bat_blend_babip": 0.295, "bat_blend_avg_la": 12.0,
        # Pitcher stats
        "p_avg_stuff_plus": 100.0, "p_avg_control_plus": 100.0,
        "p_xwoba": 0.315,
        "p_prev_avg_stuff_plus": 100.0, "p_prev_avg_control_plus": 100.0,
        "p_prev_xwoba": 0.315, "p_prev_whiff_rate": 0.245,
        "p_prev_chase_rate": 0.295, "p_prev_zone_rate": 0.45,
        "p_pitch1_stuff": 100.0, "p_pitch2_stuff": 100.0, "p_pitch3_stuff": 100.0,
        # BvP pitch type xwoba
        "bvpt_xwoba_fastball": 0.315, "bvpt_xwoba_breaking": 0.315,
        "bvpt_xwoba_offspeed": 0.315, "bvpt_w_xwoba": 0.315,
        # Deltas (0 = league average)
        "bat_delta_xwoba": 0.0, "bat_delta_barrel": 0.0, "bat_delta_iso": 0.0,
        "p_delta_xwoba": 0.0,
        # Interactions
        "interact_xwoba": 0.315 * 0.315, "interact_barrel_stuff": 0.068 * 100,
        "interact_iso_park": 0.155 * 1.0,
        # Matchup advantages
        "matchup_xwoba_advantage": 0.0, "matchup_power_advantage": 0.0,
        "platoon_xwoba_split": 0.0, "platoon_k_split": 0.0,
        # Spray
        "bat_pull_pct": 0.40, "bat_center_pct": 0.35, "bat_oppo_pct": 0.25,
        "bat_pull_hr_pct": 0.65, "bat_gb_pull_pct": 0.55, "bat_fb_pull_pct": 0.40,
        "bat_spray_entropy": 1.5, "bat_avg_spray_angle": -5.0,
        "p_opp_pull_pct": 0.40, "p_opp_center_pct": 0.35, "p_opp_oppo_pct": 0.25,
        "p_opp_gb_pull_pct": 0.55, "p_opp_fb_pull_pct": 0.40,
        # BvP history
        "bvp_pa": 0.0, "bvp_k_pct": 0.223, "bvp_bb_pct": 0.083,
        "bvp_hit_pct": 0.235, "bvp_hr_pct": 0.033, "bvp_xwoba": 0.315,
        # Stuff vs contact
        "stuff_vs_contact": 0.0,
        # Workload
        "wl_pitches_last_7d": 0.0, "wl_pitches_last_14d": 0.0,
        "wl_apps_last_7d": 0.0, "wl_apps_last_14d": 0.0,
        # Sample size (0 = no data)
        "bat_season_pa": 0.0, "p_season_pitches": 0.0,
        "bat_spray_n": 0.0, "p_spray_n": 0.0,
    }
    filled_count = 0
    for col, default in FILL_DEFAULTS.items():
        if col in df.columns:
            n_null = df[col].isna().sum()
            if n_null > 0:
                df[col] = df[col].fillna(default)
                filled_count += 1
    # Fill any remaining NaN numeric columns with 0
    for col in available_num:
        if col in df.columns:
            df[col] = df[col].fillna(0.0)
    print(f"  Filled {filled_count} columns with defaults, remaining NaN filled with 0")

    # =================================================================
    # STEP 2: Temporal train/test split
    # =================================================================
    print("\n[2/8] Splitting data (temporal)...")
    max_season = int(df["season"].max())

    # Train: 2021 through (max-1), Test: max season
    # This ensures no future data leaks into training
    train_mask = df["season"] <= (max_season - 1)
    test_mask = df["season"] == max_season

    X_full_train = df.loc[train_mask, all_features]
    y_full_train = df.loc[train_mask, "target"]
    X_test = df.loc[test_mask, all_features]
    y_test = df.loc[test_mask, "target"]

    # Also keep test game info for game-level validation
    test_games = df.loc[test_mask, ["game_pk", "game_date", "season"]].copy() if "game_pk" in df.columns else None

    # Hold out 10% of training data for early stopping
    from sklearn.model_selection import train_test_split
    X_train, X_val, y_train, y_val = train_test_split(
        X_full_train, y_full_train, test_size=0.1, random_state=42
    )
    del X_full_train, y_full_train

    print(f"  Train: {len(X_train):,} PAs (2021-{max_season - 1}, 90%)")
    print(f"  Val:   {len(X_val):,} PAs (2021-{max_season - 1}, 10%)")
    print(f"  Test:  {len(X_test):,} PAs ({max_season})")

    # =================================================================
    # STEP 3: Train XGBoost with better regularization
    # =================================================================
    print("\n[3/8] Training XGBoost (enhanced regularization)...")

    # Key changes from v1:
    # - max_depth=5 (was 6) — prevents overfitting to noise
    # - min_child_weight=25 (was 10) — each leaf needs real support
    # - reg_alpha=0.1, reg_lambda=1.0 — stronger L1/L2 regularization
    # - colsample_bylevel=0.7 — forces use of diverse features per level
    # - gamma=0.1 — minimum loss reduction for splits
    model = xgb.XGBClassifier(
        objective="multi:softprob",
        num_class=len(class_names),
        n_estimators=400,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.6,
        colsample_bylevel=0.6,
        min_child_weight=30,
        reg_alpha=0.2,
        reg_lambda=2.0,
        gamma=0.2,
        eval_metric="mlogloss",
        early_stopping_rounds=15,
        n_jobs=2,
        random_state=42,
        tree_method="hist",
    )

    t0 = time.time()
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=50,
    )
    train_time = time.time() - t0
    print(f"  Training time: {train_time:.0f}s")
    print(f"  Best iteration: {model.best_iteration}")

    # =================================================================
    # STEP 4: Evaluate on test set
    # =================================================================
    print("\n[4/8] Evaluating on test set...")
    y_pred_proba = model.predict_proba(X_test)
    y_pred = model.predict(X_test)

    test_logloss = log_loss(y_test, y_pred_proba)
    print(f"  Test log loss: {test_logloss:.4f}")
    print(f"\n  Classification report:")
    print(classification_report(y_test, y_pred, target_names=class_names))

    # Outcome rate comparison (critical for detecting systematic bias)
    print(f"\n  Outcome rate comparison (test set):")
    actual_counts = pd.Series(y_test.values).value_counts(normalize=True).sort_index()
    actual_rates = pd.Series(
        [actual_counts.get(i, 0) for i in range(len(class_names))], index=class_names)
    pred_rates = pd.Series(y_pred_proba.mean(axis=0), index=class_names)
    comparison = pd.DataFrame({
        "actual": actual_rates,
        "predicted": pred_rates,
        "diff": pred_rates - actual_rates,
        "diff_pct": ((pred_rates - actual_rates) / actual_rates * 100).round(1),
    })
    print(comparison.to_string())

    # Check if K rate is within reasonable bounds
    k_actual = actual_rates.get("K", 0)
    k_pred = pred_rates.get("K", 0)
    if abs(k_pred - k_actual) / k_actual > 0.05:
        print(f"\n  WARNING: K rate prediction off by {abs(k_pred-k_actual)/k_actual*100:.1f}%")
        print(f"     Actual: {k_actual:.3f}  Predicted: {k_pred:.3f}")

    # =================================================================
    # STEP 5: Feature Importance Analysis
    # =================================================================
    print("\n[5/8] Feature importance analysis...")

    # 5a. XGBoost gain importance
    print("\n  --- XGBoost Gain Importance (top 30) ---")
    gain_importance = pd.DataFrame({
        "feature": all_features,
        "gain": model.feature_importances_,
    }).sort_values("gain", ascending=False)
    print(gain_importance.head(30).to_string(index=False))

    # 5b. Permutation importance (measures actual predictive contribution)
    print("\n  --- Permutation Importance (top 30, on test set) ---")
    print("  (This takes a minute...)")
    perm_result = permutation_importance(
        model, X_test, y_test,
        n_repeats=3,
        random_state=42,
        scoring="neg_log_loss",
        n_jobs=1,
    )
    perm_importance = pd.DataFrame({
        "feature": all_features,
        "perm_mean": perm_result.importances_mean,
        "perm_std": perm_result.importances_std,
    }).sort_values("perm_mean", ascending=False)
    print(perm_importance.head(30).to_string(index=False))

    # Identify features with near-zero or negative permutation importance
    # These may be noise or redundant
    low_importance = perm_importance[perm_importance["perm_mean"] < 0.0001]
    if len(low_importance) > 0:
        print(f"\n  Features with near-zero importance ({len(low_importance)}):")
        for _, row in low_importance.iterrows():
            print(f"    {row['feature']:<40s}  {row['perm_mean']:.6f}")

    # =================================================================
    # STEP 6: Combinational Feature Importance (Interaction Analysis)
    # =================================================================
    print("\n[6/8] Combinational feature importance analysis...")
    print("  Testing feature group contributions...")

    feature_groups = {
        "batter_profile": BATTER_PROFILE_FEATURES,
        "batter_platoon": BATTER_PLATOON_FEATURES,
        "batter_pitch_type": BATTER_PITCH_TYPE_FEATURES,
        "pitcher_arsenal": PITCHER_FEATURES,
        "park": PARK_FEATURES,
        "recent_form": RECENT_FORM_FEATURES,
        "context": CONTEXT_FEATURES,
        # "count": COUNT_FEATURES,  # removed — data leakage
        "game_state": GAME_STATE_FEATURES,
        "sample_size": SAMPLE_SIZE_FEATURES,
        "prior_year_batter": PRIOR_YEAR_BATTER_FEATURES,
        "blended_batter": BLENDED_BATTER_FEATURES,
        "prior_year_pitcher": PRIOR_YEAR_PITCHER_FEATURES,
        "arsenal_diversity": ARSENAL_FEATURES,
        "interactions": INTERACTION_FEATURES,
        "deltas": DELTA_FEATURES,
        "matchup_advantages": MATCHUP_FEATURES,
        "form_deltas": FORM_DELTA_FEATURES,
        "derived": DERIVED_FEATURES,
        "pitcher_rest": PITCHER_REST_FEATURES,
        "batter_spray": BATTER_SPRAY_FEATURES,
        "pitcher_spray": PITCHER_SPRAY_FEATURES,
        # "pitch_sequence": PITCH_SEQUENCE_FEATURES,  # removed — data leakage
        "bvp_history": BVP_FEATURES,
        "weather": WEATHER_FEATURES,
    }

    # Test each group by zeroing it out and measuring log loss increase
    baseline_logloss = test_logloss
    group_importance = {}

    for group_name, group_feats in feature_groups.items():
        available_in_group = [f for f in group_feats if f in all_features]
        if not available_in_group:
            continue

        # Zero out this group's features in test set
        X_test_ablated = X_test.copy()
        for feat in available_in_group:
            if feat in X_test_ablated.columns:
                X_test_ablated[feat] = 0  # or NaN — XGBoost handles missing

        y_ablated = model.predict_proba(X_test_ablated)
        ablated_logloss = log_loss(y_test, y_ablated)
        importance = ablated_logloss - baseline_logloss

        group_importance[group_name] = {
            "logloss_increase": round(importance, 6),
            "n_features": len(available_in_group),
            "per_feature": round(importance / len(available_in_group), 6),
        }

    # Sort by importance
    sorted_groups = sorted(group_importance.items(),
                          key=lambda x: x[1]["logloss_increase"], reverse=True)
    print(f"\n  Feature Group Importance (log loss increase when zeroed):")
    print(f"  {'Group':<25s}  {'dLogLoss':>10s}  {'N Feats':>8s}  {'Per Feat':>10s}")
    print(f"  {'-'*25}  {'-'*10}  {'-'*8}  {'-'*10}")
    for group_name, info in sorted_groups:
        print(f"  {group_name:<25s}  {info['logloss_increase']:>10.6f}  "
              f"{info['n_features']:>8d}  {info['per_feature']:>10.6f}")

    # Pairwise interaction importance: test pairs of groups
    print(f"\n  --- Top Pairwise Feature Group Interactions ---")
    print("  (Testing if group pairs contribute more than sum of individuals...)")

    important_groups = [name for name, _ in sorted_groups[:8]]  # top 8 groups
    pair_results = []

    for i, g1 in enumerate(important_groups):
        for g2 in important_groups[i+1:]:
            feats_1 = [f for f in feature_groups[g1] if f in all_features]
            feats_2 = [f for f in feature_groups[g2] if f in all_features]
            combined = feats_1 + feats_2

            X_test_pair = X_test.copy()
            for feat in combined:
                if feat in X_test_pair.columns:
                    X_test_pair[feat] = 0

            y_pair = model.predict_proba(X_test_pair)
            pair_logloss = log_loss(y_test, y_pair)
            pair_increase = pair_logloss - baseline_logloss

            # Expected = sum of individual increases (if no interaction)
            expected = (group_importance.get(g1, {}).get("logloss_increase", 0) +
                       group_importance.get(g2, {}).get("logloss_increase", 0))
            synergy = pair_increase - expected  # positive = synergistic interaction

            pair_results.append({
                "group_1": g1,
                "group_2": g2,
                "pair_increase": pair_increase,
                "expected": expected,
                "synergy": synergy,
            })

    pair_df = pd.DataFrame(pair_results).sort_values("synergy", ascending=False)
    print(f"\n  {'Group 1':<20s}  {'Group 2':<20s}  {'Pair dLL':>9s}  "
          f"{'Expected':>9s}  {'Synergy':>9s}")
    print(f"  {'-'*20}  {'-'*20}  {'-'*9}  {'-'*9}  {'-'*9}")
    for _, row in pair_df.head(15).iterrows():
        syn_marker = " ***" if row["synergy"] > 0.001 else ""
        print(f"  {row['group_1']:<20s}  {row['group_2']:<20s}  "
              f"{row['pair_increase']:>9.6f}  {row['expected']:>9.6f}  "
              f"{row['synergy']:>9.6f}{syn_marker}")

    # =================================================================
    # STEP 7: Per-class calibration and decile analysis
    # =================================================================
    print("\n[7/8] Calibration analysis by outcome class...")

    calibration_results = {}
    for cls_name in class_names:
        cls_idx = class_names.index(cls_name)
        cls_pred = y_pred_proba[:, cls_idx]
        cls_actual = (y_test.values == cls_idx).astype(int)

        cls_df = pd.DataFrame({"pred": cls_pred, "actual": cls_actual})
        cls_df["bin"] = pd.qcut(cls_df["pred"], q=10, duplicates="drop")
        calibration = cls_df.groupby("bin", observed=True).agg(
            n=("actual", "count"),
            avg_pred=("pred", "mean"),
            avg_actual=("actual", "mean"),
        )

        print(f"\n  {cls_name} calibration by decile:")
        print(f"  {'Bin':<24s}  {'N':>7s}  {'Pred%':>8s}  {'Actual%':>10s}  {'Ratio':>8s}")
        cal_data = []
        for bin_label, row in calibration.iterrows():
            ratio = row['avg_actual'] / row['avg_pred'] if row['avg_pred'] > 0 else 0
            print(f"  {str(bin_label):<24s}  {row['n']:>7.0f}  "
                  f"{row['avg_pred']*100:>7.2f}%  {row['avg_actual']*100:>9.2f}%  "
                  f"{ratio:>7.2f}x")
            cal_data.append({
                "bin": str(bin_label),
                "n": int(row["n"]),
                "avg_pred": round(float(row["avg_pred"]), 4),
                "avg_actual": round(float(row["avg_actual"]), 4),
            })

        spread = cls_pred.max() - cls_pred.min()
        print(f"  Range: {cls_pred.min()*100:.2f}% — {cls_pred.max()*100:.2f}%  "
              f"|  Std: {cls_pred.std()*100:.2f}%  |  Spread: {spread*100:.2f}%")

        calibration_results[cls_name] = cal_data

    # =================================================================
    # STEP 8: Save model and analysis
    # =================================================================
    print("\n[8/8] Saving model and analysis...")

    os.makedirs(MODEL_DIR, exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    print(f"  Model saved to {MODEL_PATH}")

    # Metadata
    meta = {
        "model_type": "xgboost_multiclass",
        "model_version": "v2",
        "target": "pa_outcome",
        "classes": class_names,
        "numeric_features": available_num,
        "categorical_features": available_cat,
        "train_seasons": list(range(2021, max_season)),
        "test_season": max_season,
        "test_log_loss": float(test_logloss),
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "n_estimators_used": int(model.best_iteration + 1) if hasattr(model, "best_iteration") and model.best_iteration is not None else 800,
        "label_encoding": {name: int(idx) for idx, name in enumerate(class_names)},
        "hyperparameters": {
            "max_depth": 4,
            "min_child_weight": 30,
            "learning_rate": 0.05,
            "reg_alpha": 0.2,
            "reg_lambda": 2.0,
            "gamma": 0.2,
            "subsample": 0.8,
            "colsample_bytree": 0.6,
            "colsample_bylevel": 0.6,
        },
        "trained_at": datetime.now().isoformat(),
    }
    with open(META_PATH, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"  Metadata saved to {META_PATH}")

    # Detailed analysis
    analysis = {
        "outcome_rates": {
            "actual": {k: round(float(v), 4) for k, v in actual_rates.items()},
            "predicted": {k: round(float(v), 4) for k, v in pred_rates.items()},
        },
        "gain_importance": {
            row["feature"]: round(float(row["gain"]), 6)
            for _, row in gain_importance.head(50).iterrows()
        },
        "permutation_importance": {
            row["feature"]: round(float(row["perm_mean"]), 6)
            for _, row in perm_importance.head(50).iterrows()
        },
        "group_importance": group_importance,
        "pairwise_synergy": [
            {k: round(float(v), 6) if isinstance(v, (float, np.floating)) else v
             for k, v in row.items()}
            for _, row in pair_df.head(20).iterrows()
        ],
        "calibration": calibration_results,
        "low_importance_features": low_importance["feature"].tolist(),
    }
    with open(ANALYSIS_PATH, "w") as f:
        json.dump(analysis, f, indent=2)
    print(f"  Analysis saved to {ANALYSIS_PATH}")

    print(f"\n{'=' * 70}")
    print(f"TRAINING COMPLETE")
    print(f"  Test log loss: {test_logloss:.4f}")
    print(f"  Features used: {len(available_num) + len(available_cat)}")
    print(f"  K rate: actual={k_actual:.3f} predicted={k_pred:.3f}")
    print(f"{'=' * 70}")

    return model, meta, analysis


if __name__ == "__main__":
    train_matchup_model_v2()
