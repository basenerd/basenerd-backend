#!/usr/bin/env python3
"""
Train the matchup prediction model.

Input: data/matchup_train.parquet
Output: models/matchup_model.joblib + models/matchup_model_meta.json

Model: XGBoost multiclass classifier
Target: PA outcome (K, BB, HBP, 1B, 2B, 3B, HR, OUT, IBB)
Features: Batter profile + pitcher arsenal + platoon + park + context

Outputs class probabilities for each PA, which feed into the game simulator.
"""

import os
import sys
import json
import joblib
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import classification_report, log_loss
from sklearn.preprocessing import LabelEncoder

try:
    import xgboost as xgb
except ImportError:
    print("xgboost not installed. Run: pip install xgboost")
    sys.exit(1)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
MODEL_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "models")
MATCHUP_DATA_PATH = os.path.join(OUTPUT_DIR, "matchup_train.parquet")
MODEL_PATH = os.path.join(MODEL_DIR, "matchup_model.joblib")
META_PATH = os.path.join(MODEL_DIR, "matchup_model_meta.json")

# Features to use (must exist in matchup_train.parquet)
NUMERIC_FEATURES = [
    # Batter overall profile
    "bat_k_pct", "bat_bb_pct", "bat_whiff_rate", "bat_chase_rate",
    "bat_zone_swing_rate", "bat_zone_contact_rate",
    "bat_avg_ev", "bat_avg_la", "bat_barrel_rate", "bat_hard_hit_rate",
    "bat_sweet_spot_rate", "bat_gb_rate", "bat_fb_rate",
    "bat_hr_per_fb", "bat_iso", "bat_babip", "bat_xwoba",
    # Batter platoon-specific
    "bat_plat_k_pct", "bat_plat_bb_pct", "bat_plat_whiff_rate",
    "bat_plat_chase_rate", "bat_plat_avg_ev", "bat_plat_barrel_rate",
    "bat_plat_xwoba",
    # Batter vs pitch-type category (per category)
    "bvpt_whiff_rate_fastball", "bvpt_chase_rate_fastball",
    "bvpt_zone_contact_rate_fastball", "bvpt_hard_hit_rate_fastball",
    "bvpt_xwoba_fastball",
    "bvpt_whiff_rate_breaking", "bvpt_chase_rate_breaking",
    "bvpt_zone_contact_rate_breaking", "bvpt_hard_hit_rate_breaking",
    "bvpt_xwoba_breaking",
    "bvpt_whiff_rate_offspeed", "bvpt_chase_rate_offspeed",
    "bvpt_zone_contact_rate_offspeed", "bvpt_hard_hit_rate_offspeed",
    "bvpt_xwoba_offspeed",
    # Pitch-weighted composite (batter stats weighted by pitcher's pitch mix)
    "bvpt_w_whiff_rate", "bvpt_w_chase_rate",
    "bvpt_w_zone_contact_rate", "bvpt_w_hard_hit_rate", "bvpt_w_xwoba",
    # Pitcher usage by category
    "p_usage_fastball", "p_usage_breaking", "p_usage_offspeed",
    # Pitcher aggregate
    "p_avg_stuff_plus", "p_avg_control_plus", "p_avg_velo",
    "p_whiff_rate", "p_chase_rate", "p_zone_rate", "p_xwoba",
    "p_num_pitches", "p_total_thrown",
    # Pitcher top-3 pitch stats
    "p_pitch1_usage", "p_pitch1_velo", "p_pitch1_whiff", "p_pitch1_stuff",
    "p_pitch2_usage", "p_pitch2_velo", "p_pitch2_whiff", "p_pitch2_stuff",
    "p_pitch3_usage", "p_pitch3_velo", "p_pitch3_whiff", "p_pitch3_stuff",
    # Park factors
    "park_run_factor", "park_hr_factor",
    # Context
    "inning", "outs_when_up", "n_thruorder_pitcher",
    "runner_on_1b", "runner_on_2b", "runner_on_3b",
    # Recent form (rolling 14-day)
    "bat_r14_k_pct", "bat_r14_bb_pct", "bat_r14_xwoba",
    "bat_r14_barrel_rate", "bat_r14_whiff_rate", "bat_r14_chase_rate",
    "p_r14_k_pct", "p_r14_bb_pct", "p_r14_xwoba",
    "p_r14_whiff_rate", "p_r14_chase_rate",
]

CATEGORICAL_FEATURES = [
    "stand",      # L/R
    "p_throws",   # L/R
]


def train_matchup_model():
    print("Loading matchup training data...")
    df = pd.read_parquet(MATCHUP_DATA_PATH)
    print(f"  {len(df):,} plate appearances")

    # Filter to features that actually exist
    available_num = [f for f in NUMERIC_FEATURES if f in df.columns]
    available_cat = [f for f in CATEGORICAL_FEATURES if f in df.columns]
    missing = set(NUMERIC_FEATURES) - set(available_num)
    if missing:
        print(f"  Missing numeric features (will be NaN): {missing}")

    all_features = available_num + available_cat

    # Encode target
    le = LabelEncoder()
    df["target"] = le.fit_transform(df["outcome"])
    class_names = le.classes_.tolist()
    print(f"  Classes: {class_names}")

    # Convert features to float32 in-place to save memory, then drop unused cols
    for col in available_num:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
    for col in available_cat:
        df[col] = df[col].astype("category").cat.codes.astype("float32")

    # Keep only what we need
    keep = all_features + ["target", "season"]
    df = df[keep].copy()

    # Split: train on 2021-(N-1), holdout test on latest season N.
    # Use a random 10% of training data as validation for early stopping,
    # since the latest season may be too small early in the year.
    max_season = int(df["season"].max())
    train_mask = df["season"] <= (max_season - 1)
    test_mask = df["season"] == max_season

    X_full_train = df.loc[train_mask, all_features]
    y_full_train = df.loc[train_mask, "target"]
    X_test = df.loc[test_mask, all_features]
    y_test = df.loc[test_mask, "target"]
    del df

    # Hold out 10% of training data for early stopping validation
    from sklearn.model_selection import train_test_split as _tts
    X_train, X_val, y_train, y_val = _tts(
        X_full_train, y_full_train, test_size=0.1, random_state=42
    )
    del X_full_train, y_full_train

    print(f"\n  Train: {len(X_train):,} PAs (2021-{max_season - 1}, 90%)")
    print(f"  Val:   {len(X_val):,} PAs (2021-{max_season - 1}, 10% holdout)")
    print(f"  Test:  {len(X_test):,} PAs ({max_season})")

    # Train XGBoost
    # Tuning notes (2026-03-15):
    # - min_child_weight reduced from 100 to 10 so the model can differentiate
    #   rare-event rates (HR, 3B, HBP) between elite and weak hitters.
    #   At 100, each HR-class leaf needed ~3300 samples, crushing all HR preds
    #   toward the ~3.3% league average.
    # - max_depth increased from 4 to 6 for richer batter×pitcher interactions
    # - Removed 300k sample cap (hist method handles full dataset efficiently)
    # - early_stopping_rounds increased to 20 to let the model converge
    print("\nTraining XGBoost multiclass model...")
    model = xgb.XGBClassifier(
        objective="multi:softprob",
        num_class=len(class_names),
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.7,
        min_child_weight=10,
        reg_alpha=0.05,
        reg_lambda=0.5,
        eval_metric="mlogloss",
        early_stopping_rounds=20,
        n_jobs=2,
        random_state=42,
        tree_method="hist",
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=50,
    )

    # Evaluate
    y_pred_proba = model.predict_proba(X_test)
    y_pred = model.predict(X_test)

    print(f"\nTest log loss: {log_loss(y_test, y_pred_proba):.4f}")
    print(f"\nClassification report:")
    print(classification_report(y_test, y_pred, target_names=class_names))

    # Feature importance
    importance = pd.DataFrame({
        "feature": all_features,
        "importance": model.feature_importances_,
    }).sort_values("importance", ascending=False)
    print(f"\nTop 15 features:")
    print(importance.head(15).to_string(index=False))

    # Predicted vs actual outcome rates
    print(f"\nOutcome rate comparison (test set):")
    # Compute actual rates from y_test (encoded) by inverting label encoding
    actual_counts = pd.Series(y_test.values).value_counts(normalize=True).sort_index()
    actual_rates = pd.Series([actual_counts.get(i, 0) for i in range(len(class_names))], index=class_names)
    pred_rates = pd.Series(y_pred_proba.mean(axis=0), index=class_names).sort_index()
    comparison = pd.DataFrame({"actual": actual_rates, "predicted": pred_rates})
    print(comparison.to_string())

    # --- Per-decile calibration check for all outcome classes ---
    # Verifies the model properly separates talent levels for each event type
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
        print(f"\n{cls_name} calibration by predicted-probability decile:")
        print(f"  {'Bin':<24s}  {'N':>7s}  {'Pred%':>8s}  {'Actual%':>10s}")
        for bin_label, row in calibration.iterrows():
            print(f"  {str(bin_label):<24s}  {row['n']:>7.0f}  {row['avg_pred']*100:>7.2f}%  {row['avg_actual']*100:>9.2f}%")

        print(f"  Range: {cls_pred.min()*100:.2f}% — {cls_pred.max()*100:.2f}%  |  Std: {cls_pred.std()*100:.2f}%")

    # Save model
    os.makedirs(MODEL_DIR, exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    print(f"\nModel saved to {MODEL_PATH}")

    # Save metadata
    meta = {
        "model_type": "xgboost_multiclass",
        "target": "pa_outcome",
        "classes": class_names,
        "numeric_features": available_num,
        "categorical_features": available_cat,
        "train_seasons": list(range(2021, max_season)),
        "test_season": max_season,
        "test_log_loss": float(log_loss(y_test, y_pred_proba)),
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "n_estimators_used": int(model.best_iteration + 1) if hasattr(model, "best_iteration") and model.best_iteration is not None else 500,
        "label_encoding": {name: int(idx) for idx, name in enumerate(class_names)},
    }
    with open(META_PATH, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"Metadata saved to {META_PATH}")


if __name__ == "__main__":
    train_matchup_model()
