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

    # Prepare feature matrix
    X = df[all_features].copy()

    # Convert all numeric columns to float (psycopg returns Decimal)
    for col in available_num:
        X[col] = pd.to_numeric(X[col], errors="coerce").astype("float64")

    # Encode categoricals
    for col in available_cat:
        X[col] = X[col].astype("category").cat.codes

    y = df["target"]

    # Train/test split (time-based: train on 2021-2024, test on 2025)
    train_mask = df["season"] <= 2024
    test_mask = df["season"] == 2025

    X_train, X_test = X[train_mask], X[test_mask]
    y_train, y_test = y[train_mask], y[test_mask]

    print(f"\n  Train: {len(X_train):,} PAs (2021-2024)")
    print(f"  Test:  {len(X_test):,} PAs (2025)")

    # Train XGBoost
    print("\nTraining XGBoost multiclass model...")
    model = xgb.XGBClassifier(
        objective="multi:softprob",
        num_class=len(class_names),
        n_estimators=500,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=100,
        reg_alpha=0.1,
        reg_lambda=1.0,
        eval_metric="mlogloss",
        early_stopping_rounds=10,
        n_jobs=-1,
        random_state=42,
        tree_method="hist",  # faster histogram-based method
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
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
    actual_rates = df[test_mask]["outcome"].value_counts(normalize=True).sort_index()
    pred_rates = pd.Series(y_pred_proba.mean(axis=0), index=class_names).sort_index()
    comparison = pd.DataFrame({"actual": actual_rates, "predicted": pred_rates})
    print(comparison.to_string())

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
        "train_seasons": [2021, 2022, 2023, 2024],
        "test_season": 2025,
        "test_log_loss": float(log_loss(y_test, y_pred_proba)),
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "n_estimators_used": int(model.best_iteration + 1) if hasattr(model, "best_iteration") else 500,
        "label_encoding": {name: int(idx) for idx, name in enumerate(class_names)},
    }
    with open(META_PATH, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"Metadata saved to {META_PATH}")


if __name__ == "__main__":
    train_matchup_model()
