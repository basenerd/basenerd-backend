#!/usr/bin/env python3
"""
Train the pitch selection prediction model.

Input: data/pitch_selection_train.parquet + data/pitcher_arsenal.parquet
Output: models/pitch_selection_model.joblib + models/pitch_selection_meta.json

Model: XGBoost multiclass classifier
Target: pitch_type thrown
Features: Count, game state, pitcher identity (via arsenal profile), batter hand,
          previous pitch in AB, times through order

This predicts what pitch types a pitcher will throw in a given situation,
which feeds the matchup model's per-pitch-type outcome predictions.
"""

import os
import sys
import json
import joblib
import pandas as pd
import numpy as np
from sklearn.metrics import classification_report, log_loss
from sklearn.preprocessing import LabelEncoder

try:
    import xgboost as xgb
except ImportError:
    print("xgboost not installed. Run: pip install xgboost")
    sys.exit(1)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
MODEL_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "models")
PITCH_DATA_PATH = os.path.join(OUTPUT_DIR, "pitch_selection_train.parquet")
ARSENAL_PATH = os.path.join(OUTPUT_DIR, "pitcher_arsenal.parquet")
MODEL_PATH = os.path.join(MODEL_DIR, "pitch_selection_model.joblib")
META_PATH = os.path.join(MODEL_DIR, "pitch_selection_meta.json")

# Keep only pitch types with enough samples
MIN_PITCH_TYPE_COUNT = 5000

# Common pitch types to model
VALID_PITCH_TYPES = {"FF", "SI", "FC", "SL", "CU", "CH", "FS", "KC", "ST", "SV", "KN"}


def train_pitch_selection_model():
    print("Loading pitch selection training data...")
    df = pd.read_parquet(PITCH_DATA_PATH)
    print(f"  {len(df):,} pitches")

    # Filter to valid pitch types
    df = df[df["pitch_type"].isin(VALID_PITCH_TYPES)].copy()
    print(f"  {len(df):,} after filtering to valid pitch types")

    # Drop rare pitch types
    type_counts = df["pitch_type"].value_counts()
    keep_types = type_counts[type_counts >= MIN_PITCH_TYPE_COUNT].index.tolist()
    df = df[df["pitch_type"].isin(keep_types)].copy()
    print(f"  {len(df):,} after dropping rare types")
    print(f"  Pitch types: {sorted(keep_types)}")

    # --- Merge pitcher arsenal features ---
    if os.path.exists(ARSENAL_PATH):
        print("Loading pitcher arsenal profiles...")
        arsenal = pd.read_parquet(ARSENAL_PATH)

        # Get pitcher-level stats (ALL handedness, weighted across pitch types)
        arsenal_all = arsenal[arsenal["stand"] == "ALL"].copy()
        pitcher_stats = arsenal_all.groupby(["pitcher", "season"]).agg(
            p_avg_velo=("avg_velo", lambda x: np.average(x.dropna(), weights=arsenal_all.loc[x.dropna().index, "n"])
                        if x.notna().any() else np.nan),
            p_num_pitch_types=("pitch_type", "nunique"),
        ).reset_index()

        # Also get per-pitcher pitch type usage rates as features
        # (pivot: one column per pitch type showing that pitcher's usage rate)
        usage_pivot = arsenal_all.pivot_table(
            index=["pitcher", "season"],
            columns="pitch_type",
            values="usage",
            fill_value=0,
        ).reset_index()
        usage_pivot.columns = [f"arsenal_{c}" if c not in ["pitcher", "season"] else c
                               for c in usage_pivot.columns]

        df = df.merge(pitcher_stats, on=["pitcher", "season"], how="left")
        df = df.merge(usage_pivot, on=["pitcher", "season"], how="left")

    # --- Feature engineering ---
    # Encode categoricals
    le_stand = LabelEncoder()
    df["stand_enc"] = le_stand.fit_transform(df["stand"].fillna("R"))

    le_p_throws = LabelEncoder()
    df["p_throws_enc"] = le_p_throws.fit_transform(df["p_throws"].fillna("R"))

    # Previous pitch type encoding
    le_prev = LabelEncoder()
    df["prev_pitch_valid"] = df["prev_pitch_type"].fillna("NONE")
    # Only keep known types
    df.loc[~df["prev_pitch_valid"].isin(set(keep_types) | {"NONE"}), "prev_pitch_valid"] = "OTHER"
    le_prev.fit(sorted(set(df["prev_pitch_valid"])))
    df["prev_pitch_enc"] = le_prev.transform(df["prev_pitch_valid"])

    # Feature list
    numeric_features = [
        "balls", "strikes",
        "ahead_in_count", "behind_in_count", "two_strikes", "three_balls", "first_pitch",
        "outs_when_up", "inning",
        "early_innings", "mid_innings", "late_innings",
        "runner_on_1b", "runner_on_2b", "runner_on_3b", "runners_on", "risp",
        "score_diff",
        "n_thruorder_pitcher",
        "pitch_num_in_ab",
        "stand_enc", "p_throws_enc", "prev_pitch_enc",
    ]

    # Add arsenal columns if they exist
    arsenal_cols = [c for c in df.columns if c.startswith("arsenal_")]
    numeric_features.extend(arsenal_cols)
    if "p_avg_velo" in df.columns:
        numeric_features.append("p_avg_velo")
    if "p_num_pitch_types" in df.columns:
        numeric_features.append("p_num_pitch_types")

    # Add prev_pitch_velo
    if "prev_pitch_velo" in df.columns:
        numeric_features.append("prev_pitch_velo")

    available_features = [f for f in numeric_features if f in df.columns]

    # Target
    le_target = LabelEncoder()
    df["target"] = le_target.fit_transform(df["pitch_type"])
    class_names = le_target.classes_.tolist()

    # Free the full dataframe columns we no longer need
    keep_cols = available_features + ["target", "season"]
    df = df[keep_cols].copy()

    # Use float32 to reduce memory
    for col in available_features:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

    # Time-based split
    train_mask = df["season"] <= 2024
    test_mask = df["season"] == 2025

    # Sample training data to fit in memory (keep all test data)
    train_df = df[train_mask]
    if len(train_df) > 1_500_000:
        train_df = train_df.sample(n=1_500_000, random_state=42)
        print(f"  Sampled training data to {len(train_df):,} pitches")
    test_df = df[test_mask]
    del df

    X_train = train_df[available_features]
    y_train = train_df["target"]
    X_test = test_df[available_features]
    y_test = test_df["target"]
    del train_df, test_df

    print(f"\n  Train: {len(X_train):,} pitches")
    print(f"  Test:  {len(X_test):,} pitches")
    print(f"  Features: {len(available_features)}")

    # Train model
    print("\nTraining XGBoost pitch selection model...")
    model = xgb.XGBClassifier(
        objective="multi:softprob",
        num_class=len(class_names),
        n_estimators=400,
        max_depth=7,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=100,
        eval_metric="mlogloss",
        early_stopping_rounds=15,
        n_jobs=-1,
        random_state=42,
        tree_method="hist",
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
        "feature": available_features,
        "importance": model.feature_importances_,
    }).sort_values("importance", ascending=False)
    print(f"\nTop 15 features:")
    print(importance.head(15).to_string(index=False))

    # Save
    os.makedirs(MODEL_DIR, exist_ok=True)
    joblib.dump({
        "model": model,
        "label_encoder": le_target,
        "stand_encoder": le_stand,
        "p_throws_encoder": le_p_throws,
        "prev_pitch_encoder": le_prev,
    }, MODEL_PATH)
    print(f"\nModel saved to {MODEL_PATH}")

    meta = {
        "model_type": "xgboost_multiclass",
        "target": "pitch_type",
        "classes": class_names,
        "features": available_features,
        "train_seasons": [2021, 2022, 2023, 2024],
        "test_season": 2025,
        "test_log_loss": float(log_loss(y_test, y_pred_proba)),
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
    }
    with open(META_PATH, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"Metadata saved to {META_PATH}")


if __name__ == "__main__":
    train_pitch_selection_model()
