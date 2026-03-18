#!/usr/bin/env python3
"""
Train per-umpire strike zone models.

For each umpire with sufficient data, trains an individual
HistGradientBoostingClassifier to predict P(called strike).
Also trains a league-average fallback model on all umpires' data.

Output:
  - models/umpire_zone_models/_league_avg.joblib   (fallback model)
  - models/umpire_zone_models/{umpire_id}.joblib   (per-umpire models)
  - models/umpire_zone_models/registry.json        (index of all trained models)
"""

import os
import json
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import log_loss, brier_score_loss, roc_auc_score

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
MODEL_DIR = os.path.join(os.path.dirname(__file__), "..", "models", "umpire_zone_models")
TRAIN_PATH = os.path.join(DATA_DIR, "umpire_zone_train.parquet")

# Minimum called pitches to train an individual model
MIN_PITCHES = 2000

# Features used by each per-umpire model
FEATURES = [
    "plate_x",
    "plate_z_norm",
    "dist_from_edge_x",
    "dist_from_edge_z_top",
    "dist_from_edge_z_bot",
    "pitch_type",
    "stand",
    "balls",
    "strikes",
]

CAT_FEATURES = ["pitch_type", "stand"]
TARGET = "is_called_strike"


def _prepare_features(df, cat_categories):
    """Encode categoricals and return feature matrix."""
    df = df.copy()
    for col in CAT_FEATURES:
        df[col] = pd.Categorical(df[col], categories=cat_categories[col])
    return df[FEATURES]


def _train_one(df, cat_categories, label=""):
    """Train a single HistGBT classifier on the given data.

    Returns (model, eval_metrics) or (None, None) if training fails.
    """
    # Temporal split: last 20% by date
    df = df.sort_values("game_date").reset_index(drop=True)
    split_idx = int(len(df) * 0.8)
    train_df = df.iloc[:split_idx]
    test_df = df.iloc[split_idx:]

    if len(train_df) < 200 or len(test_df) < 50:
        return None, None

    X_train = _prepare_features(train_df, cat_categories)
    y_train = train_df[TARGET]
    X_test = _prepare_features(test_df, cat_categories)
    y_test = test_df[TARGET]

    cat_indices = [FEATURES.index(c) for c in CAT_FEATURES]

    model = HistGradientBoostingClassifier(
        max_iter=300,
        max_depth=5,
        min_samples_leaf=30,
        learning_rate=0.05,
        l2_regularization=0.1,
        categorical_features=cat_indices,
        random_state=42,
    )
    model.fit(X_train, y_train)

    # Evaluate
    y_prob = model.predict_proba(X_test)[:, 1]
    metrics = {
        "log_loss": round(float(log_loss(y_test, y_prob)), 4),
        "brier_score": round(float(brier_score_loss(y_test, y_prob)), 4),
        "auc": round(float(roc_auc_score(y_test, y_prob)), 4),
        "train_rows": len(train_df),
        "test_rows": len(test_df),
        "cs_rate": round(float(y_train.mean()), 3),
    }

    if label:
        print(f"  {label}: brier={metrics['brier_score']:.4f}  "
              f"auc={metrics['auc']:.4f}  n={len(df):,}")

    return model, metrics


def train():
    if not os.path.exists(TRAIN_PATH):
        print(f"Training data not found at {TRAIN_PATH}")
        print("Run build_umpire_zone_data.py first.")
        return

    print("Loading training data...")
    df = pd.read_parquet(TRAIN_PATH)
    print(f"  {len(df):,} rows, {df['hp_umpire_id'].nunique()} umpires")

    # Build global category lists (shared across all models for consistency)
    cat_categories = {}
    for col in CAT_FEATURES:
        cat_categories[col] = sorted(df[col].dropna().unique().tolist())

    os.makedirs(MODEL_DIR, exist_ok=True)

    # --- Train league-average fallback model ---
    print("\nTraining league-average model (all umpires)...")
    league_model, league_metrics = _train_one(df, cat_categories, label="LEAGUE AVG")
    if league_model is None:
        print("ERROR: Could not train league-average model.")
        return

    league_path = os.path.join(MODEL_DIR, "_league_avg.joblib")
    joblib.dump(league_model, league_path)
    print(f"  Saved to {league_path}")

    # --- Train per-umpire models ---
    print(f"\nTraining per-umpire models (min {MIN_PITCHES} pitches)...")
    ump_counts = df.groupby("hp_umpire_id").size()
    eligible = ump_counts[ump_counts >= MIN_PITCHES].index.tolist()
    print(f"  {len(eligible)} umpires eligible (of {len(ump_counts)} total)")

    registry = {
        "features": FEATURES,
        "cat_features": CAT_FEATURES,
        "cat_categories": cat_categories,
        "target": TARGET,
        "min_pitches": MIN_PITCHES,
        "league_avg_metrics": league_metrics,
        "umpires": {},
    }

    trained = 0
    skipped = 0
    for ump_id in sorted(eligible):
        ump_df = df[df["hp_umpire_id"] == ump_id]
        model, metrics = _train_one(
            ump_df, cat_categories, label=f"Umpire {ump_id}"
        )

        if model is None:
            skipped += 1
            continue

        model_path = os.path.join(MODEL_DIR, f"{ump_id}.joblib")
        joblib.dump(model, model_path)

        registry["umpires"][str(ump_id)] = {
            "total_pitches": int(len(ump_df)),
            **metrics,
        }
        trained += 1

    print(f"\n  Trained: {trained}  Skipped: {skipped}  "
          f"Fallback (< {MIN_PITCHES} pitches): {len(ump_counts) - len(eligible)}")

    # Save registry
    registry_path = os.path.join(MODEL_DIR, "registry.json")
    with open(registry_path, "w") as f:
        json.dump(registry, f, indent=2)
    print(f"\nRegistry saved to {registry_path}")

    # Summary stats
    if registry["umpires"]:
        briers = [u["brier_score"] for u in registry["umpires"].values()]
        aucs = [u["auc"] for u in registry["umpires"].values()]
        print(f"\nPer-umpire model stats:")
        print(f"  Brier score: median={np.median(briers):.4f}  "
              f"min={min(briers):.4f}  max={max(briers):.4f}")
        print(f"  AUC:         median={np.median(aucs):.4f}  "
              f"min={min(aucs):.4f}  max={max(aucs):.4f}")


if __name__ == "__main__":
    train()
