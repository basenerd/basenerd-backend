#!/usr/bin/env python3
"""
Train Control+ model.

Control+ measures pitch location quality, independent of physical stuff.
It uses plate location relative to the strike zone to predict run value,
scaled to mean=100, std=15 (higher = better command).

Features: plate_x, plate_z, sz_top, sz_bot, pitch_type
Target:   rv_resid = delta_run_exp - count/base_state context baseline
"""
import os
import sys
import json
import math
import joblib
import argparse
import numpy as np
import pandas as pd
import psycopg2

from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import OrdinalEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline as SKPipeline

DATABASE_URL = os.environ.get("DATABASE_URL", "")

NUM_FEATURES = ["plate_x", "plate_z", "sz_top", "sz_bot"]
CAT_FEATURES = ["pitch_type"]
FEATURE_COLS = NUM_FEATURES + CAT_FEATURES

MODEL_VERSION = "control_v2_location_hgbm"
TRAIN_YEARS = (2021, 2024)
SAMPLE_FRAC = 1.0       # use all data; set <1.0 to subsample for faster iteration
RANDOM_STATE = 42

CONTROL_CENTER = 100.0
CONTROL_SCALE = 15.0
CONTROL_CLIP_MIN = 40.0
CONTROL_CLIP_MAX = 160.0


def get_conn(url: str):
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url)


def fetch_training_data(conn) -> pd.DataFrame:
    print("Fetching training data...")
    sql = """
        SELECT
            pitcher,
            game_year,
            plate_x,
            plate_z,
            sz_top,
            sz_bot,
            pitch_type,
            balls,
            strikes,
            outs_when_up,
            COALESCE(on_1b, 0) +
            COALESCE(on_2b, 0) * 2 +
            COALESCE(on_3b, 0) * 4  AS base_state,
            delta_run_exp
        FROM statcast_pitches
        WHERE game_type = 'R'
          AND game_year BETWEEN %s AND %s
          AND plate_x IS NOT NULL
          AND plate_z IS NOT NULL
          AND sz_top  IS NOT NULL
          AND sz_bot  IS NOT NULL
          AND delta_run_exp IS NOT NULL
          AND pitch_type IS NOT NULL
    """
    df = pd.read_sql(sql, conn, params=TRAIN_YEARS)
    print(f"  Loaded {len(df):,} pitches")
    return df


def compute_rv_resid(df: pd.DataFrame) -> pd.DataFrame:
    """Subtract count/base-state mean from delta_run_exp (same approach as Stuff+)."""
    print("Computing context baselines...")
    group_cols = ["balls", "strikes", "outs_when_up", "base_state"]
    baselines = (
        df.groupby(group_cols)["delta_run_exp"]
        .mean()
        .reset_index()
        .rename(columns={"delta_run_exp": "baseline"})
    )
    df = df.merge(baselines, on=group_cols, how="left")
    df["rv_resid"] = df["delta_run_exp"] - df["baseline"].fillna(0)

    # Store baseline map for meta
    baseline_map = {}
    for _, row in baselines.iterrows():
        key = f"{int(row.balls)}|{int(row.strikes)}|{int(row.outs_when_up)}|{int(row.base_state)}"
        baseline_map[key] = float(row.baseline)

    return df, baseline_map


def build_pipeline():
    from sklearn.compose import ColumnTransformer
    from sklearn.preprocessing import OrdinalEncoder

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", "passthrough", NUM_FEATURES),
            ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CAT_FEATURES),
        ]
    )

    model = HistGradientBoostingRegressor(
        max_iter=300,
        max_depth=6,
        learning_rate=0.05,
        min_samples_leaf=100,
        random_state=RANDOM_STATE,
        verbose=1,
    )

    pipe = SKPipeline([
        ("pre", preprocessor),
        ("model", model),
    ])
    return pipe


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-url", default=DATABASE_URL, help="PostgreSQL connection URL")
    parser.add_argument("--out-model", default="models/control_model.pkl")
    parser.add_argument("--out-meta", default="models/control_model_meta.json")
    args = parser.parse_args()

    if not args.db_url:
        print("ERROR: provide --db-url or set DATABASE_URL env var")
        sys.exit(1)

    conn = get_conn(args.db_url)

    df = fetch_training_data(conn)
    conn.close()

    if SAMPLE_FRAC < 1.0:
        df = df.sample(frac=SAMPLE_FRAC, random_state=RANDOM_STATE)
        print(f"  Sampled down to {len(df):,} pitches")

    df, baseline_map = compute_rv_resid(df)

    X = df[FEATURE_COLS]
    y = df["rv_resid"]

    print("Train/val split...")
    X_train, X_val, y_train, y_val, meta_train, meta_val = train_test_split(
        X, y, df[["pitcher", "game_year"]], test_size=0.1, random_state=RANDOM_STATE
    )
    print(f"  Train: {len(X_train):,}  Val: {len(X_val):,}")

    print("Training model...")
    pipe = build_pipeline()
    pipe.fit(X_train, y_train)

    val_preds = pipe.predict(X_val)
    val_rmse = math.sqrt(mean_squared_error(y_val, val_preds))
    val_r2 = r2_score(y_val, val_preds)
    print(f"  Val RMSE: {val_rmse:.5f}   R²: {val_r2:.5f}")

    # Compute goodness_std at the PITCHER-SEASON level, not per-pitch.
    # Per-pitch std compresses to near-zero when averaged over 2000+ pitches.
    # Instead we want: 1 std above average pitcher -> Control+ = 115.
    all_preds = pipe.predict(X)
    goodness_all = -pd.Series(all_preds, index=X.index)
    pitcher_season_key = df["pitcher"].astype(str) + "_" + df["game_year"].astype(str)
    # Only count pitcher-seasons with enough pitches to be stable
    pitcher_season_counts = pitcher_season_key.value_counts()
    qualified = pitcher_season_counts[pitcher_season_counts >= 200].index
    pitcher_season_means = (
        goodness_all.groupby(pitcher_season_key)
        .mean()
        .loc[lambda s: s.index.isin(qualified)]
    )
    goodness_std = float(pitcher_season_means.std())
    print(f"  Pitcher-season goodness_std: {goodness_std:.6f}  (n={len(pitcher_season_means)} pitcher-seasons)")

    # Sanity check: what does the pitcher-season spread look like?
    ctrl_pitcher = CONTROL_CENTER + CONTROL_SCALE * (pitcher_season_means / goodness_std)
    ctrl_pitcher = ctrl_pitcher.clip(CONTROL_CLIP_MIN, CONTROL_CLIP_MAX)
    print(f"  Pitcher-season Control+ — mean: {ctrl_pitcher.mean():.1f}  std: {ctrl_pitcher.std():.1f}  min: {ctrl_pitcher.min():.1f}  max: {ctrl_pitcher.max():.1f}")

    os.makedirs(os.path.dirname(args.out_model), exist_ok=True)
    joblib.dump(pipe, args.out_model)
    print(f"Saved model -> {args.out_model}")

    meta = {
        "model_version": MODEL_VERSION,
        "train_year_range": list(TRAIN_YEARS),
        "target_raw": "delta_run_exp",
        "target_model": "rv_resid",
        "num_features": NUM_FEATURES,
        "cat_features": CAT_FEATURES,
        "context_keys": ["balls", "strikes", "outs_when_up", "base_state"],
        "context_baseline_map": baseline_map,
        "goodness_std": goodness_std,
        "control_center": CONTROL_CENTER,
        "control_scale": CONTROL_SCALE,
        "control_clip_min": CONTROL_CLIP_MIN,
        "control_clip_max": CONTROL_CLIP_MAX,
        "val_rmse": val_rmse,
        "val_r2": val_r2,
    }
    with open(args.out_meta, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"Saved meta  -> {args.out_meta}")
    print("Done.")


if __name__ == "__main__":
    main()
