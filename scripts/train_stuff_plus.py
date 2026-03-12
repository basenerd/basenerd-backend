#!/usr/bin/env python3
"""
Train Stuff+ model (v3).

Stuff+ measures physical pitch quality using release characteristics, movement,
spin, trajectory, and approach angles to predict run value, scaled to mean=100,
std=15 (higher = better stuff).

Features: release_speed, release_spin_rate, release_extension, release_pos_x,
          release_pos_z, release_pos_y, pfx_x, pfx_z, vx0, vy0, vz0, ax, ay, az,
          sz_top, sz_bot, vert_approach_angle, horiz_approach_angle, pitch_type

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
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OrdinalEncoder
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Original numeric features (from statcast_pitches)
RAW_NUM_FEATURES = [
    "release_speed", "release_spin_rate", "release_extension",
    "release_pos_x", "release_pos_z", "release_pos_y",
    "pfx_x", "pfx_z",
    "vx0", "vy0", "vz0",
    "ax", "ay", "az",
    "sz_top", "sz_bot",
]

# Derived features (computed from raw trajectory data)
DERIVED_FEATURES = ["vert_approach_angle", "horiz_approach_angle"]

NUM_FEATURES = RAW_NUM_FEATURES + DERIVED_FEATURES
CAT_FEATURES = ["pitch_type"]
FEATURE_COLS = NUM_FEATURES + CAT_FEATURES

MODEL_VERSION = "stuff_v3_angles_hgbm"
TRAIN_YEARS = (2021, 2025)
SAMPLE_FRAC = 1.0
RANDOM_STATE = 42

STUFF_CENTER = 100.0
STUFF_SCALE = 15.0
STUFF_CLIP_MIN = 40.0
STUFF_CLIP_MAX = 160.0

Y_PLATE = 17.0 / 12.0  # front of home plate in feet


def get_conn(url: str):
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url)


def compute_approach_angles(df: pd.DataFrame) -> pd.DataFrame:
    """Compute vertical and horizontal approach angles at the plate.

    Uses the pitch trajectory equations:
        y(t) = y0 + vy0*t + 0.5*ay*t^2
    Solve for time t when y = Y_PLATE, then compute plate velocities:
        vz_plate = vz0 + az*t
        vy_plate = vy0 + ay*t
        vx_plate = vx0 + ax*t
    VAA = arctan(vz_plate / |vy_plate|) in degrees
    HAA = arctan(vx_plate / |vy_plate|) in degrees
    """
    y0 = df["release_pos_y"].values
    vy0 = df["vy0"].values
    ay = df["ay"].values
    vx0 = df["vx0"].values
    vz0 = df["vz0"].values
    ax = df["ax"].values
    az = df["az"].values

    # Solve quadratic: 0.5*ay*t^2 + vy0*t + (y0 - Y_PLATE) = 0
    a = 0.5 * ay
    b = vy0
    c = y0 - Y_PLATE

    discriminant = b**2 - 4.0 * a * c
    # Clamp negative discriminants to 0 (shouldn't happen with valid data)
    discriminant = np.maximum(discriminant, 0.0)

    # Two roots; we want the positive one (time forward)
    # ay is typically negative (deceleration), so a < 0
    # vy0 is negative (toward plate), so -b is positive
    # We want the smaller positive root
    sqrt_disc = np.sqrt(discriminant)
    with np.errstate(divide="ignore", invalid="ignore"):
        t1 = (-b + sqrt_disc) / (2.0 * a)
        t2 = (-b - sqrt_disc) / (2.0 * a)

    # Pick the positive root that makes physical sense (~0.4s for a pitch)
    t = np.where((t2 > 0) & (t2 < t1), t2, t1)
    t = np.where(t > 0, t, np.nan)

    vy_plate = vy0 + ay * t
    vz_plate = vz0 + az * t
    vx_plate = vx0 + ax * t

    # VAA: negative means ball is dropping as it crosses plate (typical)
    # Use vy_plate directly (negative = toward catcher) so arctan gives correct sign
    abs_vy = np.abs(vy_plate)
    with np.errstate(divide="ignore", invalid="ignore"):
        vaa = np.degrees(np.arctan2(vz_plate, abs_vy))
        haa = np.degrees(np.arctan2(vx_plate, abs_vy))

    df = df.copy()
    df["vert_approach_angle"] = vaa
    df["horiz_approach_angle"] = haa
    return df


def fetch_training_data(conn) -> pd.DataFrame:
    print("Fetching training data...")
    sql = """
        SELECT
            pitcher,
            game_year,
            release_speed,
            release_spin_rate,
            release_extension,
            release_pos_x,
            release_pos_z,
            release_pos_y,
            pfx_x,
            pfx_z,
            vx0, vy0, vz0,
            ax, ay, az,
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
          AND release_speed IS NOT NULL
          AND pfx_x IS NOT NULL
          AND pfx_z IS NOT NULL
          AND delta_run_exp IS NOT NULL
          AND pitch_type IS NOT NULL
          AND vy0 IS NOT NULL
          AND ay IS NOT NULL
    """
    df = pd.read_sql(sql, conn, params=TRAIN_YEARS)
    print(f"  Loaded {len(df):,} pitches")
    return df


def compute_rv_resid(df: pd.DataFrame) -> tuple:
    """Subtract count/base-state mean from delta_run_exp."""
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

    baseline_map = {}
    for _, row in baselines.iterrows():
        key = f"{int(row.balls)}|{int(row.strikes)}|{int(row.outs_when_up)}|{int(row.base_state)}"
        baseline_map[key] = float(row.baseline)

    return df, baseline_map


def build_pipeline():
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", "passthrough", NUM_FEATURES),
            ("cat", OrdinalEncoder(
                handle_unknown="use_encoded_value", unknown_value=-1
            ), CAT_FEATURES),
        ]
    )

    model = HistGradientBoostingRegressor(
        max_iter=300,
        max_depth=7,
        learning_rate=0.05,
        min_samples_leaf=100,
        early_stopping=True,
        n_iter_no_change=15,
        tol=1e-6,
        validation_fraction=0.1,
        random_state=RANDOM_STATE,
        verbose=0,
    )

    pipe = Pipeline([
        ("pre", preprocessor),
        ("model", model),
    ])
    return pipe


def main():
    parser = argparse.ArgumentParser(description="Train Stuff+ v3 model")
    parser.add_argument("--db-url", default=DATABASE_URL,
                        help="PostgreSQL connection URL")
    parser.add_argument("--out-model", default="models/stuff_model.pkl")
    parser.add_argument("--out-meta", default="models/stuff_model_meta.json")
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

    # Compute derived features
    print("Computing approach angles...")
    df = compute_approach_angles(df)
    valid_mask = df["vert_approach_angle"].notna() & df["horiz_approach_angle"].notna()
    dropped = (~valid_mask).sum()
    if dropped > 0:
        print(f"  Dropped {dropped:,} rows with invalid approach angles")
        df = df[valid_mask].reset_index(drop=True)

    print(f"  VAA range: [{df['vert_approach_angle'].min():.1f}, {df['vert_approach_angle'].max():.1f}] degrees")
    print(f"  HAA range: [{df['horiz_approach_angle'].min():.1f}, {df['horiz_approach_angle'].max():.1f}] degrees")

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

    # Feature importances (from the HistGradientBoosting model)
    hgbm = pipe.named_steps["model"]
    feat_names = NUM_FEATURES + CAT_FEATURES
    if hasattr(hgbm, "feature_importances_"):
        importances = hgbm.feature_importances_
        print("\n  Feature importances:")
        for name, imp in sorted(zip(feat_names, importances), key=lambda x: -x[1]):
            print(f"    {name:>25s}: {imp:.4f}")
    else:
        print("\n  (feature_importances_ not available in this sklearn version)")

    # Compute goodness_std at the PITCHER-SEASON level
    all_preds = pipe.predict(X)
    goodness_all = -pd.Series(all_preds, index=X.index)
    pitcher_season_key = df["pitcher"].astype(str) + "_" + df["game_year"].astype(str)
    pitcher_season_counts = pitcher_season_key.value_counts()
    qualified = pitcher_season_counts[pitcher_season_counts >= 200].index
    pitcher_season_means = (
        goodness_all.groupby(pitcher_season_key)
        .mean()
        .loc[lambda s: s.index.isin(qualified)]
    )
    goodness_std = float(pitcher_season_means.std())
    print(f"\n  Pitcher-season goodness_std: {goodness_std:.6f}  (n={len(pitcher_season_means)} pitcher-seasons)")

    stuff_pitcher = STUFF_CENTER + STUFF_SCALE * (pitcher_season_means / goodness_std)
    stuff_pitcher = stuff_pitcher.clip(STUFF_CLIP_MIN, STUFF_CLIP_MAX)
    print(f"  Pitcher-season Stuff+ — mean: {stuff_pitcher.mean():.1f}  std: {stuff_pitcher.std():.1f}  "
          f"min: {stuff_pitcher.min():.1f}  max: {stuff_pitcher.max():.1f}")

    os.makedirs(os.path.dirname(args.out_model), exist_ok=True)
    joblib.dump(pipe, args.out_model)
    print(f"\nSaved model -> {args.out_model}")

    meta = {
        "model_version": MODEL_VERSION,
        "model_type": "HistGradientBoostingRegressor",
        "train_year_range": list(TRAIN_YEARS),
        "target_raw": "delta_run_exp",
        "target_model": "rv_resid",
        "num_features": NUM_FEATURES,
        "raw_num_features": RAW_NUM_FEATURES,
        "derived_features": DERIVED_FEATURES,
        "cat_features": CAT_FEATURES,
        "context_keys": ["balls", "strikes", "outs_when_up", "base_state"],
        "context_baseline_map": baseline_map,
        "goodness_std": goodness_std,
        "stuff_center": STUFF_CENTER,
        "stuff_scale": STUFF_SCALE,
        "stuff_clip_min": STUFF_CLIP_MIN,
        "stuff_clip_max": STUFF_CLIP_MAX,
        "val_rmse": val_rmse,
        "val_r2": val_r2,
    }
    with open(args.out_meta, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"Saved meta  -> {args.out_meta}")
    print("Done.")


if __name__ == "__main__":
    main()
