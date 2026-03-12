#!/usr/bin/env python3
"""
Train Stuff+ models (v5) — one model per pitch type.

Each pitch type gets its own HistGradientBoostingRegressor trained on rv_resid,
with its own goodness_std for normalization (mean=100, std=15 within type).
A global fallback model handles rare/unknown pitch types.

Output:
  models/stuff_models/registry.json   — maps pitch_type → model/meta files
  models/stuff_models/<PT>.pkl        — per-type pipeline
  models/stuff_models/<PT>_meta.json  — per-type metadata
  models/stuff_models/_global.pkl     — fallback model
  models/stuff_models/_global_meta.json
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

RAW_NUM_FEATURES = [
    "release_speed", "release_spin_rate", "release_extension",
    "release_pos_x", "release_pos_z", "release_pos_y",
    "pfx_x", "pfx_z",
    "vx0", "vy0", "vz0",
    "ax", "ay", "az",
    "sz_top", "sz_bot",
]

DERIVED_FEATURES = ["vert_approach_angle", "horiz_approach_angle"]

NUM_FEATURES = RAW_NUM_FEATURES + DERIVED_FEATURES
CAT_FEATURES = ["p_throws"]
FEATURE_COLS = NUM_FEATURES + CAT_FEATURES

# Global model also uses pitch_type as a feature
GLOBAL_CAT_FEATURES = ["pitch_type", "p_throws"]
GLOBAL_FEATURE_COLS = NUM_FEATURES + GLOBAL_CAT_FEATURES

MODEL_VERSION = "stuff_v5_per_type_hgbm"
TRAIN_YEARS = (2021, 2025)
RANDOM_STATE = 42

# Minimum pitches to train a dedicated model for a pitch type
MIN_PITCHES_FOR_TYPE = 5000

STUFF_CENTER = 100.0
STUFF_SCALE = 15.0
STUFF_CLIP_MIN = 40.0
STUFF_CLIP_MAX = 160.0

Y_PLATE = 17.0 / 12.0


def get_conn(url: str):
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url)


def compute_approach_angles(df: pd.DataFrame) -> pd.DataFrame:
    """Compute vertical and horizontal approach angles at the plate."""
    y0 = df["release_pos_y"].values
    vy0 = df["vy0"].values
    ay = df["ay"].values
    vx0 = df["vx0"].values
    vz0 = df["vz0"].values
    ax = df["ax"].values
    az = df["az"].values

    a = 0.5 * ay
    b = vy0
    c = y0 - Y_PLATE

    discriminant = b**2 - 4.0 * a * c
    discriminant = np.maximum(discriminant, 0.0)

    sqrt_disc = np.sqrt(discriminant)
    with np.errstate(divide="ignore", invalid="ignore"):
        t1 = (-b + sqrt_disc) / (2.0 * a)
        t2 = (-b - sqrt_disc) / (2.0 * a)

    t = np.where((t2 > 0) & (t2 < t1), t2, t1)
    t = np.where(t > 0, t, np.nan)

    vy_plate = vy0 + ay * t
    vz_plate = vz0 + az * t
    vx_plate = vx0 + ax * t

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
            p_throws,
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


def build_pipeline(cat_features):
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", "passthrough", NUM_FEATURES),
            ("cat", OrdinalEncoder(
                handle_unknown="use_encoded_value", unknown_value=-1
            ), cat_features),
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


def train_single_model(df_type, pitch_type_label, cat_features, feature_cols, out_dir):
    """Train a single model on the given dataframe subset. Returns (meta_dict, val_rmse, val_r2)."""
    X = df_type[feature_cols]
    y = df_type["rv_resid"]

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.1, random_state=RANDOM_STATE
    )
    print(f"    Train: {len(X_train):,}  Val: {len(X_val):,}")

    pipe = build_pipeline(cat_features)
    pipe.fit(X_train, y_train)

    val_preds = pipe.predict(X_val)
    val_rmse = math.sqrt(mean_squared_error(y_val, val_preds))
    val_r2 = r2_score(y_val, val_preds)
    print(f"    Val RMSE: {val_rmse:.5f}   R²: {val_r2:.5f}")

    # Per-pitch goodness_std
    all_preds = pipe.predict(X)
    goodness_all = -pd.Series(all_preds, index=X.index)
    goodness_std = float(goodness_all.std())
    print(f"    Per-pitch goodness_std: {goodness_std:.6f}")

    # Sanity check
    sp_all = STUFF_CENTER + STUFF_SCALE * (goodness_all / goodness_std)
    sp_clipped = sp_all.clip(STUFF_CLIP_MIN, STUFF_CLIP_MAX)
    pct_clipped = ((sp_all < STUFF_CLIP_MIN) | (sp_all > STUFF_CLIP_MAX)).mean() * 100
    print(f"    Stuff+ — mean: {sp_clipped.mean():.1f}  std: {sp_clipped.std():.1f}  "
          f"clipped: {pct_clipped:.1f}%")

    # Feature importances
    hgbm = pipe.named_steps["model"]
    feat_names = NUM_FEATURES + cat_features
    if hasattr(hgbm, "feature_importances_"):
        importances = hgbm.feature_importances_
        print("    Feature importances (top 5):")
        for name, imp in sorted(zip(feat_names, importances), key=lambda x: -x[1])[:5]:
            print(f"      {name:>25s}: {imp:.4f}")

    # Save model
    model_file = os.path.join(out_dir, f"{pitch_type_label}.pkl")
    meta_file = os.path.join(out_dir, f"{pitch_type_label}_meta.json")
    joblib.dump(pipe, model_file)

    meta = {
        "model_version": MODEL_VERSION,
        "model_type": "HistGradientBoostingRegressor",
        "pitch_type": pitch_type_label,
        "train_year_range": list(TRAIN_YEARS),
        "n_pitches": len(df_type),
        "num_features": NUM_FEATURES,
        "raw_num_features": RAW_NUM_FEATURES,
        "derived_features": DERIVED_FEATURES,
        "cat_features": cat_features,
        "goodness_std": goodness_std,
        "stuff_center": STUFF_CENTER,
        "stuff_scale": STUFF_SCALE,
        "stuff_clip_min": STUFF_CLIP_MIN,
        "stuff_clip_max": STUFF_CLIP_MAX,
        "val_rmse": val_rmse,
        "val_r2": val_r2,
    }

    with open(meta_file, "w") as f:
        json.dump(meta, f, indent=2)

    print(f"    Saved -> {model_file}")
    return meta


def main():
    parser = argparse.ArgumentParser(description="Train Stuff+ v5 per-pitch-type models")
    parser.add_argument("--db-url", default=DATABASE_URL,
                        help="PostgreSQL connection URL")
    parser.add_argument("--out-dir", default="models/stuff_models")
    args = parser.parse_args()

    if not args.db_url:
        print("ERROR: provide --db-url or set DATABASE_URL env var")
        sys.exit(1)

    conn = get_conn(args.db_url)
    df = fetch_training_data(conn)
    conn.close()

    # Compute derived features
    print("Computing approach angles...")
    df = compute_approach_angles(df)
    valid_mask = df["vert_approach_angle"].notna() & df["horiz_approach_angle"].notna()
    dropped = (~valid_mask).sum()
    if dropped > 0:
        print(f"  Dropped {dropped:,} rows with invalid approach angles")
        df = df[valid_mask].reset_index(drop=True)

    # Compute context baselines (shared across all pitch types)
    df, baseline_map = compute_rv_resid(df)

    # Pitch type counts
    type_counts = df["pitch_type"].value_counts()
    print(f"\nPitch type counts:")
    for pt, n in type_counts.items():
        marker = " *" if n >= MIN_PITCHES_FOR_TYPE else "  (-> global fallback)"
        print(f"  {pt:>4s}: {n:>10,}{marker}")

    dedicated_types = sorted(type_counts[type_counts >= MIN_PITCHES_FOR_TYPE].index.tolist())
    fallback_types = sorted(type_counts[type_counts < MIN_PITCHES_FOR_TYPE].index.tolist())
    print(f"\nDedicated models: {dedicated_types}")
    print(f"Fallback types:   {fallback_types}")

    out_dir = args.out_dir
    os.makedirs(out_dir, exist_ok=True)

    registry = {
        "model_version": MODEL_VERSION,
        "model_type": "per_pitch_type",
        "train_year_range": list(TRAIN_YEARS),
        "target_raw": "delta_run_exp",
        "target_model": "rv_resid",
        "num_features": NUM_FEATURES,
        "raw_num_features": RAW_NUM_FEATURES,
        "derived_features": DERIVED_FEATURES,
        "context_keys": ["balls", "strikes", "outs_when_up", "base_state"],
        "context_baseline_map": baseline_map,
        "stuff_center": STUFF_CENTER,
        "stuff_scale": STUFF_SCALE,
        "stuff_clip_min": STUFF_CLIP_MIN,
        "stuff_clip_max": STUFF_CLIP_MAX,
        "pitch_type_models": {},
        "fallback_model": "_global",
        "fallback_types": fallback_types,
    }

    # Train per-pitch-type models
    for pt in dedicated_types:
        print(f"\n{'='*60}")
        print(f"Training model for {pt} ({type_counts[pt]:,} pitches)")
        print(f"{'='*60}")
        df_pt = df[df["pitch_type"] == pt].copy()
        meta = train_single_model(df_pt, pt, CAT_FEATURES, FEATURE_COLS, out_dir)
        registry["pitch_type_models"][pt] = {
            "model_file": f"{pt}.pkl",
            "meta_file": f"{pt}_meta.json",
            "n_pitches": int(type_counts[pt]),
            "goodness_std": meta["goodness_std"],
            "val_rmse": meta["val_rmse"],
            "val_r2": meta["val_r2"],
        }

    # Train global fallback model (all pitch types, uses pitch_type as feature)
    print(f"\n{'='*60}")
    print(f"Training GLOBAL fallback model ({len(df):,} pitches)")
    print(f"{'='*60}")
    global_meta = train_single_model(
        df, "_global", GLOBAL_CAT_FEATURES, GLOBAL_FEATURE_COLS, out_dir
    )
    registry["global_goodness_std"] = global_meta["goodness_std"]
    registry["global_val_rmse"] = global_meta["val_rmse"]
    registry["global_val_r2"] = global_meta["val_r2"]

    # Save registry
    registry_path = os.path.join(out_dir, "registry.json")
    with open(registry_path, "w") as f:
        json.dump(registry, f, indent=2)
    print(f"\nSaved registry -> {registry_path}")

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Models trained: {len(dedicated_types)} dedicated + 1 global fallback")
    for pt in dedicated_types:
        info = registry["pitch_type_models"][pt]
        print(f"  {pt:>4s}: n={info['n_pitches']:>10,}  "
              f"RMSE={info['val_rmse']:.5f}  R²={info['val_r2']:.5f}  "
              f"goodness_std={info['goodness_std']:.6f}")
    print(f"  GLOB: n={len(df):>10,}  "
          f"RMSE={registry['global_val_rmse']:.5f}  R²={registry['global_val_r2']:.5f}")
    print("Done.")


if __name__ == "__main__":
    main()
