#!/usr/bin/env python3
"""Score Stuff+ on statcast_pitches_live using per-pitch-type models (v5).

Loads the model registry from models/stuff_models/registry.json, routes each
pitch to its pitch-type-specific model, and writes stuff_plus back to the DB.
Falls back to the global model for unknown pitch types.
"""
import os
import ssl
import json
import math
import sys
import joblib
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text

print("Python:", sys.version)

DATABASE_URL = os.environ.get("DATABASE_URL")

TABLE_NAME = os.environ.get("STUFF_TABLE", "statcast_pitches_live")
DAYS_BACK = int(os.environ.get("STUFF_DAYS_BACK", "4"))
BATCH_SIZE = int(os.environ.get("STUFF_BATCH_SIZE", "3000"))

REGISTRY_PATH = os.environ.get(
    "STUFF_REGISTRY_PATH", "models/stuff_models/registry.json"
)

KEY_COLS = ["game_pk", "at_bat_number", "pitch_number"]


def utc_now():
    return datetime.now(timezone.utc)


def safe_float(x):
    try:
        if x is None:
            return None
        if isinstance(x, float) and math.isnan(x):
            return None
        return float(x)
    except Exception:
        return None


def clip(v, lo, hi):
    if v is None:
        return None
    return max(lo, min(hi, v))


_Y_PLATE = 17.0 / 12.0


def compute_approach_angles(df: pd.DataFrame) -> pd.DataFrame:
    """Compute vertical and horizontal approach angles at the plate."""
    y0 = df["release_pos_y"].values.astype(np.float64)
    vy0 = df["vy0"].values.astype(np.float64)
    ay = df["ay"].values.astype(np.float64)
    vx0 = df["vx0"].values.astype(np.float64)
    vz0 = df["vz0"].values.astype(np.float64)
    ax = df["ax"].values.astype(np.float64)
    az = df["az"].values.astype(np.float64)

    a = 0.5 * ay
    b = vy0
    c = y0 - _Y_PLATE

    discriminant = np.maximum(b**2 - 4.0 * a * c, 0.0)
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
        df["vert_approach_angle"] = np.degrees(np.arctan2(vz_plate, abs_vy))
        df["horiz_approach_angle"] = np.degrees(np.arctan2(vx_plate, abs_vy))

    return df


def build_engine():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL env var not set.")

    db_url = DATABASE_URL
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+pg8000://", 1)
    elif db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+pg8000://", 1)

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    engine = create_engine(
        db_url,
        connect_args={"ssl_context": ssl_context},
        pool_pre_ping=True,
    )
    print("DB driver:", engine.url.drivername)
    return engine


def load_models(registry_path):
    """Load the model registry and all per-type models + global fallback."""
    with open(registry_path, "r", encoding="utf-8") as f:
        registry = json.load(f)

    models_dir = os.path.dirname(registry_path)
    pipes = {}

    # Load per-type models
    for pt, info in registry.get("pitch_type_models", {}).items():
        model_path = os.path.join(models_dir, info["model_file"])
        meta_path = os.path.join(models_dir, info["meta_file"])
        pipes[pt] = {
            "pipe": joblib.load(model_path),
            "goodness_std": float(info["goodness_std"]),
        }
        with open(meta_path, "r", encoding="utf-8") as f:
            pipes[pt]["meta"] = json.load(f)
        print(f"  Loaded model for {pt}")

    # Load global fallback
    fallback_key = registry.get("fallback_model", "_global")
    fallback_model_path = os.path.join(models_dir, f"{fallback_key}.pkl")
    fallback_meta_path = os.path.join(models_dir, f"{fallback_key}_meta.json")
    with open(fallback_meta_path, "r", encoding="utf-8") as f:
        fallback_meta = json.load(f)
    pipes["_global"] = {
        "pipe": joblib.load(fallback_model_path),
        "goodness_std": float(registry.get("global_goodness_std", 0.01)),
        "meta": fallback_meta,
    }
    print("  Loaded global fallback model")

    return registry, pipes


def score_batch(df, registry, pipes):
    """Score a batch of pitches, routing each to its pitch-type model."""
    center = float(registry.get("stuff_center", 100.0))
    scale = float(registry.get("stuff_scale", 15.0))
    clip_min = float(registry.get("stuff_clip_min", 40.0))
    clip_max = float(registry.get("stuff_clip_max", 160.0))
    model_version = registry.get("model_version", "unknown")

    # Compute derived features
    derived = registry.get("derived_features", [])
    if derived:
        df = compute_approach_angles(df)

    results = pd.Series(np.nan, index=df.index, name="stuff_plus")
    raw_preds = pd.Series(np.nan, index=df.index, name="stuff_raw")

    # Group by pitch type and score each group
    for pt, group in df.groupby("pitch_type"):
        if pt in pipes:
            model_info = pipes[pt]
        else:
            model_info = pipes["_global"]

        meta = model_info["meta"]
        feature_cols = meta["num_features"] + meta["cat_features"]
        goodness_std = model_info["goodness_std"] or 0.01

        X = group[feature_cols]
        preds = model_info["pipe"].predict(X)

        goodness = -pd.Series(preds, index=group.index)
        sp = center + scale * (goodness / goodness_std)
        sp = sp.clip(clip_min, clip_max)

        results.loc[group.index] = sp
        raw_preds.loc[group.index] = preds

    return raw_preds, results, model_version


def main():
    if not os.path.exists(REGISTRY_PATH):
        raise RuntimeError(f"Registry not found at {REGISTRY_PATH}")

    print("Loading models...")
    registry, pipes = load_models(REGISTRY_PATH)

    engine = build_engine()

    cutoff = utc_now() - timedelta(days=DAYS_BACK)

    select_sql = text(f"""
        SELECT *
        FROM {TABLE_NAME}
        WHERE game_date >= :cutoff
          AND (stuff_plus IS NULL OR stuff_raw IS NULL)
        LIMIT :lim
    """)

    update_sql = text(f"""
        UPDATE {TABLE_NAME}
           SET stuff_raw = :stuff_raw,
               stuff_plus = :stuff_plus,
               stuff_model_version = :stuff_model_version,
               stuff_updated_at = :stuff_updated_at
         WHERE game_pk = :game_pk
           AND at_bat_number = :at_bat_number
           AND pitch_number = :pitch_number
    """)

    total = 0

    with engine.begin() as conn:
        print("DB check:", conn.execute(text("SELECT 1")).scalar())

        while True:
            df = pd.read_sql(
                select_sql,
                conn,
                params={"cutoff": cutoff.date(), "lim": BATCH_SIZE},
            )

            if df.empty:
                print("No rows to score.")
                break

            raw_preds, stuff_plus, model_version = score_batch(
                df, registry, pipes
            )

            payload = []
            for i, row in df.iterrows():
                payload.append({
                    "game_pk": int(row["game_pk"]),
                    "at_bat_number": int(row["at_bat_number"]),
                    "pitch_number": int(row["pitch_number"]),
                    "stuff_raw": safe_float(raw_preds.loc[i]),
                    "stuff_plus": safe_float(stuff_plus.loc[i]),
                    "stuff_model_version": model_version,
                    "stuff_updated_at": utc_now(),
                })

            conn.execute(update_sql, payload)

            total += len(payload)
            print("Updated:", len(payload))

    print("DONE total_scored=", total)


if __name__ == "__main__":
    main()
