#!/usr/bin/env python3
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

MODEL_PATH = os.environ.get("STUFF_MODEL_PATH", "models/stuff_model.pkl")
META_PATH = os.environ.get("STUFF_META_PATH", "models/stuff_model_meta.json")

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

    # ✅ SSL but do NOT verify cert (needed for Render/self-signed chains)
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

def main():
    if not os.path.exists(MODEL_PATH):
        raise RuntimeError(f"Model file not found at {MODEL_PATH}")

    pipe = joblib.load(MODEL_PATH)

    with open(META_PATH, "r", encoding="utf-8") as f:
        meta = json.load(f)

    num_features = meta["num_features"]
    cat_features = meta["cat_features"]
    feature_cols = num_features + cat_features

    goodness_std = float(meta.get("goodness_std", 0.01)) or 0.01
    stuff_center = float(meta.get("stuff_center", 100.0))
    stuff_scale = float(meta.get("stuff_scale", 15.0))
    clip_min = float(meta.get("stuff_clip_min", 40.0))
    clip_max = float(meta.get("stuff_clip_max", 160.0))
    model_version = meta.get("model_version", "unknown")

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

            # Compute derived features if needed
            derived = meta.get("derived_features", [])
            if derived:
                df = compute_approach_angles(df)

            X = df[feature_cols]
            preds = pipe.predict(X)

            goodness = -pd.Series(preds)
            stuff_plus = stuff_center + stuff_scale * (goodness / goodness_std)
            stuff_plus = stuff_plus.apply(lambda v: clip(safe_float(v), clip_min, clip_max))

            payload = []

            for i, row in df.iterrows():
                payload.append({
                    "game_pk": int(row["game_pk"]),
                    "at_bat_number": int(row["at_bat_number"]),
                    "pitch_number": int(row["pitch_number"]),
                    "stuff_raw": safe_float(preds[i]),
                    "stuff_plus": safe_float(stuff_plus.iloc[i]),
                    "stuff_model_version": model_version,
                    "stuff_updated_at": utc_now(),
                })

            conn.execute(update_sql, payload)

            total += len(payload)
            print("Updated:", len(payload))

    print("DONE total_scored=", total)


if __name__ == "__main__":
    main()
