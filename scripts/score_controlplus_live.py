#!/usr/bin/env python3
"""
Score Control+ for pitches in statcast_pitches_live.

Mirrors score_stuffplus_live.py — reads unscored rows, predicts,
writes control_raw + control_plus back to the table.
"""
import os
import ssl
import json
import math
import sys
import joblib
import pandas as pd
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text

print("Python:", sys.version)

DATABASE_URL = os.environ.get("DATABASE_URL")

TABLE_NAME = os.environ.get("CONTROL_TABLE", "statcast_pitches_live")
DAYS_BACK = int(os.environ.get("CONTROL_DAYS_BACK", "4"))
BATCH_SIZE = int(os.environ.get("CONTROL_BATCH_SIZE", "3000"))

MODEL_PATH = os.environ.get("CONTROL_MODEL_PATH", "models/control_model.pkl")
META_PATH = os.environ.get("CONTROL_META_PATH", "models/control_model_meta.json")

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
    control_center = float(meta.get("control_center", 100.0))
    control_scale = float(meta.get("control_scale", 15.0))
    clip_min = float(meta.get("control_clip_min", 40.0))
    clip_max = float(meta.get("control_clip_max", 160.0))
    model_version = meta.get("model_version", "unknown")

    engine = build_engine()

    cutoff = utc_now() - timedelta(days=DAYS_BACK)

    select_sql = text(f"""
        SELECT *
        FROM {TABLE_NAME}
        WHERE game_date >= :cutoff
          AND (control_plus IS NULL OR control_raw IS NULL)
          AND plate_x IS NOT NULL
          AND plate_z IS NOT NULL
          AND sz_top  IS NOT NULL
          AND sz_bot  IS NOT NULL
          AND pitch_type IS NOT NULL
        LIMIT :lim
    """)

    update_sql = text(f"""
        UPDATE {TABLE_NAME}
           SET control_raw           = :control_raw,
               control_plus          = :control_plus,
               control_model_version = :control_model_version,
               control_updated_at    = :control_updated_at
         WHERE game_pk       = :game_pk
           AND at_bat_number = :at_bat_number
           AND pitch_number  = :pitch_number
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

            X = df[feature_cols]
            preds = pipe.predict(X)

            goodness = -pd.Series(preds)
            control_plus = control_center + control_scale * (goodness / goodness_std)
            control_plus = control_plus.apply(lambda v: clip(safe_float(v), clip_min, clip_max))

            payload = []
            for i, row in df.iterrows():
                payload.append({
                    "game_pk":               int(row["game_pk"]),
                    "at_bat_number":         int(row["at_bat_number"]),
                    "pitch_number":          int(row["pitch_number"]),
                    "control_raw":           safe_float(preds[i]),
                    "control_plus":          safe_float(control_plus.iloc[i]),
                    "control_model_version": model_version,
                    "control_updated_at":    utc_now(),
                })

            conn.execute(update_sql, payload)

            total += len(payload)
            print("Updated:", len(payload))

    print("DONE total_scored=", total)


if __name__ == "__main__":
    main()
