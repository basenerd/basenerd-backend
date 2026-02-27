"""
score_stuffplus_live.py

Runs on Render Cron every 30 minutes:
- Fetches recent pitches from a live table that are missing Stuff+ fields
- Loads a trained sklearn Pipeline (joblib) + meta JSON
- Scores Stuff+ using YOUR training conventions:
    - model predicts rv_resid_hat
    - goodness = -rv_resid_hat  (higher is better)
    - stuff_plus = center + scale * (goodness / goodness_std)
    - clipped to [clip_min, clip_max]
- Updates rows using composite key: (game_pk, at_bat_number, pitch_number)

Requirements (in requirements.txt):
- pandas
- numpy
- scikit-learn==<match training version>  (you trained with 1.3.2)
- joblib
- sqlalchemy
- pg8000

Env vars (Render):
- DATABASE_URL (provided by Render; usually starts with postgresql://)
Optional:
- STUFF_TABLE (default statcast_pitches_live)
- STUFF_DAYS_BACK (default 4)
- STUFF_BATCH_SIZE (default 3000)
- STUFF_MODEL_PATH (default models/stuff_model.pkl)
- STUFF_META_PATH (default models/stuff_model_meta.json)
"""

import os
import ssl
import json
import math
import joblib
import pandas as pd
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text


# ----------------------------
# Config
# ----------------------------
DATABASE_URL = os.environ.get("DATABASE_URL")

TABLE_NAME = os.environ.get("STUFF_TABLE", "statcast_pitches_live")
DAYS_BACK = int(os.environ.get("STUFF_DAYS_BACK", "4"))
BATCH_SIZE = int(os.environ.get("STUFF_BATCH_SIZE", "3000"))

MODEL_PATH = os.environ.get("STUFF_MODEL_PATH", "models/stuff_model.pkl")
META_PATH = os.environ.get("STUFF_META_PATH", "models/stuff_model_meta.json")

KEY_COLS = ["game_pk", "at_bat_number", "pitch_number"]


# ----------------------------
# Helpers
# ----------------------------
def utc_now() -> datetime:
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
    """
    Render's DATABASE_URL is usually: postgresql://...
    Force SQLAlchemy to use pg8000: postgresql+pg8000://...
    Also use pg8000-compatible SSL via ssl_context.
    """
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL env var not set.")

    db_url = DATABASE_URL.strip()

    # Force pg8000 driver
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+pg8000://", 1)
    elif db_url.startswith("postgres://"):
        # some platforms use postgres://
        db_url = db_url.replace("postgres://", "postgresql+pg8000://", 1)
    elif db_url.startswith("postgresql+pg8000://"):
        pass
    else:
        # If it's already using another driver, force it if it's plain postgresql
        if db_url.startswith("postgresql+psycopg2://"):
            db_url = db_url.replace("postgresql+psycopg2://", "postgresql+pg8000://", 1)

    # pg8000 SSL: use ssl_context (NOT sslmode)
    ssl_context = ssl.create_default_context()

    engine = create_engine(
        db_url,
        connect_args={"ssl_context": ssl_context},
        pool_pre_ping=True,
    )

    # Sanity check driver
    if engine.url.drivername != "postgresql+pg8000":
        raise RuntimeError(
            f"Engine driver is {engine.url.drivername}, expected postgresql+pg8000. "
            f"Got DATABASE_URL='{DATABASE_URL[:60]}...'"
        )

    return engine


def load_meta(meta_path: str) -> dict:
    if not os.path.exists(meta_path):
        raise RuntimeError(f"Meta file not found at {meta_path}")
    with open(meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    # Validate keys we expect from your training script
    required = ["num_features", "cat_features", "goodness_std"]
    missing = [k for k in required if k not in meta]
    if missing:
        raise RuntimeError(f"Meta JSON missing keys: {missing}")

    return meta


def ensure_required_columns(df: pd.DataFrame, cols: list, table_name: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required columns in {table_name}: {missing}")


# ----------------------------
# Main
# ----------------------------
def main():
    # Load model + meta
    if not os.path.exists(MODEL_PATH):
        raise RuntimeError(f"Model file not found at {MODEL_PATH}")

    pipe = joblib.load(MODEL_PATH)
    meta = load_meta(META_PATH)

    num_features = meta["num_features"]
    cat_features = meta["cat_features"]
    feature_cols = list(num_features) + list(cat_features)

    # Stuff+ scaling parameters from meta (your training script)
    goodness_std = float(meta.get("goodness_std", 0.01)) or 0.01
    stuff_center = float(meta.get("stuff_center", 100.0))
    stuff_scale = float(meta.get("stuff_scale", 15.0))
    clip_min = float(meta.get("stuff_clip_min", 40.0))
    clip_max = float(meta.get("stuff_clip_max", 160.0))
    model_version = str(meta.get("model_version", "unknown"))

    engine = build_engine()

    # Only score recent
    cutoff = utc_now() - timedelta(days=DAYS_BACK)

    select_sql = text(f"""
        SELECT *
        FROM {TABLE_NAME}
        WHERE game_date >= :cutoff
          AND (stuff_plus IS NULL OR stuff_raw IS NULL)
        ORDER BY game_date DESC
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
    loops = 0

    with engine.begin() as conn:
        # quick connectivity check
        one = conn.execute(text("SELECT 1")).scalar()
        print("DB connectivity OK, SELECT 1 ->", one)

        while True:
            loops += 1
            df = pd.read_sql(
                select_sql,
                conn,
                params={"cutoff": cutoff.date(), "lim": BATCH_SIZE},
            )

            if df.empty:
                print(f"[{utc_now().isoformat()}] No rows to score (recent {DAYS_BACK} days).")
                break

            ensure_required_columns(df, KEY_COLS, TABLE_NAME)
            ensure_required_columns(df, feature_cols, TABLE_NAME)

            # Predict rv_resid_hat
            X = df[feature_cols].copy()
            rv_resid_hat = pipe.predict(X)

            # Convert to Stuff+ using your convention:
            # goodness = -rv_resid_hat (higher is better)
            goodness = -pd.Series(rv_resid_hat)

            stuff_plus = stuff_center + stuff_scale * (goodness / goodness_std)
            stuff_plus = stuff_plus.apply(lambda v: clip(safe_float(v), clip_min, clip_max))

            out = df[KEY_COLS].copy()
            out["stuff_raw"] = [safe_float(v) for v in rv_resid_hat]  # store raw model output
            out["stuff_plus"] = stuff_plus.values
            out["stuff_model_version"] = model_version
            out["stuff_updated_at"] = utc_now()

            payload = []
            for r in out.itertuples(index=False):
                payload.append({
                    "game_pk": int(r.game_pk),
                    "at_bat_number": int(r.at_bat_number),
                    "pitch_number": int(r.pitch_number),
                    "stuff_raw": r.stuff_raw,
                    "stuff_plus": r.stuff_plus,
                    "stuff_model_version": r.stuff_model_version,
                    "stuff_updated_at": r.stuff_updated_at,
                })

            conn.execute(update_sql, payload)

            batch_n = len(payload)
            total += batch_n
            print(f"[{utc_now().isoformat()}] batch_updated={batch_n} total_updated={total} loops={loops}")

            # If we fetched less than batch size, likely done
            if batch_n < BATCH_SIZE:
                break

    print(f"DONE total_scored={total}")


if __name__ == "__main__":
    main()
