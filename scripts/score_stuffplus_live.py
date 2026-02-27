import os
import json
import math
import joblib
import pandas as pd
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text

DATABASE_URL = 'postgresql://basenerd_user:d5LmELIOiEszYPBSLSDT1oIi79gkgDV6@dpg-d5i0tku3jp1c73f1d3gg-a/basenerd'
TABLE_NAME   = os.environ.get("STUFF_TABLE", "statcast_pitches_live")
DAYS_BACK    = int(os.environ.get("STUFF_DAYS_BACK", "4"))
BATCH_SIZE   = int(os.environ.get("STUFF_BATCH_SIZE", "3000"))

# Where the artifacts live on Render
MODEL_PATH = os.environ.get("STUFF_MODEL_PATH", "models/stuff_model.pkl")
META_PATH  = os.environ.get("STUFF_META_PATH",  "models/stuff_model_meta.json")

KEY_COLS = ["game_pk", "at_bat_number", "pitch_number"]

def utc_now():
    return datetime.now(timezone.utc)

def build_engine():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL env var not set.")
    return create_engine(DATABASE_URL, pool_pre_ping=True)

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

def ctx_key(balls, strikes, outs_when_up, base_state):
    return f"{int(balls)}|{int(strikes)}|{int(outs_when_up)}|{int(base_state)}"

def main():
    # --- load artifacts ---
    pipe = joblib.load(MODEL_PATH)
    with open(META_PATH, "r", encoding="utf-8") as f:
        meta = json.load(f)

    num_features = meta["num_features"]
    cat_features = meta["cat_features"]
    feats = num_features + cat_features

    ctx_map = meta.get("context_baseline_map", {})  # str -> float
    goodness_std = float(meta.get("goodness_std", 0.01)) or 0.01

    stuff_center = float(meta.get("stuff_center", 100.0))
    stuff_scale  = float(meta.get("stuff_scale", 15.0))
    clip_min     = float(meta.get("stuff_clip_min", 40.0))
    clip_max     = float(meta.get("stuff_clip_max", 160.0))

    model_version = meta.get("model_version", "unknown")

    engine = build_engine()
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

    with engine.begin() as conn:
        while True:
            df = pd.read_sql(select_sql, conn, params={"cutoff": cutoff.date(), "lim": BATCH_SIZE})
            if df.empty:
                break

            # sanity checks
            missing_keys = [c for c in KEY_COLS if c not in df.columns]
            if missing_keys:
                raise RuntimeError(f"Missing key cols: {missing_keys}")

            missing_feats = [c for c in feats if c not in df.columns]
            if missing_feats:
                raise RuntimeError(f"Missing feature cols in {TABLE_NAME}: {missing_feats}")

            # --- predict rv_resid_hat ---
            X = df[feats].copy()
            rv_resid_hat = pipe.predict(X)

            # --- Stuff+ mapping (your training convention) ---
            # goodness = -rv_resid_hat (higher is better)
            goodness = -pd.Series(rv_resid_hat)

            stuff_plus = stuff_center + stuff_scale * (goodness / goodness_std)
            stuff_plus = stuff_plus.apply(lambda v: clip(safe_float(v), clip_min, clip_max))

            out = df[KEY_COLS].copy()
            out["stuff_raw"] = [safe_float(v) for v in rv_resid_hat]  # store model output (rv_resid_hat)
            out["stuff_plus"] = stuff_plus.values
            out["stuff_model_version"] = str(model_version)
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

            total += len(payload)
            print(f"[{utc_now().isoformat()}] updated={len(payload)} total={total}")

    print(f"DONE total_scored={total}")

if __name__ == "__main__":
    main()
