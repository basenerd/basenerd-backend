#!/usr/bin/env python3
"""Score Stuff+ and Control+ over statcast_pitches into pitch_model_scores.

Replaces the orphaned statcast_pitches_live path: the main statcast_pitches
table is updated nightly with all regular-season pitches and has every physics
feature the models need. This scores unscored pitches and upserts the results
into pitch_model_scores, keyed by (game_pk, at_bat_number, pitch_number) — the
same keys the arsenal/report joins already use.

Stuff+ uses the per-pitch-type model registry (models/stuff_models/), which
loads cleanly. Control+ uses control_model.pkl via the pipeline-bypass ported
from generate_pitcher_report_pdf.py (the pickled ColumnTransformer/loss don't
deserialize across sklearn versions).

Env knobs:
  SCORE_MIN_YEAR   only score pitches with game_year >= this (default 2025)
  SCORE_BATCH_SIZE rows per batch (default 40000)
  SCORE_MAX_BATCHES safety cap on batches per run (default 1000)
"""
import os
import sys
import json

import numpy as np
import pandas as pd
import joblib

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "game_prediction"))
from db_utils import get_conn  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STUFF_REGISTRY = os.path.join(ROOT, "models", "stuff_models", "registry.json")
CTRL_MODEL = os.path.join(ROOT, "models", "control_model.pkl")
CTRL_META = os.path.join(ROOT, "models", "control_model_meta.json")

MIN_YEAR = int(os.environ.get("SCORE_MIN_YEAR", "2025"))
BATCH_SIZE = int(os.environ.get("SCORE_BATCH_SIZE", "40000"))
MAX_BATCHES = int(os.environ.get("SCORE_MAX_BATCHES", "1000"))

SCORE_TABLE = "pitch_model_scores"

# Base columns pulled for scoring (superset of both models' features).
PULL_COLS = [
    "game_pk", "at_bat_number", "pitch_number", "pitch_type", "p_throws",
    "release_speed", "release_spin_rate", "release_extension",
    "release_pos_x", "release_pos_z", "release_pos_y",
    "pfx_x", "pfx_z", "vx0", "vy0", "vz0", "ax", "ay", "az",
    "sz_top", "sz_bot", "plate_x", "plate_z",
]

_Y_PLATE = 17.0 / 12.0


def _approach_angles(df):
    y0 = df["release_pos_y"].to_numpy(np.float64)
    vy0 = df["vy0"].to_numpy(np.float64)
    ay = df["ay"].to_numpy(np.float64)
    vx0 = df["vx0"].to_numpy(np.float64)
    vz0 = df["vz0"].to_numpy(np.float64)
    ax = df["ax"].to_numpy(np.float64)
    az = df["az"].to_numpy(np.float64)
    a = 0.5 * ay
    b = vy0
    c = y0 - _Y_PLATE
    disc = np.maximum(b**2 - 4.0 * a * c, 0.0)
    sq = np.sqrt(disc)
    with np.errstate(divide="ignore", invalid="ignore"):
        t1 = (-b + sq) / (2.0 * a)
        t2 = (-b - sq) / (2.0 * a)
    t = np.where((t2 > 0) & (t2 < t1), t2, t1)
    t = np.where(t > 0, t, np.nan)
    vy_p = vy0 + ay * t
    vz_p = vz0 + az * t
    vx_p = vx0 + ax * t
    absvy = np.abs(vy_p)
    with np.errstate(divide="ignore", invalid="ignore"):
        df["vert_approach_angle"] = np.degrees(np.arctan2(vz_p, absvy))
        df["horiz_approach_angle"] = np.degrees(np.arctan2(vx_p, absvy))
    return df


# ---------------------------------------------------------------------------
# Stuff+ (per-pitch-type registry)
# ---------------------------------------------------------------------------
def load_stuff():
    reg = json.load(open(STUFF_REGISTRY, encoding="utf-8"))
    mdir = os.path.dirname(STUFF_REGISTRY)
    pipes = {}
    for pt, info in reg.get("pitch_type_models", {}).items():
        pipes[pt] = {
            "pipe": joblib.load(os.path.join(mdir, info["model_file"])),
            "goodness_std": float(info["goodness_std"]) or 0.01,
            "meta": json.load(open(os.path.join(mdir, info["meta_file"]), encoding="utf-8")),
        }
    fk = reg.get("fallback_model", "_global")
    pipes["_global"] = {
        "pipe": joblib.load(os.path.join(mdir, f"{fk}.pkl")),
        "goodness_std": float(reg.get("global_goodness_std", 0.01)) or 0.01,
        "meta": json.load(open(os.path.join(mdir, f"{fk}_meta.json"), encoding="utf-8")),
    }
    return reg, pipes


def score_stuff(df, reg, pipes):
    center = float(reg.get("stuff_center", 100.0))
    scale = float(reg.get("stuff_scale", 15.0))
    cmin = float(reg.get("stuff_clip_min", 40.0))
    cmax = float(reg.get("stuff_clip_max", 160.0))
    version = reg.get("model_version", "unknown")
    df = _approach_angles(df)
    out = pd.Series(np.nan, index=df.index)
    for pt, g in df.groupby("pitch_type"):
        mi = pipes.get(pt, pipes["_global"])
        fc = mi["meta"]["num_features"] + mi["meta"]["cat_features"]
        preds = mi["pipe"].predict(g[fc])
        sp = center + scale * (-pd.Series(preds, index=g.index) / mi["goodness_std"])
        out.loc[g.index] = sp.clip(cmin, cmax)
    return out, version


# ---------------------------------------------------------------------------
# Control+ (pipeline-bypass, ported from generate_pitcher_report_pdf.py)
# ---------------------------------------------------------------------------
def load_control():
    pipe = joblib.load(CTRL_MODEL)
    meta = json.load(open(CTRL_META, encoding="utf-8"))
    return pipe, meta


def score_control(df, pipe, meta):
    num_feats = meta["num_features"]
    cat_feats = meta["cat_features"]
    gstd = float(meta.get("goodness_std", 0.01)) or 0.01
    center = float(meta.get("control_center", 100.0))
    scale = float(meta.get("control_scale", 15.0))
    cmin = float(meta.get("control_clip_min", 40.0))
    cmax = float(meta.get("control_clip_max", 160.0))
    version = meta.get("model_version", "unknown")

    model = pipe.named_steps["model"]
    if not hasattr(model, "_preprocessor"):
        model._preprocessor = None
    if not hasattr(model._loss, "link"):
        try:
            from sklearn._loss.loss import HalfSquaredError
        except ImportError:
            from sklearn.ensemble._hist_gradient_boosting.loss import LeastSquares as HalfSquaredError
        model._loss = HalfSquaredError()

    # Only rows with all control features present
    mask = df[num_feats + cat_feats].notna().all(axis=1)
    out = pd.Series(np.nan, index=df.index)
    if not mask.any():
        return out, version

    sub = df.loc[mask]
    num_data = sub[num_feats].to_numpy(np.float64)
    n_bins = model._bin_mapper.n_bins_non_missing_[-1]
    all_types = sorted(['CH', 'CS', 'CU', 'EP', 'FA', 'FC', 'FF', 'FS', 'FT',
                        'KC', 'KN', 'SI', 'SL', 'ST', 'SV', 'SC', 'PO'])[:n_bins]
    type_map = {pt: float(i) for i, pt in enumerate(all_types)}
    cat_enc = np.array([type_map.get(v, 0.0) for v in sub[cat_feats[0]].to_numpy()]).reshape(-1, 1)
    X = np.hstack([num_data, cat_enc])
    preds = model._raw_predict(X).ravel()
    cp = center + scale * (-pd.Series(preds, index=sub.index) / gstd)
    out.loc[sub.index] = cp.clip(cmin, cmax)
    return out, version


# ---------------------------------------------------------------------------
def ensure_table(cur):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCORE_TABLE} (
            game_pk BIGINT NOT NULL,
            at_bat_number INTEGER NOT NULL,
            pitch_number INTEGER NOT NULL,
            stuff_raw DOUBLE PRECISION,
            stuff_plus DOUBLE PRECISION,
            control_raw DOUBLE PRECISION,
            control_plus DOUBLE PRECISION,
            stuff_model_version TEXT,
            control_model_version TEXT,
            updated_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (game_pk, at_bat_number, pitch_number)
        )
    """)


def _clean(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return v.item() if hasattr(v, "item") else v


def upsert(cur, df, stuff, s_ver, control, c_ver):
    cur.execute("DROP TABLE IF EXISTS _score_stage")
    cur.execute("""
        CREATE TEMP TABLE _score_stage (
            game_pk BIGINT, at_bat_number INTEGER, pitch_number INTEGER,
            stuff_plus DOUBLE PRECISION, control_plus DOUBLE PRECISION,
            stuff_model_version TEXT, control_model_version TEXT
        ) ON COMMIT DROP
    """)
    cols = "(game_pk, at_bat_number, pitch_number, stuff_plus, control_plus, stuff_model_version, control_model_version)"
    with cur.copy(f"COPY _score_stage {cols} FROM STDIN") as copy:
        for idx, row in df.iterrows():
            sv = _clean(stuff.loc[idx])
            cv = _clean(control.loc[idx])
            copy.write_row([
                int(row["game_pk"]), int(row["at_bat_number"]), int(row["pitch_number"]),
                sv, cv,
                s_ver if sv is not None else None,
                c_ver if cv is not None else None,
            ])
    cur.execute(f"""
        INSERT INTO {SCORE_TABLE}
            (game_pk, at_bat_number, pitch_number, stuff_plus, control_plus,
             stuff_model_version, control_model_version, updated_at)
        SELECT game_pk, at_bat_number, pitch_number, stuff_plus, control_plus,
               stuff_model_version, control_model_version, now()
        FROM _score_stage
        ON CONFLICT (game_pk, at_bat_number, pitch_number) DO UPDATE SET
            stuff_plus = EXCLUDED.stuff_plus,
            control_plus = EXCLUDED.control_plus,
            stuff_model_version = EXCLUDED.stuff_model_version,
            control_model_version = EXCLUDED.control_model_version,
            updated_at = now()
    """)


SELECT_SQL = f"""
    SELECT {', '.join('sp.' + c for c in PULL_COLS)}
    FROM statcast_pitches sp
    LEFT JOIN {SCORE_TABLE} s
        ON s.game_pk = sp.game_pk
        AND s.at_bat_number = sp.at_bat_number
        AND s.pitch_number = sp.pitch_number
    WHERE sp.game_type = 'R'
      AND sp.game_year >= %s
      AND sp.pitch_type IS NOT NULL AND sp.pitch_type != ''
      AND sp.vy0 IS NOT NULL
      AND s.game_pk IS NULL
    LIMIT %s
"""


def main():
    print(f"Scoring pitches (min_year={MIN_YEAR}, batch={BATCH_SIZE})")
    reg, stuff_pipes = load_stuff()
    ctrl_pipe, ctrl_meta = load_control()
    print("Models loaded.")

    total = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            ensure_table(cur)
        conn.commit()

        for batch_i in range(MAX_BATCHES):
            df = pd.read_sql(SELECT_SQL, conn, params=(MIN_YEAR, BATCH_SIZE))
            if df.empty:
                print("No more unscored pitches.")
                break

            stuff, s_ver = score_stuff(df.copy(), reg, stuff_pipes)
            control, c_ver = score_control(df.copy(), ctrl_pipe, ctrl_meta)

            with conn.cursor() as cur:
                upsert(cur, df, stuff, s_ver, control, c_ver)
            conn.commit()

            total += len(df)
            print(f"  batch {batch_i+1}: scored {len(df):,} "
                  f"(stuff={int(stuff.notna().sum()):,}, control={int(control.notna().sum()):,}) "
                  f"| total={total:,}")

    print(f"DONE. Scored {total:,} pitches -> {SCORE_TABLE}")


if __name__ == "__main__":
    main()
