#!/usr/bin/env python3
"""
Build training data for per-umpire strike zone models.

Joins statcast called pitches with umpire IDs (from game_outcomes.parquet)
to produce a per-pitch training dataset with engineered features.

Output: data/umpire_zone_train.parquet

Requires:
  - data/game_outcomes.parquet  (run build_game_outcomes.py first)
  - DATABASE_URL env var pointing to statcast_pitches table
"""

import os
import sys
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "game_prediction"))
from db_utils import query_df

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT_PATH = os.path.join(DATA_DIR, "umpire_zone_train.parquet")
GAME_OUTCOMES_PATH = os.path.join(DATA_DIR, "game_outcomes.parquet")

ZONE_X_HALF = 0.83  # half plate width in feet


def build():
    # --- Load umpire-game mapping ---
    if not os.path.exists(GAME_OUTCOMES_PATH):
        print(f"game_outcomes.parquet not found at {GAME_OUTCOMES_PATH}")
        print("Run build_game_outcomes.py first.")
        return

    game_outcomes = pd.read_parquet(GAME_OUTCOMES_PATH)
    umpire_games = game_outcomes[["game_pk", "hp_umpire_id"]].copy()
    umpire_games = umpire_games[umpire_games["hp_umpire_id"].notna()]
    umpire_games["hp_umpire_id"] = umpire_games["hp_umpire_id"].astype(int)
    print(f"  {len(umpire_games):,} games with umpire data")

    # --- Query called pitches ---
    print("Querying called pitches from statcast_pitches...")
    sql = """
    SELECT
        game_pk,
        game_year,
        game_date,
        plate_x,
        plate_z,
        sz_top,
        sz_bot,
        pitch_type,
        stand,
        description,
        balls,
        strikes
    FROM statcast_pitches
    WHERE game_type = 'R'
      AND game_year BETWEEN 2021 AND 2025
      AND description IN ('called_strike', 'ball', 'blocked_ball')
      AND plate_x IS NOT NULL
      AND plate_z IS NOT NULL
      AND sz_top IS NOT NULL
      AND sz_bot IS NOT NULL
      AND pitch_type IS NOT NULL
    """
    pitches = query_df(sql)
    print(f"  {len(pitches):,} called pitches loaded")

    if pitches.empty:
        print("No data found. Exiting.")
        return

    # --- Join umpire IDs ---
    pitches = pitches.merge(
        umpire_games[["game_pk", "hp_umpire_id"]],
        on="game_pk", how="inner"
    )
    print(f"  {len(pitches):,} pitches matched to umpires")

    # --- Engineer features ---
    # Normalized vertical position (0 = knees, 1 = letters)
    sz_range = pitches["sz_top"] - pitches["sz_bot"]
    pitches["plate_z_norm"] = np.where(
        sz_range > 0,
        (pitches["plate_z"] - pitches["sz_bot"]) / sz_range,
        0.5
    )

    # Distance from zone edges (positive = outside zone)
    pitches["dist_from_edge_x"] = pitches["plate_x"].abs() - ZONE_X_HALF
    pitches["dist_from_edge_z_top"] = pitches["plate_z"] - pitches["sz_top"]
    pitches["dist_from_edge_z_bot"] = pitches["sz_bot"] - pitches["plate_z"]

    # Target
    pitches["is_called_strike"] = (pitches["description"] == "called_strike").astype(int)

    # --- Select output columns ---
    output_cols = [
        # identifiers
        "game_pk", "game_year", "game_date", "hp_umpire_id",
        # location features
        "plate_x", "plate_z", "plate_z_norm", "sz_top", "sz_bot",
        "dist_from_edge_x", "dist_from_edge_z_top", "dist_from_edge_z_bot",
        # pitch context
        "pitch_type", "stand", "balls", "strikes",
        # target
        "is_called_strike",
    ]
    result = pitches[output_cols].copy()

    # Ensure numeric types
    for col in ["balls", "strikes"]:
        result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0).astype(int)

    os.makedirs(DATA_DIR, exist_ok=True)
    result.to_parquet(OUTPUT_PATH, index=False)

    print(f"\nTraining data saved to {OUTPUT_PATH}")
    print(f"  {len(result):,} rows")
    print(f"  {result['hp_umpire_id'].nunique():,} unique umpires")
    print(f"  Called strike rate: {result['is_called_strike'].mean():.3f}")
    print(f"  Pitch types: {sorted(result['pitch_type'].unique())}")
    print(f"  Seasons: {sorted(result['game_year'].unique())}")

    # Per-umpire counts
    ump_counts = result.groupby("hp_umpire_id").size().sort_values(ascending=False)
    print(f"\n  Pitches per umpire: median={ump_counts.median():.0f}, "
          f"min={ump_counts.min()}, max={ump_counts.max()}")
    print(f"  Umpires with >= 2000 pitches: {(ump_counts >= 2000).sum()}")


if __name__ == "__main__":
    build()
