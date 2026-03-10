#!/usr/bin/env python3
"""
Build pitch selection training data from statcast_pitches.

For each pitch thrown, captures the context that drives pitch selection:
- Pitcher identity and arsenal
- Batter identity and handedness
- Count (balls, strikes)
- Outs, inning, score differential
- Baserunners
- Times through order
- Previous pitch in the AB

Target: pitch_type thrown

This dataset trains the pitch selection prediction model.

Output: data/pitch_selection_train.parquet
"""

import os
import sys
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import query_df

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "pitch_selection_train.parquet")


def build_pitch_selection_data():
    print("Querying pitch sequence data...")

    sql = """
    SELECT
        pitcher,
        batter,
        game_pk,
        game_date,
        game_year,
        at_bat_number,
        pitch_number,
        pitch_type,
        stand,
        p_throws,
        balls,
        strikes,
        outs_when_up,
        inning,
        inning_topbot,
        on_1b,
        on_2b,
        on_3b,
        bat_score,
        fld_score,
        n_thruorder_pitcher,
        release_speed,
        description
    FROM statcast_pitches
    WHERE game_type = 'R'
      AND game_year BETWEEN 2021 AND 2025
      AND pitch_type IS NOT NULL
      AND pitch_type != ''
      AND pitcher IS NOT NULL
      AND batter IS NOT NULL
    ORDER BY game_pk, at_bat_number, pitch_number
    """
    df = query_df(sql)
    print(f"  {len(df):,} pitches loaded")

    if df.empty:
        print("No data found. Exiting.")
        return

    # --- Feature engineering ---

    # Score differential from pitcher's perspective
    df["score_diff"] = df["fld_score"] - df["bat_score"]

    # Baserunner state encoding
    df["runners_on"] = (
        (df["on_1b"].notna() & (df["on_1b"] != 0)).astype(int) +
        (df["on_2b"].notna() & (df["on_2b"] != 0)).astype(int) +
        (df["on_3b"].notna() & (df["on_3b"] != 0)).astype(int)
    )
    df["runner_on_1b"] = (df["on_1b"].notna() & (df["on_1b"] != 0)).astype(int)
    df["runner_on_2b"] = (df["on_2b"].notna() & (df["on_2b"] != 0)).astype(int)
    df["runner_on_3b"] = (df["on_3b"].notna() & (df["on_3b"] != 0)).astype(int)

    # Scoring position flag
    df["risp"] = ((df["runner_on_2b"] == 1) | (df["runner_on_3b"] == 1)).astype(int)

    # Count encoding
    df["count_state"] = df["balls"].astype(str) + "-" + df["strikes"].astype(str)
    df["ahead_in_count"] = (df["strikes"] > df["balls"]).astype(int)
    df["behind_in_count"] = (df["balls"] > df["strikes"]).astype(int)
    df["two_strikes"] = (df["strikes"] == 2).astype(int)
    df["three_balls"] = (df["balls"] == 3).astype(int)
    df["first_pitch"] = ((df["balls"] == 0) & (df["strikes"] == 0)).astype(int)

    # Inning buckets
    df["early_innings"] = (df["inning"] <= 3).astype(int)
    df["mid_innings"] = ((df["inning"] >= 4) & (df["inning"] <= 6)).astype(int)
    df["late_innings"] = (df["inning"] >= 7).astype(int)

    # Previous pitch in the AB
    df = df.sort_values(["game_pk", "at_bat_number", "pitch_number"])
    df["prev_pitch_type"] = df.groupby(["game_pk", "at_bat_number"])["pitch_type"].shift(1)
    df["prev_pitch_velo"] = df.groupby(["game_pk", "at_bat_number"])["release_speed"].shift(1)
    df["prev_pitch_result"] = df.groupby(["game_pk", "at_bat_number"])["description"].shift(1)

    # Pitch number in the AB
    df["pitch_num_in_ab"] = df.groupby(["game_pk", "at_bat_number"]).cumcount() + 1

    # Select features and target
    feature_cols = [
        "pitcher", "batter", "game_pk", "game_date", "game_year",
        "at_bat_number", "pitch_number",
        "stand", "p_throws",
        "balls", "strikes", "count_state",
        "ahead_in_count", "behind_in_count", "two_strikes", "three_balls", "first_pitch",
        "outs_when_up", "inning",
        "early_innings", "mid_innings", "late_innings",
        "runner_on_1b", "runner_on_2b", "runner_on_3b", "runners_on", "risp",
        "score_diff",
        "n_thruorder_pitcher",
        "prev_pitch_type", "prev_pitch_velo", "prev_pitch_result",
        "pitch_num_in_ab",
        "pitch_type",  # TARGET
    ]

    result = df[feature_cols].copy()
    result = result.rename(columns={"game_year": "season"})

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    result.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nPitch selection training data saved to {OUTPUT_PATH}")
    print(f"  {len(result):,} pitches")
    print(f"\nPitch type distribution:")
    print(result["pitch_type"].value_counts().head(10).to_string())


if __name__ == "__main__":
    build_pitch_selection_data()
