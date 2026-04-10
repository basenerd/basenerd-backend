#!/usr/bin/env python3
"""
Build within-AB pitch sequence context features.

Uses SQL window functions to compute most features server-side,
avoiding pulling millions of raw pitch rows over the network.

Output: data/pitch_sequence_features.parquet
  Keyed on (batter, pitcher, game_pk, at_bat_number)
"""

import os
import sys
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import query_df

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "pitch_sequence_features.parquet")


def build_pitch_sequence_features():
    from datetime import date
    current_year = date.today().year

    print("Querying pitch sequence features (server-side aggregation)...")

    # Do ALL the heavy lifting in SQL using window functions
    # This computes per-AB aggregates without pulling raw pitch data
    # Use inline CASE expressions in window functions since PostgreSQL
    # can't reference column aliases in the same SELECT level
    sql = f"""
    WITH ab_pitches AS (
        SELECT
            batter, pitcher, game_pk, game_date, at_bat_number,
            pitch_number, pitch_type, description, events, release_speed, zone,
            CASE
                WHEN pitch_type IN ('FF','SI','FC') THEN 0
                WHEN pitch_type IN ('SL','CU','KC','ST','SV') THEN 1
                WHEN pitch_type IN ('CH','FS','KN') THEN 2
                ELSE 3
            END AS pitch_cat,
            -- LAG: previous pitch info
            LAG(CASE WHEN pitch_type IN ('FF','SI','FC') THEN 0
                     WHEN pitch_type IN ('SL','CU','KC','ST','SV') THEN 1
                     WHEN pitch_type IN ('CH','FS','KN') THEN 2 ELSE 3 END)
                OVER (PARTITION BY game_pk, at_bat_number ORDER BY pitch_number) AS prev_pitch_cat,
            LAG(CASE WHEN description IN ('swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt')
                     THEN 1 ELSE 0 END)
                OVER (PARTITION BY game_pk, at_bat_number ORDER BY pitch_number) AS prev_was_whiff,
            LAG(CASE WHEN description IN ('called_strike','swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt')
                     THEN 1 ELSE 0 END)
                OVER (PARTITION BY game_pk, at_bat_number ORDER BY pitch_number) AS prev_was_strike,
            LAG(CASE WHEN description IN ('ball','blocked_ball','hit_by_pitch','pitchout','intent_ball')
                     THEN 1 ELSE 0 END)
                OVER (PARTITION BY game_pk, at_bat_number ORDER BY pitch_number) AS prev_was_ball,
            FIRST_VALUE(release_speed)
                OVER (PARTITION BY game_pk, at_bat_number ORDER BY pitch_number) AS first_velo,
            MAX(pitch_number) OVER (PARTITION BY game_pk, at_bat_number) AS max_pitch_in_ab,
            COUNT(*) OVER (PARTITION BY game_pk, at_bat_number) AS n_pitches_in_ab,
            -- Cumulative whiffs before this pitch
            SUM(CASE WHEN description IN ('swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt')
                     THEN 1 ELSE 0 END)
                OVER (PARTITION BY game_pk, at_bat_number ORDER BY pitch_number
                      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cum_whiffs,
            -- Cumulative chases before this pitch
            SUM(CASE WHEN zone IN (11,12,13,14)
                     AND description IN ('swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt',
                                         'foul','foul_bunt','hit_into_play')
                     THEN 1 ELSE 0 END)
                OVER (PARTITION BY game_pk, at_bat_number ORDER BY pitch_number
                      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cum_chases,
            -- Cumulative in-zone pitches before this pitch
            SUM(CASE WHEN zone BETWEEN 1 AND 9 THEN 1 ELSE 0 END)
                OVER (PARTITION BY game_pk, at_bat_number ORDER BY pitch_number
                      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cum_in_zone,
            -- Cumulative fastballs before this pitch
            SUM(CASE WHEN pitch_type IN ('FF','SI','FC') THEN 1 ELSE 0 END)
                OVER (PARTITION BY game_pk, at_bat_number ORDER BY pitch_number
                      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cum_fastballs,
            -- Count of pitches before this one
            COUNT(*) OVER (PARTITION BY game_pk, at_bat_number ORDER BY pitch_number
                      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cum_pitches
        FROM statcast_pitches
        WHERE game_type = 'R'
          AND game_year BETWEEN 2021 AND {current_year}
          AND pitch_type IS NOT NULL AND pitch_type != ''
    )
    SELECT
        batter, pitcher, game_pk, game_date, at_bat_number,
        n_pitches_in_ab AS seq_n_pitches,
        COALESCE(prev_pitch_cat, -1) AS seq_prev_pitch_cat,
        COALESCE(prev_was_whiff, 0) AS seq_prev_was_whiff,
        COALESCE(prev_was_strike, 0) AS seq_prev_was_strike,
        COALESCE(prev_was_ball, 0) AS seq_prev_was_ball,
        CASE WHEN prev_pitch_cat IS NOT NULL AND prev_pitch_cat = pitch_cat THEN 1 ELSE 0 END AS seq_prev_same_cat,
        CASE WHEN cum_pitches > 0 THEN cum_fastballs::float / cum_pitches ELSE 0.5 END AS seq_fb_pct_in_ab,
        COALESCE(cum_whiffs, 0) AS seq_whiffs_in_ab,
        COALESCE(cum_chases, 0) AS seq_chases_in_ab,
        CASE WHEN cum_pitches > 0 THEN cum_in_zone::float / cum_pitches ELSE 0.5 END AS seq_zone_pct_in_ab,
        CASE WHEN release_speed IS NOT NULL AND first_velo IS NOT NULL
             THEN release_speed - first_velo ELSE 0 END AS seq_velo_trend
    FROM ab_pitches
    WHERE events IS NOT NULL AND events != ''
      AND pitch_number = max_pitch_in_ab
    """

    df = query_df(sql)
    print(f"  {len(df):,} plate appearances with sequence features")

    if df.empty:
        print("No data. Exiting.")
        return

    df["game_date"] = pd.to_datetime(df["game_date"])

    # Convert numeric columns
    for col in df.columns:
        if col.startswith("seq_"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nPitch sequence features saved: {len(df):,} rows")


if __name__ == "__main__":
    build_pitch_sequence_features()
