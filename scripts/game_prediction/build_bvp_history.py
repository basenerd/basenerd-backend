#!/usr/bin/env python3
"""
Build batter vs pitcher head-to-head history features.

Uses SQL window functions for server-side aggregation to avoid
pulling millions of raw rows over the network.

Output: data/bvp_history.parquet
  Keyed on (batter, pitcher, game_pk)
"""

import os
import sys
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import query_df

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "bvp_history.parquet")


def build_bvp_history():
    from datetime import date
    current_year = date.today().year

    print("Querying BvP history (server-side aggregation)...")

    # Use window functions to compute cumulative stats per batter-pitcher matchup
    # BEFORE each game date (no look-ahead)
    sql = f"""
    WITH pa_events AS (
        SELECT
            batter,
            pitcher,
            game_pk,
            game_date,
            events,
            estimated_woba_using_speedangle,
            CASE WHEN events IN ('strikeout','strikeout_double_play') THEN 1 ELSE 0 END AS is_k,
            CASE WHEN events = 'walk' THEN 1 ELSE 0 END AS is_bb,
            CASE WHEN events IN ('single','double','triple','home_run') THEN 1 ELSE 0 END AS is_hit,
            CASE WHEN events = 'home_run' THEN 1 ELSE 0 END AS is_hr,
            ROW_NUMBER() OVER (PARTITION BY batter, pitcher ORDER BY game_date, game_pk) AS matchup_pa_num
        FROM statcast_pitches
        WHERE game_type = 'R'
          AND game_year BETWEEN 2021 AND {current_year}
          AND events IS NOT NULL
          AND events != ''
    ),
    matchup_cum AS (
        SELECT
            batter,
            pitcher,
            game_pk,
            game_date,
            matchup_pa_num,
            -- Cumulative stats BEFORE this PA (exclude current)
            matchup_pa_num - 1 AS prior_pa,
            SUM(is_k) OVER (PARTITION BY batter, pitcher ORDER BY game_date, game_pk
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cum_k,
            SUM(is_bb) OVER (PARTITION BY batter, pitcher ORDER BY game_date, game_pk
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cum_bb,
            SUM(is_hit) OVER (PARTITION BY batter, pitcher ORDER BY game_date, game_pk
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cum_hit,
            SUM(is_hr) OVER (PARTITION BY batter, pitcher ORDER BY game_date, game_pk
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cum_hr,
            SUM(COALESCE(estimated_woba_using_speedangle, 0)) OVER (
                PARTITION BY batter, pitcher ORDER BY game_date, game_pk
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cum_xwoba_sum,
            SUM(CASE WHEN estimated_woba_using_speedangle IS NOT NULL THEN 1 ELSE 0 END) OVER (
                PARTITION BY batter, pitcher ORDER BY game_date, game_pk
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cum_xwoba_n
        FROM pa_events
    )
    SELECT DISTINCT ON (batter, pitcher, game_pk)
        batter,
        pitcher,
        game_pk,
        game_date,
        prior_pa AS bvp_pa,
        CASE WHEN prior_pa > 0 THEN cum_k::float / prior_pa ELSE NULL END AS bvp_k_pct,
        CASE WHEN prior_pa > 0 THEN cum_bb::float / prior_pa ELSE NULL END AS bvp_bb_pct,
        CASE WHEN prior_pa > 0 THEN cum_hit::float / prior_pa ELSE NULL END AS bvp_hit_pct,
        CASE WHEN prior_pa > 0 THEN cum_hr::float / prior_pa ELSE NULL END AS bvp_hr_pct,
        CASE WHEN cum_xwoba_n > 0 THEN cum_xwoba_sum / cum_xwoba_n ELSE NULL END AS bvp_xwoba
    FROM matchup_cum
    WHERE prior_pa >= 1
    ORDER BY batter, pitcher, game_pk, matchup_pa_num DESC
    """

    df = query_df(sql)
    print(f"  {len(df):,} matchup-game rows with BvP history")

    if df.empty:
        print("No data. Exiting.")
        return

    df["game_date"] = pd.to_datetime(df["game_date"])
    for col in ["bvp_pa", "bvp_k_pct", "bvp_bb_pct", "bvp_hit_pct", "bvp_hr_pct", "bvp_xwoba"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Bayesian shrinkage toward league average for small samples
    RELIABLE_BVP_PA = 15
    LG_AVG = {"bvp_k_pct": 0.223, "bvp_bb_pct": 0.083,
              "bvp_hit_pct": 0.235, "bvp_hr_pct": 0.033, "bvp_xwoba": 0.315}

    for col, lg in LG_AVG.items():
        valid = df[col].notna()
        w = (df.loc[valid, "bvp_pa"] / RELIABLE_BVP_PA).clip(0, 1)
        df.loc[valid, col] = w * df.loc[valid, col] + (1 - w) * lg

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nBvP history saved: {len(df):,} rows")
    print(f"  Unique matchup pairs: {df.groupby(['batter', 'pitcher']).ngroups:,}")
    print(f"  Average PA history: {df['bvp_pa'].mean():.1f}")


if __name__ == "__main__":
    build_bvp_history()
