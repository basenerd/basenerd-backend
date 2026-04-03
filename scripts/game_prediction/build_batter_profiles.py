#!/usr/bin/env python3
"""
Build batter profiles from statcast_pitches.
Server-side aggregation to avoid pulling millions of rows.

Output: data/batter_profiles.parquet
"""

import os
import sys
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import query_df

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "batter_profiles.parquet")


def _run_batter_query(group_by_p_throws=True):
    """Run server-side aggregation for batter profiles."""
    from datetime import date
    current_year = date.today().year
    p_throws_col = "p_throws," if group_by_p_throws else ""
    p_throws_group = ", p_throws" if group_by_p_throws else ""

    sql = f"""
    SELECT
        batter,
        game_year,
        {p_throws_col}
        COUNT(*) AS pitches,
        SUM(CASE WHEN events IS NOT NULL AND events != '' THEN 1 ELSE 0 END) AS pa,
        -- Swings
        SUM(CASE WHEN description IN (
            'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt',
            'foul','foul_bunt','hit_into_play'
        ) THEN 1 ELSE 0 END) AS swings,
        SUM(CASE WHEN description IN (
            'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt'
        ) THEN 1 ELSE 0 END) AS whiffs,
        -- Zone discipline
        SUM(CASE WHEN zone BETWEEN 1 AND 9 THEN 1 ELSE 0 END) AS in_zone_pitches,
        SUM(CASE WHEN zone IN (11,12,13,14) THEN 1 ELSE 0 END) AS chase_opps,
        SUM(CASE WHEN zone IN (11,12,13,14)
            AND description IN (
                'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt',
                'foul','foul_bunt','hit_into_play'
            ) THEN 1 ELSE 0 END) AS chases,
        SUM(CASE WHEN zone BETWEEN 1 AND 9
            AND description IN (
                'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt',
                'foul','foul_bunt','hit_into_play'
            ) THEN 1 ELSE 0 END) AS zone_swings,
        SUM(CASE WHEN zone BETWEEN 1 AND 9
            AND description IN ('foul','foul_bunt','hit_into_play')
            THEN 1 ELSE 0 END) AS zone_contacts,
        -- PA outcomes
        SUM(CASE WHEN events IN ('strikeout','strikeout_double_play') THEN 1 ELSE 0 END) AS ks,
        SUM(CASE WHEN events = 'walk' THEN 1 ELSE 0 END) AS bbs,
        SUM(CASE WHEN events IN ('single','double','triple','home_run') THEN 1 ELSE 0 END) AS hits,
        SUM(CASE WHEN events = 'home_run' THEN 1 ELSE 0 END) AS hrs,
        SUM(CASE WHEN events = 'single' THEN 1 ELSE 0 END) AS singles,
        -- BIP
        SUM(CASE WHEN events IN (
            'single','double','triple','home_run','field_out',
            'grounded_into_double_play','double_play','fielders_choice',
            'fielders_choice_out','force_out','field_error','sac_fly',
            'sac_bunt','triple_play'
        ) THEN 1 ELSE 0 END) AS bip,
        -- Contact quality
        AVG(launch_speed) AS avg_ev,
        AVG(launch_angle) AS avg_la,
        SUM(CASE WHEN launch_speed >= 95 THEN 1 ELSE 0 END) AS hard_hits,
        SUM(CASE WHEN launch_angle BETWEEN 8 AND 32 THEN 1 ELSE 0 END) AS sweet_spots,
        SUM(CASE WHEN launch_angle < 10 AND launch_angle IS NOT NULL THEN 1 ELSE 0 END) AS gbs,
        SUM(CASE WHEN launch_angle >= 25 AND launch_angle IS NOT NULL THEN 1 ELSE 0 END) AS fbs,
        AVG(estimated_woba_using_speedangle) AS xwoba
    FROM statcast_pitches
    WHERE game_type = 'R'
      AND game_year BETWEEN 2021 AND {current_year}
    GROUP BY batter, game_year{p_throws_group}
    """
    return query_df(sql)


def build_batter_profiles():
    print("Querying batter profiles (server-side aggregation)...")

    # Per pitcher handedness
    df_split = _run_batter_query(group_by_p_throws=True)
    df_split = df_split.rename(columns={"p_throws": "vs_hand"})
    print(f"  {len(df_split):,} batter/year/hand groups")

    # Combined (ALL)
    df_all = _run_batter_query(group_by_p_throws=False)
    df_all["vs_hand"] = "ALL"
    print(f"  {len(df_all):,} batter/year groups (ALL)")

    df = pd.concat([df_split, df_all], ignore_index=True)

    # Compute rate stats
    df["k_pct"] = np.where(df["pa"] > 0, df["ks"] / df["pa"], np.nan)
    df["bb_pct"] = np.where(df["pa"] > 0, df["bbs"] / df["pa"], np.nan)
    df["whiff_rate"] = np.where(df["swings"] > 0, df["whiffs"] / df["swings"], np.nan)
    df["chase_rate"] = np.where(df["chase_opps"] > 0, df["chases"] / df["chase_opps"], np.nan)
    df["zone_swing_rate"] = np.where(df["in_zone_pitches"] > 0,
                                      df["zone_swings"] / df["in_zone_pitches"], np.nan)
    df["zone_contact_rate"] = np.where(df["zone_swings"] > 0,
                                        df["zone_contacts"] / df["zone_swings"], np.nan)
    df["barrel_rate"] = np.nan  # can't compute barrel precisely in SQL
    df["hard_hit_rate"] = np.where(df["bip"] > 0, df["hard_hits"] / df["bip"], np.nan)
    df["sweet_spot_rate"] = np.where(df["bip"] > 0, df["sweet_spots"] / df["bip"], np.nan)
    df["gb_rate"] = np.where(df["bip"] > 0, df["gbs"] / df["bip"], np.nan)
    df["fb_rate"] = np.where(df["bip"] > 0, df["fbs"] / df["bip"], np.nan)
    df["hr_per_fb"] = np.where(df["fbs"] > 0, df["hrs"] / df["fbs"], np.nan)
    df["iso"] = np.where(df["pa"] > 0,
                          ((df["hits"] - df["singles"]) + df["hrs"] * 2) / df["pa"], np.nan)
    df["babip"] = np.where((df["bip"] - df["hrs"]) > 0,
                            (df["hits"] - df["hrs"]) / (df["bip"] - df["hrs"]), np.nan)

    keep_cols = [
        "batter", "game_year", "vs_hand", "pa", "pitches",
        "k_pct", "bb_pct", "whiff_rate", "chase_rate",
        "zone_swing_rate", "zone_contact_rate",
        "avg_ev", "avg_la", "barrel_rate", "hard_hit_rate", "sweet_spot_rate",
        "gb_rate", "fb_rate", "hr_per_fb", "iso", "babip", "xwoba",
    ]

    result = df[keep_cols].copy()
    result = result.rename(columns={"game_year": "season"})
    result = result.sort_values(["season", "batter", "vs_hand"]).reset_index(drop=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    result.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nBatter profiles saved to {OUTPUT_PATH}")
    print(f"  {len(result):,} rows ({result['batter'].nunique():,} batters)")


if __name__ == "__main__":
    build_batter_profiles()
