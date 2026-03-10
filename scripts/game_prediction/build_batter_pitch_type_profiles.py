#!/usr/bin/env python3
"""
Build batter performance profiles broken down by pitch type.

Captures how each batter performs against specific pitch categories
(fastballs, breaking balls, offspeed) and individual pitch types.

Output: data/batter_pitch_type_profiles.parquet
"""

import os
import sys
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import query_df

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "batter_pitch_type_profiles.parquet")

# Map pitch types to categories
PITCH_CATEGORY = {
    "FF": "fastball", "SI": "fastball", "FC": "fastball",
    "SL": "breaking", "CU": "breaking", "KC": "breaking", "ST": "breaking", "SV": "breaking",
    "CH": "offspeed", "FS": "offspeed",
    "KN": "offspeed",
}

VALID_PITCH_TYPES = set(PITCH_CATEGORY.keys())


def build_batter_pitch_type_profiles():
    print("Querying batter-vs-pitch-type data (server-side aggregation)...")

    sql = """
    SELECT
        batter,
        game_year,
        pitch_type,
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
        -- BIP outcomes
        SUM(CASE WHEN events IN ('strikeout','strikeout_double_play') THEN 1 ELSE 0 END) AS ks,
        SUM(CASE WHEN events IN ('single','double','triple','home_run') THEN 1 ELSE 0 END) AS hits,
        SUM(CASE WHEN events = 'home_run' THEN 1 ELSE 0 END) AS hrs,
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
        AVG(estimated_woba_using_speedangle) AS xwoba,
        -- Called strikes (pitch-level, not PA-level)
        SUM(CASE WHEN description = 'called_strike' THEN 1 ELSE 0 END) AS called_strikes
    FROM statcast_pitches
    WHERE game_type = 'R'
      AND game_year BETWEEN 2021 AND 2025
      AND pitch_type IS NOT NULL
      AND pitch_type != ''
    GROUP BY batter, game_year, pitch_type
    """
    df = query_df(sql)
    print(f"  {len(df):,} batter/year/pitch_type groups")

    if df.empty:
        print("No data found.")
        return

    # Filter to valid pitch types
    df = df[df["pitch_type"].isin(VALID_PITCH_TYPES)].copy()
    print(f"  {len(df):,} after filtering to valid pitch types")

    # Compute rate stats
    df["whiff_rate"] = np.where(df["swings"] > 0, df["whiffs"] / df["swings"], np.nan)
    df["chase_rate"] = np.where(df["chase_opps"] > 0, df["chases"] / df["chase_opps"], np.nan)
    df["zone_swing_rate"] = np.where(df["in_zone_pitches"] > 0,
                                      df["zone_swings"] / df["in_zone_pitches"], np.nan)
    df["zone_contact_rate"] = np.where(df["zone_swings"] > 0,
                                        df["zone_contacts"] / df["zone_swings"], np.nan)
    df["hard_hit_rate"] = np.where(df["bip"] > 0, df["hard_hits"] / df["bip"], np.nan)
    df["csw_rate"] = (df["called_strikes"] + df["whiffs"]) / df["pitches"]

    # Add pitch category
    df["pitch_category"] = df["pitch_type"].map(PITCH_CATEGORY)

    # --- Also build category-level aggregates (fastball/breaking/offspeed) ---
    cat_agg = df.groupby(["batter", "game_year", "pitch_category"]).agg(
        pitches=("pitches", "sum"),
        pa=("pa", "sum"),
        swings=("swings", "sum"),
        whiffs=("whiffs", "sum"),
        in_zone_pitches=("in_zone_pitches", "sum"),
        chase_opps=("chase_opps", "sum"),
        chases=("chases", "sum"),
        zone_swings=("zone_swings", "sum"),
        zone_contacts=("zone_contacts", "sum"),
        ks=("ks", "sum"),
        hits=("hits", "sum"),
        hrs=("hrs", "sum"),
        bip=("bip", "sum"),
        hard_hits=("hard_hits", "sum"),
        called_strikes=("called_strikes", "sum"),
    ).reset_index()

    # Weighted averages for contact quality
    def _weighted_mean(group, val_col, weight_col):
        valid = group[val_col].notna() & (group[weight_col] > 0)
        if not valid.any():
            return np.nan
        return np.average(group.loc[valid, val_col], weights=group.loc[valid, weight_col])

    cat_ev = df.groupby(["batter", "game_year", "pitch_category"]).apply(
        lambda g: pd.Series({
            "avg_ev": _weighted_mean(g, "avg_ev", "bip"),
            "avg_la": _weighted_mean(g, "avg_la", "bip"),
            "xwoba": _weighted_mean(g, "xwoba", "pa"),
        })
    ).reset_index()
    cat_agg = cat_agg.merge(cat_ev, on=["batter", "game_year", "pitch_category"], how="left")

    cat_agg["whiff_rate"] = np.where(cat_agg["swings"] > 0, cat_agg["whiffs"] / cat_agg["swings"], np.nan)
    cat_agg["chase_rate"] = np.where(cat_agg["chase_opps"] > 0, cat_agg["chases"] / cat_agg["chase_opps"], np.nan)
    cat_agg["zone_swing_rate"] = np.where(cat_agg["in_zone_pitches"] > 0,
                                           cat_agg["zone_swings"] / cat_agg["in_zone_pitches"], np.nan)
    cat_agg["zone_contact_rate"] = np.where(cat_agg["zone_swings"] > 0,
                                             cat_agg["zone_contacts"] / cat_agg["zone_swings"], np.nan)
    cat_agg["hard_hit_rate"] = np.where(cat_agg["bip"] > 0, cat_agg["hard_hits"] / cat_agg["bip"], np.nan)
    cat_agg["csw_rate"] = (cat_agg["called_strikes"] + cat_agg["whiffs"]) / cat_agg["pitches"]

    # Use pitch_type="CAT_fastball" etc. for category rows
    cat_agg["pitch_type"] = "CAT_" + cat_agg["pitch_category"]

    # Keep columns consistent
    keep_cols = [
        "batter", "game_year", "pitch_type", "pitch_category",
        "pitches", "pa", "swings", "whiffs",
        "whiff_rate", "chase_rate", "zone_swing_rate", "zone_contact_rate",
        "hard_hit_rate", "csw_rate",
        "avg_ev", "avg_la", "xwoba",
        "ks", "hits", "hrs", "bip",
    ]

    # Add pitch_category to individual pitch type rows too (already there)
    per_type = df[keep_cols].copy()
    cat_rows = cat_agg[keep_cols].copy()

    final = pd.concat([per_type, cat_rows], ignore_index=True)
    final = final.rename(columns={"game_year": "season"})
    final = final.sort_values(["season", "batter", "pitch_type"]).reset_index(drop=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    final.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nBatter pitch-type profiles saved to {OUTPUT_PATH}")
    print(f"  {len(final):,} rows ({final['batter'].nunique():,} batters)")
    print(f"\nPitch type breakdown:")
    print(final["pitch_type"].value_counts().to_string())


if __name__ == "__main__":
    build_batter_pitch_type_profiles()
