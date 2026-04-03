#!/usr/bin/env python3
"""
Build historical rolling 14-day form features for matchup training.

For each PA in the training set, computes the batter's and pitcher's
performance over the 14 calendar days BEFORE that PA's game_date.
This avoids look-ahead bias — each row only uses past data.

Output: data/recent_form_batter.parquet, data/recent_form_pitcher.parquet
"""

import os
import sys
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import query_df

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
BATTER_OUTPUT = os.path.join(OUTPUT_DIR, "recent_form_batter.parquet")
PITCHER_OUTPUT = os.path.join(OUTPUT_DIR, "recent_form_pitcher.parquet")

MIN_PITCHES = 20  # minimum pitches in window to use stats (else NaN)


def build_batter_recent_form():
    from datetime import date
    current_year = date.today().year
    print("Querying daily batter aggregates...")
    sql = f"""
    SELECT
        batter,
        game_date,
        COUNT(*) AS pitches,
        COUNT(*) FILTER (WHERE events IS NOT NULL AND events != '') AS pa,
        SUM(CASE WHEN events IN ('strikeout','strikeout_double_play') THEN 1 ELSE 0 END) AS ks,
        SUM(CASE WHEN events = 'walk' THEN 1 ELSE 0 END) AS bbs,
        SUM(CASE WHEN description IN (
            'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt',
            'foul','foul_bunt','hit_into_play'
        ) THEN 1 ELSE 0 END) AS swings,
        SUM(CASE WHEN description IN (
            'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt'
        ) THEN 1 ELSE 0 END) AS whiffs,
        SUM(CASE WHEN zone IN (11,12,13,14) THEN 1 ELSE 0 END) AS chase_opps,
        SUM(CASE WHEN zone IN (11,12,13,14) AND description IN (
            'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt',
            'foul','foul_bunt','hit_into_play'
        ) THEN 1 ELSE 0 END) AS chases,
        SUM(estimated_woba_using_speedangle) AS sum_xwoba,
        COUNT(estimated_woba_using_speedangle) AS n_xwoba,
        SUM(CASE WHEN launch_speed >= 98 AND launch_angle BETWEEN 26 AND 30 THEN 1 ELSE 0 END) AS barrels,
        SUM(CASE WHEN events IN (
            'single','double','triple','home_run','field_out',
            'grounded_into_double_play','double_play','fielders_choice',
            'fielders_choice_out','force_out','field_error','sac_fly'
        ) THEN 1 ELSE 0 END) AS bip
    FROM statcast_pitches
    WHERE game_type = 'R'
      AND game_year BETWEEN 2021 AND {current_year}
    GROUP BY batter, game_date
    ORDER BY batter, game_date
    """
    daily = query_df(sql)
    print(f"  {len(daily):,} batter-day rows")

    # Convert to proper types
    daily["game_date"] = pd.to_datetime(daily["game_date"])
    for col in ["pitches", "pa", "ks", "bbs", "swings", "whiffs",
                 "chase_opps", "chases", "barrels", "bip", "n_xwoba"]:
        daily[col] = pd.to_numeric(daily[col], errors="coerce").fillna(0).astype(int)
    daily["sum_xwoba"] = pd.to_numeric(daily["sum_xwoba"], errors="coerce").fillna(0)

    print("Computing rolling 14-day batter stats...")
    # Set index for rolling
    daily = daily.sort_values(["batter", "game_date"]).reset_index(drop=True)

    results = []
    for batter_id, group in daily.groupby("batter"):
        g = group.set_index("game_date").sort_index()
        # Rolling 14-day sum (excluding current day — shift by 1)
        shifted = g.shift(1)  # exclude current day's game
        for col in ["pitches", "pa", "ks", "bbs", "swings", "whiffs",
                     "chase_opps", "chases", "barrels", "bip", "n_xwoba", "sum_xwoba"]:
            g[f"r14_{col}"] = shifted[col].rolling("14D", min_periods=1).sum()

        for _, row in g.iterrows():
            pitches = row.get("r14_pitches", 0)
            if pitches < MIN_PITCHES:
                results.append({
                    "batter": batter_id,
                    "game_date": row.name,
                })
                continue

            pa = row["r14_pa"]
            results.append({
                "batter": batter_id,
                "game_date": row.name,
                "bat_r14_k_pct": row["r14_ks"] / pa if pa > 0 else np.nan,
                "bat_r14_bb_pct": row["r14_bbs"] / pa if pa > 0 else np.nan,
                "bat_r14_xwoba": row["r14_sum_xwoba"] / row["r14_n_xwoba"] if row["r14_n_xwoba"] > 0 else np.nan,
                "bat_r14_barrel_rate": row["r14_barrels"] / row["r14_bip"] if row["r14_bip"] > 0 else np.nan,
                "bat_r14_whiff_rate": row["r14_whiffs"] / row["r14_swings"] if row["r14_swings"] > 0 else np.nan,
                "bat_r14_chase_rate": row["r14_chases"] / row["r14_chase_opps"] if row["r14_chase_opps"] > 0 else np.nan,
            })

    result_df = pd.DataFrame(results)
    result_df["game_date"] = pd.to_datetime(result_df["game_date"])
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    result_df.to_parquet(BATTER_OUTPUT, index=False)
    print(f"  Batter recent form saved: {len(result_df):,} rows")
    non_null = result_df["bat_r14_k_pct"].notna().sum()
    print(f"  {non_null:,} rows with actual data ({non_null/len(result_df)*100:.1f}%)")


def build_pitcher_recent_form():
    from datetime import date
    current_year = date.today().year
    print("\nQuerying daily pitcher aggregates...")
    sql = f"""
    SELECT
        pitcher,
        game_date,
        COUNT(*) AS pitches,
        COUNT(*) FILTER (WHERE events IS NOT NULL AND events != '') AS pa,
        SUM(CASE WHEN events IN ('strikeout','strikeout_double_play') THEN 1 ELSE 0 END) AS ks,
        SUM(CASE WHEN events = 'walk' THEN 1 ELSE 0 END) AS bbs,
        SUM(CASE WHEN description IN (
            'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt',
            'foul','foul_bunt','hit_into_play'
        ) THEN 1 ELSE 0 END) AS swings,
        SUM(CASE WHEN description IN (
            'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt'
        ) THEN 1 ELSE 0 END) AS whiffs,
        SUM(CASE WHEN zone IN (11,12,13,14) THEN 1 ELSE 0 END) AS chase_opps,
        SUM(CASE WHEN zone IN (11,12,13,14) AND description IN (
            'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt',
            'foul','foul_bunt','hit_into_play'
        ) THEN 1 ELSE 0 END) AS chases,
        SUM(estimated_woba_using_speedangle) AS sum_xwoba,
        COUNT(estimated_woba_using_speedangle) AS n_xwoba
    FROM statcast_pitches
    WHERE game_type = 'R'
      AND game_year BETWEEN 2021 AND {current_year}
    GROUP BY pitcher, game_date
    ORDER BY pitcher, game_date
    """
    daily = query_df(sql)
    print(f"  {len(daily):,} pitcher-day rows")

    daily["game_date"] = pd.to_datetime(daily["game_date"])
    for col in ["pitches", "pa", "ks", "bbs", "swings", "whiffs",
                 "chase_opps", "chases", "n_xwoba"]:
        daily[col] = pd.to_numeric(daily[col], errors="coerce").fillna(0).astype(int)
    daily["sum_xwoba"] = pd.to_numeric(daily["sum_xwoba"], errors="coerce").fillna(0)

    print("Computing rolling 14-day pitcher stats...")
    daily = daily.sort_values(["pitcher", "game_date"]).reset_index(drop=True)

    results = []
    for pitcher_id, group in daily.groupby("pitcher"):
        g = group.set_index("game_date").sort_index()
        shifted = g.shift(1)
        for col in ["pitches", "pa", "ks", "bbs", "swings", "whiffs",
                     "chase_opps", "chases", "n_xwoba", "sum_xwoba"]:
            g[f"r14_{col}"] = shifted[col].rolling("14D", min_periods=1).sum()

        for _, row in g.iterrows():
            pitches = row.get("r14_pitches", 0)
            if pitches < MIN_PITCHES:
                results.append({
                    "pitcher": pitcher_id,
                    "game_date": row.name,
                })
                continue

            pa = row["r14_pa"]
            results.append({
                "pitcher": pitcher_id,
                "game_date": row.name,
                "p_r14_k_pct": row["r14_ks"] / pa if pa > 0 else np.nan,
                "p_r14_bb_pct": row["r14_bbs"] / pa if pa > 0 else np.nan,
                "p_r14_xwoba": row["r14_sum_xwoba"] / row["r14_n_xwoba"] if row["r14_n_xwoba"] > 0 else np.nan,
                "p_r14_whiff_rate": row["r14_whiffs"] / row["r14_swings"] if row["r14_swings"] > 0 else np.nan,
                "p_r14_chase_rate": row["r14_chases"] / row["r14_chase_opps"] if row["r14_chase_opps"] > 0 else np.nan,
            })

    result_df = pd.DataFrame(results)
    result_df["game_date"] = pd.to_datetime(result_df["game_date"])
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    result_df.to_parquet(PITCHER_OUTPUT, index=False)
    print(f"  Pitcher recent form saved: {len(result_df):,} rows")
    non_null = result_df["p_r14_k_pct"].notna().sum()
    print(f"  {non_null:,} rows with actual data ({non_null/len(result_df)*100:.1f}%)")


if __name__ == "__main__":
    build_batter_recent_form()
    build_pitcher_recent_form()
