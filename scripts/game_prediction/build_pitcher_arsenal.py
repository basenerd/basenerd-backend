#!/usr/bin/env python3
"""
Build pitcher arsenal profiles from statcast_pitches + statcast_pitches_live.

Server-side aggregation to avoid pulling millions of rows.

Output: data/pitcher_arsenal.parquet
"""

import os
import sys
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import query_df

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "pitcher_arsenal.parquet")


def build_pitcher_arsenal():
    from datetime import date
    current_year = date.today().year

    print(f"Querying pitcher arsenal aggregates (2021-{current_year}, server-side)...")

    sql = f"""
    SELECT
        sp.pitcher,
        sp.game_year,
        sp.stand,
        sp.pitch_type,
        COUNT(*) AS n,
        AVG(CASE WHEN sp.release_speed BETWEEN 40 AND 110 THEN sp.release_speed END) AS avg_velo,
        AVG(CASE WHEN sp.release_spin_rate BETWEEN 0 AND 5000 THEN sp.release_spin_rate END) AS avg_spin,
        AVG(CASE WHEN sp.pfx_x BETWEEN -3 AND 3 THEN sp.pfx_x * 12 END) AS avg_hb,
        AVG(CASE WHEN sp.pfx_z BETWEEN -3 AND 3 THEN sp.pfx_z * 12 END) AS avg_ivb,
        AVG(CASE WHEN sp.release_extension BETWEEN 0 AND 10 THEN sp.release_extension END) AS avg_extension,
        AVG(CASE WHEN sp.release_pos_x BETWEEN -5 AND 5 THEN sp.release_pos_x END) AS avg_rel_x,
        AVG(CASE WHEN sp.release_pos_z BETWEEN 0 AND 10 THEN sp.release_pos_z END) AS avg_rel_z,
        STDDEV(CASE WHEN sp.release_pos_x BETWEEN -5 AND 5 THEN sp.release_pos_x END) AS std_rel_x,
        STDDEV(CASE WHEN sp.release_pos_z BETWEEN 0 AND 10 THEN sp.release_pos_z END) AS std_rel_z,
        AVG(CASE WHEN spl.stuff_plus BETWEEN 0 AND 300 THEN spl.stuff_plus END) AS avg_stuff_plus,
        AVG(CASE WHEN spl.control_plus BETWEEN 0 AND 300 THEN spl.control_plus END) AS avg_control_plus,
        SUM(CASE WHEN sp.description IN (
            'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt',
            'foul','foul_bunt','hit_into_play'
        ) THEN 1 ELSE 0 END) AS swings,
        SUM(CASE WHEN sp.description IN (
            'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt'
        ) THEN 1 ELSE 0 END) AS whiffs,
        SUM(CASE WHEN sp.zone BETWEEN 1 AND 9 THEN 1 ELSE 0 END) AS in_zone,
        SUM(CASE WHEN sp.zone IN (11,12,13,14) THEN 1 ELSE 0 END) AS chase_opps,
        SUM(CASE WHEN sp.zone IN (11,12,13,14)
            AND sp.description IN (
                'swinging_strike','swinging_strike_blocked','foul_tip','missed_bunt',
                'foul','foul_bunt','hit_into_play'
            ) THEN 1 ELSE 0 END) AS chases,
        SUM(CASE WHEN sp.description = 'called_strike' THEN 1 ELSE 0 END) AS called_strikes,
        AVG(CASE WHEN sp.estimated_woba_using_speedangle BETWEEN -1 AND 5 THEN sp.estimated_woba_using_speedangle END) AS xwoba
    FROM statcast_pitches sp
    LEFT JOIN statcast_pitches_live spl
        ON sp.game_pk = spl.game_pk
        AND sp.at_bat_number = spl.at_bat_number
        AND sp.pitch_number = spl.pitch_number
    WHERE sp.game_type = 'R'
      AND sp.game_year BETWEEN 2021 AND {current_year}
      AND sp.pitch_type IS NOT NULL
      AND sp.pitch_type != ''
    GROUP BY sp.pitcher, sp.game_year, sp.stand, sp.pitch_type
    """
    df = query_df(sql)
    print(f"  {len(df):,} pitcher/year/hand/type groups")

    if df.empty:
        print("No data found.")
        return

    # Compute total pitches per pitcher/year/hand for usage rates
    totals = df.groupby(["pitcher", "game_year", "stand"])["n"].sum().reset_index(name="total_pitches")
    df = df.merge(totals, on=["pitcher", "game_year", "stand"])

    df["usage"] = df["n"] / df["total_pitches"]
    df["whiff_rate"] = np.where(df["swings"] > 0, df["whiffs"] / df["swings"], np.nan)
    df["zone_rate"] = df["in_zone"] / df["n"]
    df["chase_rate"] = np.where(df["chase_opps"] > 0, df["chases"] / df["chase_opps"], np.nan)
    df["csw_rate"] = (df["called_strikes"] + df["whiffs"]) / df["n"]

    # Also build "ALL" handedness aggregate
    all_sql = sql.replace("sp.stand,", "").replace("sp.stand, ", "").replace(
        "GROUP BY sp.pitcher, sp.game_year, sp.stand, sp.pitch_type",
        "GROUP BY sp.pitcher, sp.game_year, sp.pitch_type"
    )
    all_df = query_df(all_sql)
    all_df["stand"] = "ALL"
    print(f"  {len(all_df):,} pitcher/year/type groups (ALL hands)")

    totals_all = all_df.groupby(["pitcher", "game_year"])["n"].sum().reset_index(name="total_pitches")
    all_df = all_df.merge(totals_all, on=["pitcher", "game_year"])

    all_df["usage"] = all_df["n"] / all_df["total_pitches"]
    all_df["whiff_rate"] = np.where(all_df["swings"] > 0, all_df["whiffs"] / all_df["swings"], np.nan)
    all_df["zone_rate"] = all_df["in_zone"] / all_df["n"]
    all_df["chase_rate"] = np.where(all_df["chase_opps"] > 0, all_df["chases"] / all_df["chase_opps"], np.nan)
    all_df["csw_rate"] = (all_df["called_strikes"] + all_df["whiffs"]) / all_df["n"]

    keep_cols = [
        "pitcher", "game_year", "stand", "pitch_type", "n", "usage",
        "avg_velo", "avg_spin", "avg_hb", "avg_ivb",
        "avg_extension", "avg_rel_x", "avg_rel_z", "std_rel_x", "std_rel_z",
        "avg_stuff_plus", "avg_control_plus",
        "whiff_rate", "zone_rate", "chase_rate", "csw_rate", "xwoba",
    ]

    final = pd.concat([df[keep_cols], all_df[keep_cols]], ignore_index=True)
    final = final.rename(columns={"game_year": "season"})
    final = final.sort_values(["season", "pitcher", "stand", "pitch_type"]).reset_index(drop=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    final.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nPitcher arsenal saved to {OUTPUT_PATH}")
    print(f"  {len(final):,} rows ({final['pitcher'].nunique():,} pitchers)")


if __name__ == "__main__":
    build_pitcher_arsenal()
