#!/usr/bin/env python3
"""
Build defensive metrics from statcast_pitches.
Server-side aggregation.

Uses hit_location to assign defensive plays to fielders.
Computes out rate, error rate, and outs above average per position.

Output: data/defensive_metrics.parquet
"""

import os
import sys
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import query_df

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "defensive_metrics.parquet")

POSITION_NAMES = {2: "C", 3: "1B", 4: "2B", 5: "3B", 6: "SS", 7: "LF", 8: "CF", 9: "RF"}
REGRESS_N = 200


def build_defensive_metrics():
    from datetime import date
    current_year = date.today().year
    print("Querying defensive metrics (server-side)...")

    # Build one query per position to get the fielder at that position
    results = []
    for pos_num, pos_name in POSITION_NAMES.items():
        print(f"  Position {pos_num} ({pos_name})...")

        sql = f"""
        SELECT
            fielder_{pos_num} AS player_id,
            game_year,
            COUNT(*) AS plays,
            COUNT(DISTINCT game_pk) AS games,
            SUM(CASE WHEN events IN (
                'field_out','grounded_into_double_play','double_play',
                'fielders_choice','fielders_choice_out','force_out',
                'triple_play','sac_fly','sac_bunt'
            ) THEN 1 ELSE 0 END) AS outs_made,
            SUM(CASE WHEN events IN ('single','double','triple') THEN 1 ELSE 0 END) AS hits_allowed,
            SUM(CASE WHEN events = 'field_error' THEN 1 ELSE 0 END) AS errors,
            SUM(CASE WHEN launch_speed >= 95 THEN 1 ELSE 0 END) AS hard_hit_plays,
            SUM(CASE WHEN launch_speed >= 95 AND events IN (
                'field_out','grounded_into_double_play','double_play',
                'fielders_choice','fielders_choice_out','force_out',
                'triple_play','sac_fly','sac_bunt'
            ) THEN 1 ELSE 0 END) AS hard_hit_outs
        FROM statcast_pitches
        WHERE game_type = 'R'
          AND game_year BETWEEN 2021 AND {current_year}
          AND hit_location = {pos_num}
          AND events IS NOT NULL AND events != ''
          AND events NOT IN ('home_run')
          AND fielder_{pos_num} IS NOT NULL
        GROUP BY fielder_{pos_num}, game_year
        """
        pos_df = query_df(sql)
        pos_df["position"] = pos_num
        pos_df["position_name"] = pos_name
        results.append(pos_df)

    if not results:
        print("No data found.")
        return

    result = pd.concat(results, ignore_index=True)

    # Rates
    result["out_rate"] = np.where(result["plays"] > 0, result["outs_made"] / result["plays"], np.nan)
    result["error_rate"] = np.where(result["plays"] > 0, result["errors"] / result["plays"], np.nan)
    result["babip_against"] = np.where(result["plays"] > 0, result["hits_allowed"] / result["plays"], np.nan)
    result["hard_hit_out_rate"] = np.where(result["hard_hit_plays"] > 0,
                                            result["hard_hit_outs"] / result["hard_hit_plays"], np.nan)

    # League averages
    lg = result.groupby(["game_year", "position"]).agg(
        lg_plays=("plays", "sum"),
        lg_outs=("outs_made", "sum"),
        lg_errors=("errors", "sum"),
    ).reset_index()
    lg["lg_out_rate"] = lg["lg_outs"] / lg["lg_plays"]
    lg["lg_error_rate"] = lg["lg_errors"] / lg["lg_plays"]

    result = result.merge(lg[["game_year", "position", "lg_out_rate", "lg_error_rate"]],
                          on=["game_year", "position"], how="left")

    result["outs_above_avg"] = result["outs_made"] - result["plays"] * result["lg_out_rate"]
    result["outs_above_avg_per_game"] = np.where(result["games"] > 0,
                                                   result["outs_above_avg"] / result["games"], np.nan)

    weight = result["plays"] / (result["plays"] + REGRESS_N)
    result["regressed_out_rate"] = weight * result["out_rate"] + (1 - weight) * result["lg_out_rate"]

    result = result.rename(columns={"game_year": "season"})
    result = result.sort_values(["season", "position", "outs_above_avg"],
                                 ascending=[True, True, False]).reset_index(drop=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    result.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nDefensive metrics saved to {OUTPUT_PATH}")
    print(f"  {len(result):,} player-position-seasons")


if __name__ == "__main__":
    build_defensive_metrics()
