#!/usr/bin/env python3
"""
Build pitcher workload/fatigue features from statcast_pitches.
Server-side aggregation for per-game stats, then rolling features in Python.

Output: data/pitcher_workload.parquet
"""

import os
import sys
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import query_df

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "pitcher_workload.parquet")


def build_pitcher_workload():
    from datetime import date
    current_year = date.today().year

    print(f"Querying per-game pitcher stats (2021-{current_year}, server-side)...")

    sql = f"""
    SELECT
        pitcher,
        game_pk,
        MIN(game_date) AS game_date,
        MIN(game_year) AS game_year,
        COUNT(*) AS pitch_count,
        MIN(inning) AS min_inning,
        MAX(inning) AS max_inning,
        MAX(n_thruorder_pitcher) AS max_thruorder,
        COUNT(DISTINCT at_bat_number) AS batters_faced,
        SUM(CASE WHEN events IS NOT NULL AND events NOT IN (
            'walk','hit_by_pitch','single','double','triple',
            'home_run','field_error','catcher_interf','intent_walk'
        ) AND events != '' THEN 1 ELSE 0 END) AS outs_recorded
    FROM statcast_pitches
    WHERE game_type = 'R'
      AND game_year BETWEEN 2021 AND {current_year}
      AND pitcher IS NOT NULL
    GROUP BY pitcher, game_pk
    ORDER BY pitcher, MIN(game_date)
    """
    game_stats = query_df(sql)
    print(f"  {len(game_stats):,} pitcher-game appearances")

    if game_stats.empty:
        print("No data found.")
        return

    game_stats["game_date"] = pd.to_datetime(game_stats["game_date"])
    game_stats["is_starter"] = (game_stats["min_inning"] == 1).astype(int)
    game_stats["ip"] = game_stats["outs_recorded"] / 3.0

    # Sort for rolling calculations
    game_stats = game_stats.sort_values(["pitcher", "game_date", "game_pk"]).reset_index(drop=True)

    print("Computing rolling workload features...")
    records = []

    for pitcher_id, grp in game_stats.groupby("pitcher"):
        grp = grp.sort_values("game_date").reset_index(drop=True)

        for i in range(len(grp)):
            row = grp.iloc[i]
            current_date = row["game_date"]

            # Days rest
            days_rest = (current_date - grp.iloc[i - 1]["game_date"]).days if i > 0 else np.nan

            # Rolling windows (backward, excluding current game)
            prev = grp.iloc[:i]
            if len(prev) > 0:
                for window_days, suffix in [(7, "7d"), (14, "14d"), (30, "30d")]:
                    cutoff = current_date - pd.Timedelta(days=window_days)
                    w = prev[prev["game_date"] >= cutoff]
                    records_entry = {
                        f"pitches_last_{suffix}": w["pitch_count"].sum(),
                        f"apps_last_{suffix}": len(w),
                    }
                    if suffix == "30d":
                        records_entry["ip_last_30d"] = w["ip"].sum()
            else:
                records_entry = {}
                for suffix in ["7d", "14d", "30d"]:
                    records_entry[f"pitches_last_{suffix}"] = 0
                    records_entry[f"apps_last_{suffix}"] = 0
                records_entry["ip_last_30d"] = 0

            # Season cumulative
            season_prev = prev[prev["game_year"] == row["game_year"]] if len(prev) > 0 else pd.DataFrame()

            records.append({
                "pitcher": pitcher_id,
                "game_pk": row["game_pk"],
                "game_date": row["game_date"],
                "season": row["game_year"],
                "pitch_count": row["pitch_count"],
                "ip": row["ip"],
                "batters_faced": row["batters_faced"],
                "is_starter": row["is_starter"],
                "max_thruorder": row["max_thruorder"],
                "days_rest": days_rest,
                "season_pitches_before": season_prev["pitch_count"].sum() if len(season_prev) > 0 else 0,
                "season_ip_before": season_prev["ip"].sum() if len(season_prev) > 0 else 0,
                "season_apps_before": len(season_prev),
                **records_entry,
            })

    result = pd.DataFrame(records)
    result = result.sort_values(["season", "pitcher", "game_date"]).reset_index(drop=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    result.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nPitcher workload saved to {OUTPUT_PATH}")
    print(f"  {len(result):,} appearances ({result['pitcher'].nunique():,} pitchers)")


if __name__ == "__main__":
    build_pitcher_workload()
