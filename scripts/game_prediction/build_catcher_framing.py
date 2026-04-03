#!/usr/bin/env python3
"""
Build catcher framing metrics from statcast_pitches.
Server-side aggregation.

Output: data/catcher_framing.parquet
"""

import os
import sys
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import query_df

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "catcher_framing.parquet")

ZONE_X_HALF = 0.83
SHADOW_BUFFER = 0.5
RUNS_PER_STRIKE = 0.125


def build_catcher_framing():
    from datetime import date
    current_year = date.today().year
    print("Querying catcher framing aggregates (server-side)...")

    sql = f"""
    SELECT
        fielder_2 AS catcher,
        game_year,
        COUNT(DISTINCT game_pk) AS games,
        COUNT(*) AS total_called,
        SUM(CASE WHEN ABS(plate_x) <= {ZONE_X_HALF}
                  AND plate_z >= sz_bot AND plate_z <= sz_top
             THEN 1 ELSE 0 END) AS iz_total,
        SUM(CASE WHEN NOT (ABS(plate_x) <= {ZONE_X_HALF}
                  AND plate_z >= sz_bot AND plate_z <= sz_top)
             THEN 1 ELSE 0 END) AS ooz_total,
        SUM(CASE WHEN description = 'called_strike'
                  AND NOT (ABS(plate_x) <= {ZONE_X_HALF}
                           AND plate_z >= sz_bot AND plate_z <= sz_top)
             THEN 1 ELSE 0 END) AS ooz_strikes,
        SUM(CASE WHEN description != 'called_strike'
                  AND ABS(plate_x) <= {ZONE_X_HALF}
                  AND plate_z >= sz_bot AND plate_z <= sz_top
             THEN 1 ELSE 0 END) AS iz_balls,
        SUM(CASE WHEN (
                (ABS(plate_x) > {ZONE_X_HALF} AND ABS(plate_x) <= {ZONE_X_HALF + SHADOW_BUFFER})
                OR (plate_z < sz_bot AND plate_z >= sz_bot - {SHADOW_BUFFER})
                OR (plate_z > sz_top AND plate_z <= sz_top + {SHADOW_BUFFER})
            ) THEN 1 ELSE 0 END) AS shadow_pitches,
        SUM(CASE WHEN description = 'called_strike' AND (
                (ABS(plate_x) > {ZONE_X_HALF} AND ABS(plate_x) <= {ZONE_X_HALF + SHADOW_BUFFER})
                OR (plate_z < sz_bot AND plate_z >= sz_bot - {SHADOW_BUFFER})
                OR (plate_z > sz_top AND plate_z <= sz_top + {SHADOW_BUFFER})
            ) THEN 1 ELSE 0 END) AS shadow_strikes
    FROM statcast_pitches
    WHERE game_type = 'R'
      AND game_year BETWEEN 2021 AND {current_year}
      AND description IN ('called_strike', 'ball', 'blocked_ball')
      AND fielder_2 IS NOT NULL
      AND plate_x IS NOT NULL AND plate_z IS NOT NULL
      AND sz_top IS NOT NULL AND sz_bot IS NOT NULL
    GROUP BY fielder_2, game_year
    """
    df = query_df(sql)
    print(f"  {len(df):,} catcher-seasons")

    if df.empty:
        print("No data found.")
        return

    # League averages
    lg_sql = f"""
    SELECT
        game_year,
        SUM(CASE WHEN description = 'called_strike'
                  AND NOT (ABS(plate_x) <= {ZONE_X_HALF}
                           AND plate_z >= sz_bot AND plate_z <= sz_top)
             THEN 1 ELSE 0 END)::float /
        NULLIF(SUM(CASE WHEN NOT (ABS(plate_x) <= {ZONE_X_HALF}
                  AND plate_z >= sz_bot AND plate_z <= sz_top)
             THEN 1 ELSE 0 END), 0) AS lg_ooz_cs_rate,
        SUM(CASE WHEN description != 'called_strike'
                  AND ABS(plate_x) <= {ZONE_X_HALF}
                  AND plate_z >= sz_bot AND plate_z <= sz_top
             THEN 1 ELSE 0 END)::float /
        NULLIF(SUM(CASE WHEN ABS(plate_x) <= {ZONE_X_HALF}
                  AND plate_z >= sz_bot AND plate_z <= sz_top
             THEN 1 ELSE 0 END), 0) AS lg_iz_ball_rate
    FROM statcast_pitches
    WHERE game_type = 'R'
      AND game_year BETWEEN 2021 AND {current_year}
      AND description IN ('called_strike', 'ball', 'blocked_ball')
      AND plate_x IS NOT NULL AND plate_z IS NOT NULL
      AND sz_top IS NOT NULL AND sz_bot IS NOT NULL
    GROUP BY game_year
    """
    lg = query_df(lg_sql)
    df = df.merge(lg, on="game_year", how="left")

    df["out_zone_cs_rate"] = np.where(df["ooz_total"] > 0, df["ooz_strikes"] / df["ooz_total"], np.nan)
    df["in_zone_ball_rate"] = np.where(df["iz_total"] > 0, df["iz_balls"] / df["iz_total"], np.nan)
    df["shadow_cs_rate"] = np.where(df["shadow_pitches"] > 0,
                                     df["shadow_strikes"] / df["shadow_pitches"], np.nan)

    df["extra_strikes_ooz"] = df["ooz_strikes"] - df["ooz_total"] * df["lg_ooz_cs_rate"]
    df["lost_strikes_iz"] = df["iz_balls"] - df["iz_total"] * df["lg_iz_ball_rate"]
    df["net_framing_strikes"] = df["extra_strikes_ooz"] - df["lost_strikes_iz"]
    df["framing_strikes_per_game"] = np.where(df["games"] > 0,
                                               df["net_framing_strikes"] / df["games"], np.nan)
    df["framing_runs"] = df["net_framing_strikes"] * RUNS_PER_STRIKE
    df["framing_runs_per_game"] = df["framing_strikes_per_game"] * RUNS_PER_STRIKE

    result = df[[
        "catcher", "game_year", "games", "total_called",
        "out_zone_cs_rate", "in_zone_ball_rate",
        "shadow_pitches", "shadow_cs_rate",
        "net_framing_strikes", "framing_strikes_per_game",
        "framing_runs", "framing_runs_per_game",
    ]].copy()
    result = result.rename(columns={"game_year": "season"})
    result = result.sort_values(["season", "framing_runs"], ascending=[True, False]).reset_index(drop=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    result.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nCatcher framing saved to {OUTPUT_PATH}")
    print(f"  {len(result):,} catcher-seasons ({result['catcher'].nunique():,} catchers)")
    latest = result["season"].max()
    print(f"\nTop framers ({latest}):")
    top = result[result["season"] == latest].head(10)
    print(top[["catcher", "games", "framing_runs", "framing_runs_per_game",
               "shadow_cs_rate"]].to_string(index=False))


if __name__ == "__main__":
    build_catcher_framing()
