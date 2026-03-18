#!/usr/bin/env python3
"""
Build umpire tendencies from statcast_pitches + game boxscores.

For each umpire/season, computes:
- Zone size (called strike rate on borderline pitches)
- K rate impact (above/below league avg)
- BB rate impact
- Run environment impact
- Zone bias (high/low/inside/outside tendencies)

Requires game_outcomes.parquet (for umpire-game mapping).

Output: data/umpire_metrics.parquet
"""

import os
import sys
import json
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import query_df

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "umpire_metrics.parquet")
GAME_OUTCOMES_PATH = os.path.join(OUTPUT_DIR, "game_outcomes.parquet")

ZONE_X_HALF = 0.83  # half plate width in feet
SHADOW_BUFFER = 0.42  # tighter shadow for umpire analysis (~5 inches)


def build_umpire_metrics():
    # Check if game_outcomes exists (need umpire-game mapping)
    if not os.path.exists(GAME_OUTCOMES_PATH):
        print(f"game_outcomes.parquet not found at {GAME_OUTCOMES_PATH}")
        print("Run build_game_outcomes.py first.")
        return

    game_outcomes = pd.read_parquet(GAME_OUTCOMES_PATH)
    umpire_games = game_outcomes[["game_pk", "hp_umpire_id", "season",
                                   "home_score", "away_score", "total_runs"]].copy()
    umpire_games = umpire_games[umpire_games["hp_umpire_id"].notna()]
    umpire_games["hp_umpire_id"] = umpire_games["hp_umpire_id"].astype(int)

    print(f"  {len(umpire_games):,} games with umpire data")

    print("Querying called pitch data for umpire analysis...")

    sql = """
    SELECT
        game_pk,
        game_year,
        plate_x,
        plate_z,
        sz_top,
        sz_bot,
        stand,
        description,
        events
    FROM statcast_pitches
    WHERE game_type IN ('R', 'S')
      AND game_year BETWEEN 2021 AND 2026
      AND description IN ('called_strike', 'ball', 'blocked_ball')
      AND plate_x IS NOT NULL
      AND plate_z IS NOT NULL
      AND sz_top IS NOT NULL
      AND sz_bot IS NOT NULL
    """
    pitches = query_df(sql)
    print(f"  {len(pitches):,} called pitches loaded")

    if pitches.empty:
        print("No data found. Exiting.")
        return

    # Merge umpire IDs
    pitches = pitches.merge(umpire_games[["game_pk", "hp_umpire_id"]], on="game_pk", how="inner")
    print(f"  {len(pitches):,} pitches matched to umpires")

    # Zone classification
    pitches["in_zone"] = (
        (pitches["plate_x"].abs() <= ZONE_X_HALF) &
        (pitches["plate_z"] >= pitches["sz_bot"]) &
        (pitches["plate_z"] <= pitches["sz_top"])
    ).astype(int)

    # Shadow zones (directional)
    pitches["shadow_high"] = (
        (pitches["plate_z"] > pitches["sz_top"]) &
        (pitches["plate_z"] <= pitches["sz_top"] + SHADOW_BUFFER) &
        (pitches["plate_x"].abs() <= ZONE_X_HALF + SHADOW_BUFFER)
    ).astype(int)

    pitches["shadow_low"] = (
        (pitches["plate_z"] < pitches["sz_bot"]) &
        (pitches["plate_z"] >= pitches["sz_bot"] - SHADOW_BUFFER) &
        (pitches["plate_x"].abs() <= ZONE_X_HALF + SHADOW_BUFFER)
    ).astype(int)

    pitches["shadow_inside"] = (
        (pitches["plate_z"] >= pitches["sz_bot"]) &
        (pitches["plate_z"] <= pitches["sz_top"])
    ).astype(int) & (
        ((pitches["stand"] == "R") & (pitches["plate_x"] < -ZONE_X_HALF) &
         (pitches["plate_x"] >= -ZONE_X_HALF - SHADOW_BUFFER)) |
        ((pitches["stand"] == "L") & (pitches["plate_x"] > ZONE_X_HALF) &
         (pitches["plate_x"] <= ZONE_X_HALF + SHADOW_BUFFER))
    ).astype(int)

    pitches["shadow_outside"] = (
        (pitches["plate_z"] >= pitches["sz_bot"]) &
        (pitches["plate_z"] <= pitches["sz_top"])
    ).astype(int) & (
        ((pitches["stand"] == "R") & (pitches["plate_x"] > ZONE_X_HALF) &
         (pitches["plate_x"] <= ZONE_X_HALF + SHADOW_BUFFER)) |
        ((pitches["stand"] == "L") & (pitches["plate_x"] < -ZONE_X_HALF) &
         (pitches["plate_x"] >= -ZONE_X_HALF - SHADOW_BUFFER))
    ).astype(int)

    pitches["is_called_strike"] = (pitches["description"] == "called_strike").astype(int)

    # --- Aggregate per umpire per season ---
    ump_agg = pitches.groupby(["hp_umpire_id", "game_year"]).agg(
        total_called=("hp_umpire_id", "size"),
        total_strikes=("is_called_strike", "sum"),
        ooz_total=("in_zone", lambda x: (x == 0).sum()),
        ooz_strikes=("is_called_strike",
                      lambda x: x[pitches.loc[x.index, "in_zone"] == 0].sum()),
        iz_total=("in_zone", lambda x: (x == 1).sum()),
        iz_balls=("is_called_strike",
                   lambda x: (1 - x)[pitches.loc[x.index, "in_zone"] == 1].sum()),
        shadow_high_total=("shadow_high", "sum"),
        shadow_low_total=("shadow_low", "sum"),
        games=("game_pk", "nunique"),
    ).reset_index()

    # Shadow zone strike rates per direction
    for direction in ["high", "low"]:
        shadow_dir = pitches[pitches[f"shadow_{direction}"] == 1]
        dir_agg = shadow_dir.groupby(["hp_umpire_id", "game_year"]).agg(
            **{f"shadow_{direction}_strikes": ("is_called_strike", "sum"),
               f"shadow_{direction}_n": ("is_called_strike", "size")}
        ).reset_index()
        ump_agg = ump_agg.merge(dir_agg, on=["hp_umpire_id", "game_year"], how="left")

    # Rates
    ump_agg["overall_cs_rate"] = ump_agg["total_strikes"] / ump_agg["total_called"]
    ump_agg["ooz_cs_rate"] = np.where(ump_agg["ooz_total"] > 0,
                                        ump_agg["ooz_strikes"] / ump_agg["ooz_total"], np.nan)
    ump_agg["iz_ball_rate"] = np.where(ump_agg["iz_total"] > 0,
                                        ump_agg["iz_balls"] / ump_agg["iz_total"], np.nan)
    ump_agg["shadow_high_cs_rate"] = np.where(
        ump_agg["shadow_high_n"] > 0,
        ump_agg["shadow_high_strikes"] / ump_agg["shadow_high_n"], np.nan)
    ump_agg["shadow_low_cs_rate"] = np.where(
        ump_agg["shadow_low_n"] > 0,
        ump_agg["shadow_low_strikes"] / ump_agg["shadow_low_n"], np.nan)

    # League averages
    lg_avg = pitches.groupby("game_year").agg(
        lg_cs_rate=("is_called_strike", "mean"),
    ).reset_index()
    lg_ooz = pitches[pitches["in_zone"] == 0].groupby("game_year").agg(
        lg_ooz_cs_rate=("is_called_strike", "mean"),
    ).reset_index()

    ump_agg = ump_agg.merge(lg_avg, on="game_year", how="left")
    ump_agg = ump_agg.merge(lg_ooz, on="game_year", how="left")

    # Zone size relative to league (>1 = bigger zone)
    ump_agg["zone_size_factor"] = np.where(
        ump_agg["lg_ooz_cs_rate"] > 0,
        ump_agg["ooz_cs_rate"] / ump_agg["lg_ooz_cs_rate"],
        np.nan
    )

    # Merge run environment from game outcomes
    ump_runs = umpire_games.groupby(["hp_umpire_id", "season"]).agg(
        ump_games=("game_pk", "nunique"),
        avg_total_runs=("total_runs", "mean"),
    ).reset_index()

    lg_runs = umpire_games.groupby("season").agg(
        lg_avg_runs=("total_runs", "mean"),
    ).reset_index()

    ump_runs = ump_runs.merge(lg_runs, on="season", how="left")
    ump_runs["run_env_factor"] = ump_runs["avg_total_runs"] / ump_runs["lg_avg_runs"]

    ump_agg = ump_agg.merge(
        ump_runs[["hp_umpire_id", "season", "avg_total_runs", "run_env_factor"]],
        left_on=["hp_umpire_id", "game_year"],
        right_on=["hp_umpire_id", "season"],
        how="left"
    )

    # Select output
    result = ump_agg[[
        "hp_umpire_id", "game_year", "games", "total_called",
        "overall_cs_rate", "ooz_cs_rate", "iz_ball_rate",
        "zone_size_factor",
        "shadow_high_cs_rate", "shadow_low_cs_rate",
        "avg_total_runs", "run_env_factor",
    ]].copy()
    result = result.rename(columns={"game_year": "season"})
    result = result.sort_values(["season", "hp_umpire_id"]).reset_index(drop=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    result.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nUmpire metrics saved to {OUTPUT_PATH}")
    print(f"  {len(result):,} umpire-seasons ({result['hp_umpire_id'].nunique():,} umpires)")
    print(f"\nTightest zones (2024, lowest OOZ CS rate):")
    tight = result[result["season"] == 2024].sort_values("ooz_cs_rate").head(5)
    print(tight[["hp_umpire_id", "games", "ooz_cs_rate", "zone_size_factor",
                  "run_env_factor"]].to_string(index=False))


if __name__ == "__main__":
    build_umpire_metrics()
