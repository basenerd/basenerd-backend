#!/usr/bin/env python3
"""
Build park factors from statcast_pitches.

Computes per-venue, per-season factors for:
- Runs, HR, H, 2B, 3B, BB, K
- Split by batter handedness (L/R) and combined (ALL)

Method: Compare event rates at each venue vs league average,
regressed toward 1.0 based on sample size.

Output: data/park_factors.parquet
"""

import os
import sys
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import query_df

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "park_factors.parquet")

MIN_PA = 200
REGRESS_N = 2000


def build_park_factors():
    from datetime import date
    current_year = date.today().year
    print("Querying park factor aggregates from DB (server-side)...")

    # Do all aggregation in SQL to avoid pulling millions of rows
    sql = f"""
    SELECT
        home_team AS venue,
        game_year,
        stand,
        COUNT(*) AS pa,
        SUM(CASE WHEN events = 'home_run' THEN 1 ELSE 0 END) AS hrs,
        SUM(CASE WHEN events IN ('single','double','triple','home_run') THEN 1 ELSE 0 END) AS hits,
        SUM(CASE WHEN events = 'double' THEN 1 ELSE 0 END) AS doubles,
        SUM(CASE WHEN events = 'triple' THEN 1 ELSE 0 END) AS triples,
        SUM(CASE WHEN events = 'walk' THEN 1 ELSE 0 END) AS bbs,
        SUM(CASE WHEN events IN ('strikeout','strikeout_double_play') THEN 1 ELSE 0 END) AS ks
    FROM statcast_pitches
    WHERE events IS NOT NULL
      AND events != ''
      AND game_type = 'R'
      AND game_year BETWEEN 2021 AND {current_year}
    GROUP BY home_team, game_year, stand
    """
    venue_df = query_df(sql)
    print(f"  {len(venue_df)} venue/year/hand groups")

    if venue_df.empty:
        print("No data found.")
        return

    # League totals per year/hand
    league = venue_df.groupby(["game_year", "stand"])[
        ["pa", "hrs", "hits", "doubles", "triples", "bbs", "ks"]
    ].sum().reset_index()

    for col in ["hrs", "hits", "doubles", "triples", "bbs", "ks"]:
        league[f"lg_{col}_rate"] = league[col] / league["pa"]

    # Merge league rates
    merged = venue_df.merge(
        league[["game_year", "stand"] + [f"lg_{c}_rate" for c in
                ["hrs", "hits", "doubles", "triples", "bbs", "ks"]]],
        on=["game_year", "stand"],
    )

    # Compute regressed factors
    for col in ["hrs", "hits", "doubles", "triples", "bbs", "ks"]:
        venue_rate = merged[col] / merged["pa"]
        lg_rate = merged[f"lg_{col}_rate"]
        raw = np.where(lg_rate > 0, venue_rate / lg_rate, 1.0)
        weight = merged["pa"] / (merged["pa"] + REGRESS_N)
        merged[f"{col}_factor"] = weight * raw + (1 - weight) * 1.0

    # Overall run factor (weighted combo)
    merged["run_factor"] = (
        0.47 * merged["hits_factor"]
        + 0.38 * merged["doubles_factor"]
        + 0.55 * merged["triples_factor"]
        + 1.40 * merged["hrs_factor"]
        + 0.33 * merged["bbs_factor"]
        - 0.25 * merged["ks_factor"]
    ) / (0.47 + 0.38 + 0.55 + 1.40 + 0.33 + 0.25)

    factor_cols = ["hrs_factor", "hits_factor", "doubles_factor", "triples_factor",
                   "bbs_factor", "ks_factor", "run_factor"]
    result = merged[["venue", "game_year", "stand", "pa"] + factor_cols].copy()
    result = result.rename(columns={
        "game_year": "season",
        "hrs_factor": "hr_factor",
        "hits_factor": "hit_factor",
        "doubles_factor": "2b_factor",
        "triples_factor": "3b_factor",
        "bbs_factor": "bb_factor",
        "ks_factor": "k_factor",
    })
    result = result[result["pa"] >= MIN_PA]

    # Also build a combined (ALL) handedness version
    league_sql = f"""
    SELECT
        home_team AS venue,
        game_year,
        COUNT(*) AS pa,
        SUM(CASE WHEN events = 'home_run' THEN 1 ELSE 0 END) AS hrs,
        SUM(CASE WHEN events IN ('single','double','triple','home_run') THEN 1 ELSE 0 END) AS hits,
        SUM(CASE WHEN events = 'double' THEN 1 ELSE 0 END) AS doubles,
        SUM(CASE WHEN events = 'triple' THEN 1 ELSE 0 END) AS triples,
        SUM(CASE WHEN events = 'walk' THEN 1 ELSE 0 END) AS bbs,
        SUM(CASE WHEN events IN ('strikeout','strikeout_double_play') THEN 1 ELSE 0 END) AS ks
    FROM statcast_pitches
    WHERE events IS NOT NULL
      AND events != ''
      AND game_type = 'R'
      AND game_year BETWEEN 2021 AND {current_year}
    GROUP BY home_team, game_year
    """
    neutral = query_df(league_sql)

    lg_neutral = neutral.groupby("game_year")[
        ["pa", "hrs", "hits", "doubles", "triples", "bbs", "ks"]
    ].sum().reset_index()
    for col in ["hrs", "hits", "doubles", "triples", "bbs", "ks"]:
        lg_neutral[f"lg_{col}_rate"] = lg_neutral[col] / lg_neutral["pa"]

    neutral = neutral.merge(
        lg_neutral[["game_year"] + [f"lg_{c}_rate" for c in
                    ["hrs", "hits", "doubles", "triples", "bbs", "ks"]]],
        on="game_year",
    )

    for col in ["hrs", "hits", "doubles", "triples", "bbs", "ks"]:
        venue_rate = neutral[col] / neutral["pa"]
        lg_rate = neutral[f"lg_{col}_rate"]
        raw = np.where(lg_rate > 0, venue_rate / lg_rate, 1.0)
        weight = neutral["pa"] / (neutral["pa"] + REGRESS_N)
        neutral[f"{col}_factor"] = weight * raw + (1 - weight) * 1.0

    neutral["run_factor"] = (
        0.47 * neutral["hits_factor"]
        + 0.38 * neutral["doubles_factor"]
        + 0.55 * neutral["triples_factor"]
        + 1.40 * neutral["hrs_factor"]
        + 0.33 * neutral["bbs_factor"]
        - 0.25 * neutral["ks_factor"]
    ) / (0.47 + 0.38 + 0.55 + 1.40 + 0.33 + 0.25)

    neutral["stand"] = "ALL"
    neutral_out = neutral[["venue", "game_year", "stand", "pa",
                            "hrs_factor", "hits_factor", "doubles_factor",
                            "triples_factor", "bbs_factor", "ks_factor", "run_factor"]].copy()
    neutral_out = neutral_out.rename(columns={
        "game_year": "season",
        "hrs_factor": "hr_factor",
        "hits_factor": "hit_factor",
        "doubles_factor": "2b_factor",
        "triples_factor": "3b_factor",
        "bbs_factor": "bb_factor",
        "ks_factor": "k_factor",
    })
    neutral_out = neutral_out[neutral_out["pa"] >= MIN_PA]

    final = pd.concat([result, neutral_out], ignore_index=True)
    final = final.sort_values(["season", "venue", "stand"]).reset_index(drop=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    final.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nPark factors saved to {OUTPUT_PATH}")
    print(f"  {len(final)} rows ({final['season'].nunique()} seasons, "
          f"{final['venue'].nunique()} venues)")
    print(f"\nSample (most recent, ALL):")
    latest = final["season"].max()
    sample = final[(final["season"] == latest) & (final["stand"] == "ALL")].sort_values(
        "run_factor", ascending=False
    )
    print(sample[["venue", "pa", "hr_factor", "hit_factor", "run_factor"]].head(10).to_string(index=False))


if __name__ == "__main__":
    build_park_factors()
