#!/usr/bin/env python3
"""
Build spray angle / pull tendency profiles for batters and pitchers.

Derives from plate_x, launch_angle, and hc_x/hc_y (hit coordinates) in statcast.
Since hc_x/hc_y may not be available, we use the spray_angle approximation from
launch_angle + stand (L/R) + is_pull logic.

Features produced per batter/season:
  - pull_pct, center_pct, oppo_pct  (batted ball direction distribution)
  - pull_hr_pct                      (% of HRs that are pulled)
  - gb_pull_pct                      (ground ball pull tendency — shift indicator)
  - spray_entropy                    (how evenly distributed — 0=all one way, ~1.6=even)
  - avg_spray_angle                  (average spray angle, negative=pull)

Features produced per pitcher/season:
  - opp_pull_pct, opp_center_pct, opp_oppo_pct  (batted ball direction against them)
  - opp_gb_pull_pct                               (GB pull tendency allowed)
  - opp_fb_pull_pct                               (fly ball pull tendency allowed)

Output: data/batter_spray_profiles.parquet, data/pitcher_spray_profiles.parquet
"""

import os
import sys
import math
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import query_df

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
BATTER_OUTPUT = os.path.join(OUTPUT_DIR, "batter_spray_profiles.parquet")
PITCHER_OUTPUT = os.path.join(OUTPUT_DIR, "pitcher_spray_profiles.parquet")

# Spray angle zones (from batter's perspective):
#   Pull:   LHB → right field, RHB → left field
#   Center: straightaway
#   Oppo:   LHB → left field, RHB → right field


def build_spray_profiles():
    from datetime import date
    current_year = date.today().year

    print("Querying batted ball data for spray profiles...")

    # We use plate_x and hit_distance_sc plus stand to determine spray direction.
    # Statcast hc_x (hit coordinate x) ranges 0-250, with 125=center.
    # hc_x < 100 = left side, hc_x > 150 = right side
    # But hc_x may not be in our DB. Use a server-side approximation:
    # Pull = when RHB hits to left side (low hc_x) or LHB hits to right side (high hc_x)
    sql = f"""
    SELECT
        batter,
        pitcher,
        game_year,
        stand,
        events,
        launch_angle,
        launch_speed,
        hit_distance_sc,
        plate_x,
        hc_x,
        hc_y
    FROM statcast_pitches
    WHERE game_type = 'R'
      AND game_year BETWEEN 2021 AND {current_year}
      AND events IS NOT NULL
      AND events != ''
      AND events IN (
          'single','double','triple','home_run','field_out',
          'grounded_into_double_play','double_play','fielders_choice',
          'fielders_choice_out','force_out','field_error','sac_fly'
      )
    """
    df = query_df(sql)
    print(f"  {len(df):,} batted balls loaded")

    if df.empty:
        print("No data. Exiting.")
        return

    # Convert types
    for col in ["launch_angle", "launch_speed", "hit_distance_sc", "plate_x", "hc_x", "hc_y"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Determine spray direction
    # If hc_x is available, use it directly (most reliable)
    # hc_x: 0=left foul line, 125=center, 250=right foul line
    has_hc = df["hc_x"].notna()

    # Spray angle from hit coordinates (Statcast convention)
    # spray_angle ≈ atan2(hc_x - 125, 200 - hc_y) in degrees
    # Positive = right field, Negative = left field
    df.loc[has_hc, "spray_angle"] = np.degrees(
        np.arctan2(df.loc[has_hc, "hc_x"] - 125.0,
                    200.0 - df.loc[has_hc, "hc_y"].fillna(150))
    )

    # For rows without hc_x, we can't determine spray direction — drop them
    df = df[has_hc].copy()
    print(f"  {len(df):,} with hit coordinates")

    # Classify direction (from BATTER's perspective)
    # Pull: RHB to left field (spray_angle < -15) or LHB to right field (spray_angle > 15)
    # Oppo: opposite
    # Center: |spray_angle| <= 15

    def _classify_spray(row):
        sa = row["spray_angle"]
        stand = row["stand"]
        if abs(sa) <= 15:
            return "center"
        if stand == "R":
            return "pull" if sa < -15 else "oppo"
        else:  # L
            return "pull" if sa > 15 else "oppo"

    df["spray_dir"] = df.apply(_classify_spray, axis=1)

    # Classify batted ball type
    la = df["launch_angle"]
    df["bb_type"] = "other"
    df.loc[la < 10, "bb_type"] = "gb"
    df.loc[(la >= 10) & (la < 25), "bb_type"] = "ld"
    df.loc[(la >= 25) & (la < 50), "bb_type"] = "fb"
    df.loc[la >= 50, "bb_type"] = "popup"

    # Is home run?
    df["is_hr"] = (df["events"] == "home_run").astype(int)

    # =====================================================================
    # BATTER spray profiles
    # =====================================================================
    print("\nBuilding batter spray profiles...")
    batter_groups = df.groupby(["batter", "game_year"])

    batter_records = []
    for (batter_id, season), group in batter_groups:
        n = len(group)
        if n < 20:  # need minimum batted balls for reliable spray profile
            continue

        pull = (group["spray_dir"] == "pull").sum()
        center = (group["spray_dir"] == "center").sum()
        oppo = (group["spray_dir"] == "oppo").sum()

        # HR pull tendency
        hrs = group[group["is_hr"] == 1]
        hr_pull = (hrs["spray_dir"] == "pull").sum() if len(hrs) > 0 else 0
        hr_total = len(hrs)

        # GB pull tendency (relevant for shift decisions and BABIP)
        gbs = group[group["bb_type"] == "gb"]
        gb_pull = (gbs["spray_dir"] == "pull").sum() if len(gbs) > 0 else 0
        gb_total = len(gbs)

        # Fly ball pull tendency
        fbs = group[group["bb_type"] == "fb"]
        fb_pull = (fbs["spray_dir"] == "pull").sum() if len(fbs) > 0 else 0
        fb_total = len(fbs)

        # Spray entropy (Shannon)
        probs = np.array([pull/n, center/n, oppo/n])
        probs = probs[probs > 0]
        entropy = float(-np.sum(probs * np.log2(probs))) if len(probs) > 1 else 0.0

        batter_records.append({
            "batter": batter_id,
            "season": season,
            "bat_spray_n": n,
            "bat_pull_pct": pull / n,
            "bat_center_pct": center / n,
            "bat_oppo_pct": oppo / n,
            "bat_pull_hr_pct": hr_pull / hr_total if hr_total >= 3 else np.nan,
            "bat_gb_pull_pct": gb_pull / gb_total if gb_total >= 10 else np.nan,
            "bat_fb_pull_pct": fb_pull / fb_total if fb_total >= 10 else np.nan,
            "bat_spray_entropy": entropy,
            "bat_avg_spray_angle": group["spray_angle"].mean(),
        })

    batter_spray = pd.DataFrame(batter_records)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    batter_spray.to_parquet(BATTER_OUTPUT, index=False)
    print(f"  Batter spray profiles saved: {len(batter_spray):,} rows "
          f"({batter_spray['batter'].nunique():,} batters)")

    # =====================================================================
    # PITCHER spray profiles (what they allow)
    # =====================================================================
    print("\nBuilding pitcher spray profiles...")
    pitcher_groups = df.groupby(["pitcher", "game_year"])

    pitcher_records = []
    for (pitcher_id, season), group in pitcher_groups:
        n = len(group)
        if n < 30:
            continue

        pull = (group["spray_dir"] == "pull").sum()
        center = (group["spray_dir"] == "center").sum()
        oppo = (group["spray_dir"] == "oppo").sum()

        gbs = group[group["bb_type"] == "gb"]
        gb_pull = (gbs["spray_dir"] == "pull").sum() if len(gbs) > 0 else 0
        gb_total = len(gbs)

        fbs = group[group["bb_type"] == "fb"]
        fb_pull = (fbs["spray_dir"] == "pull").sum() if len(fbs) > 0 else 0
        fb_total = len(fbs)

        pitcher_records.append({
            "pitcher": pitcher_id,
            "season": season,
            "p_spray_n": n,
            "p_opp_pull_pct": pull / n,
            "p_opp_center_pct": center / n,
            "p_opp_oppo_pct": oppo / n,
            "p_opp_gb_pull_pct": gb_pull / gb_total if gb_total >= 15 else np.nan,
            "p_opp_fb_pull_pct": fb_pull / fb_total if fb_total >= 15 else np.nan,
        })

    pitcher_spray = pd.DataFrame(pitcher_records)
    pitcher_spray.to_parquet(PITCHER_OUTPUT, index=False)
    print(f"  Pitcher spray profiles saved: {len(pitcher_spray):,} rows "
          f"({pitcher_spray['pitcher'].nunique():,} pitchers)")


if __name__ == "__main__":
    build_spray_profiles()
