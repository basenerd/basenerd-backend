#!/usr/bin/env python3
"""
Build matchup training data: batter vs pitcher plate appearance outcomes.

For each PA, combines:
- Batter profile features (season-level discipline/contact quality)
- Batter-vs-pitch-type features (weighted by pitcher's arsenal usage)
- Pitcher arsenal features (pitch mix, stuff+, control+)
- Per-pitch-type pitcher stats (top 3 pitches)
- Platoon matchup (L/R)
- Context features (park, count leverage)

Target: PA outcome (K, BB, HBP, 1B, 2B, 3B, HR, out-in-play)

Output: data/matchup_train.parquet
"""

import os
import sys
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import query_df

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "matchup_train.parquet")

# Pitch categories for batter-vs-pitch-type features
PITCH_CATEGORY = {
    "FF": "fastball", "SI": "fastball", "FC": "fastball",
    "SL": "breaking", "CU": "breaking", "KC": "breaking", "ST": "breaking", "SV": "breaking",
    "CH": "offspeed", "FS": "offspeed", "KN": "offspeed",
}

# PA outcome mapping
OUTCOME_MAP = {
    "strikeout": "K", "strikeout_double_play": "K",
    "walk": "BB", "intent_walk": "IBB",
    "hit_by_pitch": "HBP",
    "single": "1B",
    "double": "2B",
    "triple": "3B",
    "home_run": "HR",
    "field_out": "OUT", "grounded_into_double_play": "OUT",
    "double_play": "OUT", "fielders_choice": "OUT",
    "fielders_choice_out": "OUT", "force_out": "OUT",
    "field_error": "OUT",  # count as out for modeling (defense-dependent)
    "sac_fly": "OUT", "sac_bunt": "OUT", "sac_fly_double_play": "OUT",
    "triple_play": "OUT",
    "catcher_interf": "OTHER",
}


def build_matchup_training():
    from datetime import date
    current_year = date.today().year
    print("Querying plate appearance data...")

    # Get one row per PA (the pitch where the event happened)
    sql = f"""
    SELECT
        sp.batter,
        sp.pitcher,
        sp.game_pk,
        sp.game_date,
        sp.game_year,
        sp.stand,
        sp.p_throws,
        sp.events,
        sp.launch_speed,
        sp.launch_angle,
        sp.estimated_woba_using_speedangle,
        sp.home_team,
        sp.away_team,
        sp.inning_topbot,
        sp.inning,
        sp.outs_when_up,
        sp.on_1b,
        sp.on_2b,
        sp.on_3b,
        sp.n_thruorder_pitcher
    FROM statcast_pitches sp
    WHERE sp.events IS NOT NULL
      AND sp.events != ''
      AND sp.game_type = 'R'
      AND sp.game_year BETWEEN 2021 AND {current_year}
    """
    df = query_df(sql)
    print(f"  {len(df):,} plate appearances loaded")

    if df.empty:
        print("No data found. Exiting.")
        return

    # Map outcomes
    df["outcome"] = df["events"].map(OUTCOME_MAP).fillna("OTHER")
    df = df[df["outcome"] != "OTHER"]  # drop rare events

    # Venue for park factor join
    df["venue"] = df["home_team"]

    # Runners on
    df["runner_on_1b"] = (df["on_1b"].notna() & (df["on_1b"] != 0)).astype(int)
    df["runner_on_2b"] = (df["on_2b"].notna() & (df["on_2b"] != 0)).astype(int)
    df["runner_on_3b"] = (df["on_3b"].notna() & (df["on_3b"] != 0)).astype(int)

    # --- Load pre-built feature datasets ---
    batter_path = os.path.join(OUTPUT_DIR, "batter_profiles.parquet")
    pitcher_path = os.path.join(OUTPUT_DIR, "pitcher_arsenal.parquet")
    park_path = os.path.join(OUTPUT_DIR, "park_factors.parquet")

    missing = []
    if not os.path.exists(batter_path):
        missing.append("batter_profiles.parquet")
    if not os.path.exists(pitcher_path):
        missing.append("pitcher_arsenal.parquet")
    if not os.path.exists(park_path):
        missing.append("park_factors.parquet")

    if missing:
        print(f"\nMissing prerequisite datasets: {', '.join(missing)}")
        print("Run the corresponding build scripts first.")
        print("Saving PA-level data without feature joins...")
        # Save basic PA data for now
        result = df[[
            "batter", "pitcher", "game_pk", "game_date", "game_year",
            "stand", "p_throws", "outcome", "venue",
            "launch_speed", "launch_angle", "estimated_woba_using_speedangle",
            "inning", "outs_when_up", "n_thruorder_pitcher",
            "runner_on_1b", "runner_on_2b", "runner_on_3b",
        ]].copy()
        result = result.rename(columns={"game_year": "season"})
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        result.to_parquet(OUTPUT_PATH, index=False)
        print(f"  Basic PA data saved: {len(result):,} rows")
        return

    # Load batter profiles (use "ALL" hand split as baseline)
    batter_df = pd.read_parquet(batter_path)
    batter_all = batter_df[batter_df["vs_hand"] == "ALL"].copy()
    batter_all = batter_all.drop(columns=["vs_hand"])
    batter_cols = [c for c in batter_all.columns if c not in ["batter", "season", "pa", "pitches"]]
    batter_all = batter_all.rename(columns={c: f"bat_{c}" for c in batter_cols})

    # Also get platoon-specific batter stats
    batter_plat = batter_df[batter_df["vs_hand"].isin(["L", "R"])].copy()
    plat_cols = ["k_pct", "bb_pct", "whiff_rate", "chase_rate", "avg_ev", "barrel_rate", "xwoba"]
    batter_plat_sub = batter_plat[["batter", "season", "vs_hand"] + plat_cols].copy()
    batter_plat_sub = batter_plat_sub.rename(columns={c: f"bat_plat_{c}" for c in plat_cols})

    # Load pitcher arsenal (aggregate stats, "ALL" handedness split)
    pitcher_df = pd.read_parquet(pitcher_path)
    # Compute pitcher-level aggregates (weighted across pitch types)
    pitcher_all = pitcher_df[pitcher_df["stand"] == "ALL"].copy()
    pitcher_agg = pitcher_all.groupby(["pitcher", "season"]).apply(
        lambda g: pd.Series({
            "p_avg_stuff_plus": np.average(g["avg_stuff_plus"].dropna(),
                                            weights=g.loc[g["avg_stuff_plus"].notna(), "n"])
                              if g["avg_stuff_plus"].notna().any() else np.nan,
            "p_avg_control_plus": np.average(g["avg_control_plus"].dropna(),
                                              weights=g.loc[g["avg_control_plus"].notna(), "n"])
                                if g["avg_control_plus"].notna().any() else np.nan,
            "p_avg_velo": np.average(g["avg_velo"].dropna(),
                                      weights=g.loc[g["avg_velo"].notna(), "n"])
                          if g["avg_velo"].notna().any() else np.nan,
            "p_whiff_rate": np.average(g["whiff_rate"].dropna(),
                                        weights=g.loc[g["whiff_rate"].notna(), "n"])
                            if g["whiff_rate"].notna().any() else np.nan,
            "p_chase_rate": np.average(g["chase_rate"].dropna(),
                                        weights=g.loc[g["chase_rate"].notna(), "n"])
                            if g["chase_rate"].notna().any() else np.nan,
            "p_zone_rate": np.average(g["zone_rate"].dropna(),
                                       weights=g.loc[g["zone_rate"].notna(), "n"])
                           if g["zone_rate"].notna().any() else np.nan,
            "p_xwoba": np.average(g["xwoba"].dropna(),
                                   weights=g.loc[g["xwoba"].notna(), "n"])
                       if g["xwoba"].notna().any() else np.nan,
            "p_num_pitches": len(g),
            "p_total_thrown": g["n"].sum(),
        })
    ).reset_index()

    # Load park factors
    park_df = pd.read_parquet(park_path)
    park_all = park_df[park_df["stand"] == "ALL"][["venue", "season", "run_factor", "hr_factor"]].copy()
    park_all = park_all.rename(columns={"run_factor": "park_run_factor", "hr_factor": "park_hr_factor"})

    # --- Join features to PAs ---
    print("Joining feature datasets...")

    result = df[[
        "batter", "pitcher", "game_pk", "game_date", "game_year",
        "stand", "p_throws", "outcome", "venue",
        "launch_speed", "launch_angle", "estimated_woba_using_speedangle",
        "inning", "outs_when_up", "n_thruorder_pitcher",
        "runner_on_1b", "runner_on_2b", "runner_on_3b",
    ]].copy()
    result = result.rename(columns={"game_year": "season"})

    # Join batter overall profile
    result = result.merge(batter_all, left_on=["batter", "season"],
                          right_on=["batter", "season"], how="left")

    # Join batter platoon stats (vs the pitcher's throwing hand)
    result = result.merge(batter_plat_sub,
                          left_on=["batter", "season", "p_throws"],
                          right_on=["batter", "season", "vs_hand"],
                          how="left")
    if "vs_hand" in result.columns:
        result = result.drop(columns=["vs_hand"])

    # Join pitcher aggregate
    result = result.merge(pitcher_agg, left_on=["pitcher", "season"],
                          right_on=["pitcher", "season"], how="left")

    # Join park factors
    result = result.merge(park_all, on=["venue", "season"], how="left")

    # --- NEW: Pitch-weighted batter-vs-pitch-type features ---
    batter_pt_path = os.path.join(OUTPUT_DIR, "batter_pitch_type_profiles.parquet")
    if os.path.exists(batter_pt_path):
        print("Computing pitch-weighted batter-vs-pitch-type features...")
        batter_pt = pd.read_parquet(batter_pt_path)

        # Category-level rows (CAT_fastball, CAT_breaking, CAT_offspeed)
        batter_cat = batter_pt[batter_pt["pitch_type"].str.startswith("CAT_")].copy()
        batter_cat["pitch_category"] = batter_cat["pitch_type"].str.replace("CAT_", "", 1)

        # Get pitcher's usage by category for weighting
        pitcher_usage_by_cat = pitcher_all.copy()
        pitcher_usage_by_cat["pitch_category"] = pitcher_usage_by_cat["pitch_type"].map(PITCH_CATEGORY)
        pitcher_usage_by_cat = pitcher_usage_by_cat.dropna(subset=["pitch_category"])
        pitcher_cat_usage = pitcher_usage_by_cat.groupby(
            ["pitcher", "season", "pitch_category"]
        )["n"].sum().reset_index()
        pitcher_cat_total = pitcher_cat_usage.groupby(["pitcher", "season"])["n"].sum().reset_index(name="total_n")
        pitcher_cat_usage = pitcher_cat_usage.merge(pitcher_cat_total, on=["pitcher", "season"])
        pitcher_cat_usage["cat_usage"] = pitcher_cat_usage["n"] / pitcher_cat_usage["total_n"]

        # For each PA, compute usage-weighted batter stats
        # Step 1: Cross-join PA batter/pitcher/season with category batter stats
        bat_cat_stats = ["whiff_rate", "chase_rate", "zone_contact_rate", "hard_hit_rate", "xwoba"]

        # Build a lookup: batter x season x category -> stats
        cat_lookup = batter_cat[["batter", "season", "pitch_category"] + bat_cat_stats].copy()
        cat_lookup = cat_lookup.rename(columns={c: f"bvpt_{c}" for c in bat_cat_stats})

        # Build a lookup: pitcher x season x category -> usage weight
        usage_lookup = pitcher_cat_usage[["pitcher", "season", "pitch_category", "cat_usage"]].copy()

        # Merge: for each PA, get all 3 categories' batter stats and pitcher usage weights
        weighted_features = []
        for cat in ["fastball", "breaking", "offspeed"]:
            cat_bat = cat_lookup[cat_lookup["pitch_category"] == cat].drop(columns=["pitch_category"])
            cat_bat = cat_bat.rename(columns={c: f"{c}_{cat}" for c in cat_bat.columns if c.startswith("bvpt_")})

            cat_use = usage_lookup[usage_lookup["pitch_category"] == cat][["pitcher", "season", "cat_usage"]].copy()
            cat_use = cat_use.rename(columns={"cat_usage": f"p_usage_{cat}"})

            result = result.merge(cat_bat, left_on=["batter", "season"],
                                  right_on=["batter", "season"], how="left")
            result = result.merge(cat_use, left_on=["pitcher", "season"],
                                  right_on=["pitcher", "season"], how="left")

        # Compute pitch-weighted composite stats
        for stat in bat_cat_stats:
            fb_col = f"bvpt_{stat}_fastball"
            brk_col = f"bvpt_{stat}_breaking"
            off_col = f"bvpt_{stat}_offspeed"
            fb_w = "p_usage_fastball"
            brk_w = "p_usage_breaking"
            off_w = "p_usage_offspeed"

            # Weighted average: batter stat vs each pitch category, weighted by pitcher's usage
            result[f"bvpt_w_{stat}"] = (
                result[fb_col].fillna(0) * result[fb_w].fillna(0) +
                result[brk_col].fillna(0) * result[brk_w].fillna(0) +
                result[off_col].fillna(0) * result[off_w].fillna(0)
            )
            # Handle NaN — if all batter stats are NaN, result should be NaN
            all_nan = result[fb_col].isna() & result[brk_col].isna() & result[off_col].isna()
            result.loc[all_nan, f"bvpt_w_{stat}"] = np.nan

        # Also keep the per-category batter stats as direct features
        # (the model can learn that a high breaking-ball whiff rate + a heavy breaking ball pitcher = more Ks)
        print(f"  Added pitch-weighted features for {len(result):,} PAs")
    else:
        print("  batter_pitch_type_profiles.parquet not found — skipping pitch-weighted features")

    # --- NEW: Pitcher top-3 pitch type stats ---
    # For each pitcher/season, get their top 3 pitches by usage and include per-pitch stats
    print("Computing pitcher top-3 pitch features...")
    pitcher_top3_rows = []
    for (pid, season), group in pitcher_all.groupby(["pitcher", "season"]):
        top3 = group.nlargest(3, "n")
        for rank, (_, row) in enumerate(top3.iterrows(), 1):
            pitcher_top3_rows.append({
                "pitcher": pid,
                "season": season,
                f"p_pitch{rank}_type": row["pitch_type"],
                f"p_pitch{rank}_usage": row.get("usage", row["n"] / group["n"].sum()),
                f"p_pitch{rank}_velo": row.get("avg_velo"),
                f"p_pitch{rank}_whiff": row.get("whiff_rate"),
                f"p_pitch{rank}_stuff": row.get("avg_stuff_plus"),
            })

    # Pivot top-3 pitch data
    top3_df = pd.DataFrame(pitcher_top3_rows)
    if not top3_df.empty:
        # Group by pitcher/season to combine rank 1, 2, 3 into one row
        top3_pivot = top3_df.groupby(["pitcher", "season"]).first().reset_index()
        # Need to handle multiple ranks — actually, the loop appends separate rows per rank
        # Let me restructure: build dict per pitcher/season
        top3_dict = {}
        for row in pitcher_top3_rows:
            key = (row["pitcher"], row["season"])
            if key not in top3_dict:
                top3_dict[key] = {"pitcher": row["pitcher"], "season": row["season"]}
            top3_dict[key].update({k: v for k, v in row.items() if k not in ["pitcher", "season"]})
        top3_pivot = pd.DataFrame(list(top3_dict.values()))

        result = result.merge(top3_pivot, on=["pitcher", "season"], how="left")
        print(f"  Added top-3 pitch features")

    # --- NEW: Recent form features (rolling 14-day) ---
    batter_form_path = os.path.join(OUTPUT_DIR, "recent_form_batter.parquet")
    pitcher_form_path = os.path.join(OUTPUT_DIR, "recent_form_pitcher.parquet")

    if os.path.exists(batter_form_path):
        print("Joining batter recent form (14-day rolling)...")
        bat_form = pd.read_parquet(batter_form_path)
        bat_form["game_date"] = pd.to_datetime(bat_form["game_date"])
        result["game_date"] = pd.to_datetime(result["game_date"])
        result = result.merge(bat_form, on=["batter", "game_date"], how="left")
        non_null = result["bat_r14_k_pct"].notna().sum()
        print(f"  {non_null:,} PAs with batter recent form ({non_null/len(result)*100:.1f}%)")
    else:
        print("  recent_form_batter.parquet not found — skipping")

    if os.path.exists(pitcher_form_path):
        print("Joining pitcher recent form (14-day rolling)...")
        pit_form = pd.read_parquet(pitcher_form_path)
        pit_form["game_date"] = pd.to_datetime(pit_form["game_date"])
        result["game_date"] = pd.to_datetime(result["game_date"])
        result = result.merge(pit_form, on=["pitcher", "game_date"], how="left")
        non_null = result["p_r14_k_pct"].notna().sum()
        print(f"  {non_null:,} PAs with pitcher recent form ({non_null/len(result)*100:.1f}%)")
    else:
        print("  recent_form_pitcher.parquet not found — skipping")

    result = result.sort_values(["season", "game_date", "game_pk"]).reset_index(drop=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    result.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nMatchup training data saved to {OUTPUT_PATH}")
    print(f"  {len(result):,} plate appearances")
    print(f"\nOutcome distribution:")
    print(result["outcome"].value_counts().to_string())
    print(f"\nFeature columns: {len(result.columns)}")
    print(result.columns.tolist())


if __name__ == "__main__":
    build_matchup_training()
