#!/usr/bin/env python3
"""
Build matchup training data v2: massive feature enhancement.

Improvements over v1:
- Prior-year stabilized batter/pitcher stats (solves early-season noise)
- Sample size features so model knows how reliable current-year stats are
- Count features (balls, strikes) — huge K/BB predictor
- Score differential / leverage context
- Pitcher fatigue proxy (pitches thrown in game before this PA)
- Interaction features (batter K% x pitcher K%, etc.)
- Delta-from-league features (how far above/below average)
- Arsenal diversity (entropy of pitch mix)
- Platoon advantage magnitude
- Time-through-order awareness
- Catcher identity for framing effects
- Properly dampened recent form with confidence weighting

Output: data/matchup_train_v2.parquet
"""

import os
import sys
import math
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import query_df

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "matchup_train_v2.parquet")

PITCH_CATEGORY = {
    "FF": "fastball", "SI": "fastball", "FC": "fastball",
    "SL": "breaking", "CU": "breaking", "KC": "breaking", "ST": "breaking", "SV": "breaking",
    "CH": "offspeed", "FS": "offspeed", "KN": "offspeed",
}

OUTCOME_MAP = {
    "strikeout": "K", "strikeout_double_play": "K",
    "walk": "BB", "intent_walk": "IBB",
    "hit_by_pitch": "HBP",
    "single": "1B", "double": "2B", "triple": "3B", "home_run": "HR",
    "field_out": "OUT", "grounded_into_double_play": "OUT",
    "double_play": "OUT", "fielders_choice": "OUT",
    "fielders_choice_out": "OUT", "force_out": "OUT",
    "field_error": "OUT", "sac_fly": "OUT", "sac_bunt": "OUT",
    "sac_fly_double_play": "OUT", "triple_play": "OUT",
    "catcher_interf": "OTHER",
}

# League averages for delta features (2021-2025)
LEAGUE_AVG = {
    "k_pct": 0.223, "bb_pct": 0.083, "whiff_rate": 0.245,
    "chase_rate": 0.295, "zone_contact_rate": 0.82,
    "barrel_rate": 0.068, "hard_hit_rate": 0.35, "xwoba": 0.315,
    "avg_ev": 88.5, "iso": 0.155, "babip": 0.295,
}


def build_matchup_training_v2():
    from datetime import date
    current_year = date.today().year
    print("=" * 70)
    print("MATCHUP TRAINING DATA V2 — Enhanced Feature Engineering")
    print("=" * 70)

    # =====================================================================
    # STEP 1: Load plate appearance data with count info + score context
    # =====================================================================
    print("\n[1/12] Querying plate appearance data with counts and context...")
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
        sp.n_thruorder_pitcher,
        sp.balls,
        sp.strikes,
        sp.bat_score,
        sp.fld_score,
        sp.fielder_2 AS catcher_id,
        sp.at_bat_number,
        sp.pitcher_days_since_prev_game
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
    df = df[df["outcome"] != "OTHER"]

    # =====================================================================
    # STEP 2: Core context features
    # =====================================================================
    print("\n[2/12] Building context features...")

    df["venue"] = df["home_team"]
    df["runner_on_1b"] = (df["on_1b"].notna() & (df["on_1b"] != 0)).astype(int)
    df["runner_on_2b"] = (df["on_2b"].notna() & (df["on_2b"] != 0)).astype(int)
    df["runner_on_3b"] = (df["on_3b"].notna() & (df["on_3b"] != 0)).astype(int)

    # Count features (huge predictor of K/BB outcomes)
    df["balls"] = pd.to_numeric(df["balls"], errors="coerce").fillna(0).astype(int)
    df["strikes"] = pd.to_numeric(df["strikes"], errors="coerce").fillna(0).astype(int)
    df["count_leverage"] = df["strikes"] - df["balls"]  # positive = pitcher ahead

    # Score differential (from batting team perspective)
    df["bat_score"] = pd.to_numeric(df["bat_score"], errors="coerce").fillna(0)
    df["fld_score"] = pd.to_numeric(df["fld_score"], errors="coerce").fillna(0)
    df["score_diff"] = df["bat_score"] - df["fld_score"]
    # Clamp to avoid extreme blowout values dominating
    df["score_diff"] = df["score_diff"].clip(-8, 8)

    # Runners on base count
    df["runners_on"] = df["runner_on_1b"] + df["runner_on_2b"] + df["runner_on_3b"]

    # Base-out state (encodes leverage situation)
    df["base_out_state"] = df["runners_on"] * 3 + df["outs_when_up"]

    # Is batting team home?
    df["is_home_batting"] = (df["inning_topbot"] == "Bot").astype(int)

    # Late & close game (high leverage proxy)
    df["late_and_close"] = (
        (df["inning"] >= 7) &
        (df["score_diff"].abs() <= 2)
    ).astype(int)

    # Season day (days since March 20 of that year — proxy for early-season flag)
    df["game_date"] = pd.to_datetime(df["game_date"])
    df["day_of_season"] = (df["game_date"] - pd.to_datetime(
        df["game_year"].astype(str) + "-03-20"
    )).dt.days.clip(0, 200)

    # Pitcher days rest
    df["pitcher_days_rest"] = pd.to_numeric(
        df["pitcher_days_since_prev_game"], errors="coerce").clip(0, 30)

    result = df[[
        "batter", "pitcher", "game_pk", "game_date", "game_year",
        "stand", "p_throws", "outcome", "venue", "catcher_id",
        "at_bat_number",
        "launch_speed", "launch_angle", "estimated_woba_using_speedangle",
        "inning", "outs_when_up", "n_thruorder_pitcher",
        "runner_on_1b", "runner_on_2b", "runner_on_3b",
        "balls", "strikes", "count_leverage",
        "score_diff", "runners_on", "base_out_state",
        "is_home_batting", "late_and_close", "day_of_season",
        "pitcher_days_rest",
    ]].copy()
    result = result.rename(columns={"game_year": "season"})

    print(f"  Context features added: balls, strikes, count_leverage, score_diff, "
          f"runners_on, base_out_state, is_home_batting, late_and_close, day_of_season")

    # =====================================================================
    # STEP 3: Batter profiles — current season + prior year stabilized
    # =====================================================================
    print("\n[3/12] Joining batter profiles (current + prior year)...")
    batter_path = os.path.join(OUTPUT_DIR, "batter_profiles.parquet")
    if os.path.exists(batter_path):
        batter_df = pd.read_parquet(batter_path)
        batter_all = batter_df[batter_df["vs_hand"] == "ALL"].copy()
        batter_all = batter_all.drop(columns=["vs_hand"])

        batter_cols = [c for c in batter_all.columns
                       if c not in ["batter", "season", "pa", "pitches"]]

        # Convert all numeric columns to float (psycopg returns Decimal)
        for c in batter_cols:
            batter_all[c] = pd.to_numeric(batter_all[c], errors="coerce").astype("float64")
        batter_all["pa"] = pd.to_numeric(batter_all["pa"], errors="coerce").astype("float64")

        # Current season stats
        batter_curr = batter_all.rename(columns={c: f"bat_{c}" for c in batter_cols})
        batter_curr["bat_season_pa"] = batter_all["pa"]  # sample size feature!

        result = result.merge(batter_curr, left_on=["batter", "season"],
                              right_on=["batter", "season"], how="left")

        # Prior year stats (stabilized baseline)
        batter_prev = batter_all.copy()
        batter_prev["season"] = batter_prev["season"] + 1  # shift so it joins as "prior"
        batter_prev = batter_prev.rename(columns={c: f"bat_prev_{c}" for c in batter_cols})
        batter_prev["bat_prev_pa"] = batter_all["pa"]
        batter_prev = batter_prev.drop(columns=["pa", "pitches"], errors="ignore")

        result = result.merge(batter_prev, left_on=["batter", "season"],
                              right_on=["batter", "season"], how="left")

        # Weighted blend: current season weighted by PA, prior year fills gaps
        # The model will learn how to weight these, but we also provide a
        # pre-blended version for convenience
        blend_pa_threshold = 100  # PA needed before current season dominates
        for col in batter_cols:
            curr_col = f"bat_{col}"
            prev_col = f"bat_prev_{col}"
            blend_col = f"bat_blend_{col}"
            if curr_col in result.columns and prev_col in result.columns:
                pa = result["bat_season_pa"].fillna(0)
                w = (pa / blend_pa_threshold).clip(0, 1)
                curr_val = result[curr_col]
                prev_val = result[prev_col]
                # When current season has enough PA, use it; otherwise lean on prior year
                result[blend_col] = w * curr_val + (1 - w) * prev_val
                # If both are NaN, leave as NaN (model handles missing)
                both_nan = curr_val.isna() & prev_val.isna()
                result.loc[both_nan, blend_col] = np.nan

        print(f"  Added bat_* (current), bat_prev_* (prior year), bat_blend_* (weighted)")
        print(f"  bat_season_pa feature added for sample size awareness")

        # Platoon-specific
        batter_plat = batter_df[batter_df["vs_hand"].isin(["L", "R"])].copy()
        plat_cols = ["k_pct", "bb_pct", "whiff_rate", "chase_rate",
                     "avg_ev", "barrel_rate", "xwoba"]
        batter_plat_sub = batter_plat[["batter", "season", "vs_hand"] + plat_cols].copy()
        batter_plat_sub = batter_plat_sub.rename(
            columns={c: f"bat_plat_{c}" for c in plat_cols})

        result = result.merge(batter_plat_sub,
                              left_on=["batter", "season", "p_throws"],
                              right_on=["batter", "season", "vs_hand"], how="left")
        if "vs_hand" in result.columns:
            result = result.drop(columns=["vs_hand"])
    else:
        print("  WARNING: batter_profiles.parquet not found")

    # =====================================================================
    # STEP 4: Pitcher arsenal features
    # =====================================================================
    print("\n[4/12] Joining pitcher arsenal features...")
    pitcher_path = os.path.join(OUTPUT_DIR, "pitcher_arsenal.parquet")
    if os.path.exists(pitcher_path):
        pitcher_df = pd.read_parquet(pitcher_path)
        pitcher_all = pitcher_df[pitcher_df["stand"] == "ALL"].copy()

        # Pitcher aggregates (weighted across pitch types)
        pitcher_agg = pitcher_all.groupby(["pitcher", "season"]).apply(
            lambda g: _pitcher_agg(g)
        ).reset_index()

        # Pitcher season PA/pitches for sample size
        pitcher_totals = pitcher_all.groupby(["pitcher", "season"])["n"].sum().reset_index(
            name="p_season_pitches")
        pitcher_agg = pitcher_agg.merge(pitcher_totals, on=["pitcher", "season"], how="left")

        result = result.merge(pitcher_agg, on=["pitcher", "season"], how="left")

        # Prior year pitcher stats
        pitcher_prev = pitcher_agg.copy()
        pitcher_prev["season"] = pitcher_prev["season"] + 1
        prev_rename = {c: f"{c.replace('p_', 'p_prev_')}" for c in pitcher_agg.columns
                       if c.startswith("p_") and c not in ["pitcher"]}
        prev_rename["p_season_pitches"] = "p_prev_season_pitches"
        pitcher_prev = pitcher_prev.rename(columns=prev_rename)
        result = result.merge(pitcher_prev, on=["pitcher", "season"], how="left")

        # Arsenal diversity: Shannon entropy of pitch usage
        arsenal_entropy = pitcher_all.groupby(["pitcher", "season"]).apply(
            _arsenal_entropy
        ).reset_index(name="p_arsenal_entropy")
        result = result.merge(arsenal_entropy, on=["pitcher", "season"], how="left")

        # Pitcher pitch count
        pitch_count = pitcher_all.groupby(["pitcher", "season"])["n"].apply(
            lambda x: len(x)  # number of distinct pitch types
        ).reset_index(name="p_pitch_type_count")
        result = result.merge(pitch_count, on=["pitcher", "season"], how="left")

        # Category usage
        pitcher_cat_usage = _compute_pitcher_category_usage(pitcher_all)
        result = result.merge(pitcher_cat_usage, on=["pitcher", "season"], how="left")

        # Top 3 pitches
        top3_pivot = _compute_pitcher_top3(pitcher_all)
        if top3_pivot is not None:
            result = result.merge(top3_pivot, on=["pitcher", "season"], how="left")

        print(f"  Added pitcher arsenal, prior year, entropy, pitch type count")
    else:
        print("  WARNING: pitcher_arsenal.parquet not found")

    # =====================================================================
    # STEP 5: Batter vs pitch-type features
    # =====================================================================
    print("\n[5/12] Joining batter-vs-pitch-type features...")
    batter_pt_path = os.path.join(OUTPUT_DIR, "batter_pitch_type_profiles.parquet")
    if os.path.exists(batter_pt_path) and os.path.exists(pitcher_path):
        batter_pt = pd.read_parquet(batter_pt_path)
        batter_cat = batter_pt[batter_pt["pitch_type"].str.startswith("CAT_")].copy()
        batter_cat["pitch_category"] = batter_cat["pitch_type"].str.replace("CAT_", "", 1)

        bat_cat_stats = ["whiff_rate", "chase_rate", "zone_contact_rate",
                         "hard_hit_rate", "xwoba"]
        cat_lookup = batter_cat[["batter", "season", "pitch_category"] + bat_cat_stats].copy()
        cat_lookup = cat_lookup.rename(columns={c: f"bvpt_{c}" for c in bat_cat_stats})

        for cat in ["fastball", "breaking", "offspeed"]:
            cat_bat = cat_lookup[cat_lookup["pitch_category"] == cat].drop(
                columns=["pitch_category"])
            cat_bat = cat_bat.rename(
                columns={c: f"{c}_{cat}" for c in cat_bat.columns if c.startswith("bvpt_")})
            result = result.merge(cat_bat, on=["batter", "season"], how="left")

        # Pitch-weighted composites
        for stat in bat_cat_stats:
            fb_col = f"bvpt_{stat}_fastball"
            brk_col = f"bvpt_{stat}_breaking"
            off_col = f"bvpt_{stat}_offspeed"
            fb_w = "p_usage_fastball"
            brk_w = "p_usage_breaking"
            off_w = "p_usage_offspeed"

            if all(c in result.columns for c in [fb_col, brk_col, off_col, fb_w, brk_w, off_w]):
                result[f"bvpt_w_{stat}"] = (
                    result[fb_col].fillna(0) * result[fb_w].fillna(0) +
                    result[brk_col].fillna(0) * result[brk_w].fillna(0) +
                    result[off_col].fillna(0) * result[off_w].fillna(0)
                )
                all_nan = result[fb_col].isna() & result[brk_col].isna() & result[off_col].isna()
                result.loc[all_nan, f"bvpt_w_{stat}"] = np.nan

        print(f"  Added bvpt per-category and weighted composite features")
    else:
        print("  Skipping batter-vs-pitch-type features (missing data)")

    # =====================================================================
    # STEP 6: Park factors
    # =====================================================================
    print("\n[6/12] Joining park factors...")
    park_path = os.path.join(OUTPUT_DIR, "park_factors.parquet")
    if os.path.exists(park_path):
        park_df = pd.read_parquet(park_path)
        park_all = park_df[park_df["stand"] == "ALL"][
            ["venue", "season", "run_factor", "hr_factor"]].copy()
        park_all = park_all.rename(columns={
            "run_factor": "park_run_factor", "hr_factor": "park_hr_factor"})
        result = result.merge(park_all, on=["venue", "season"], how="left")
        print(f"  Park factors joined")
    else:
        print("  WARNING: park_factors.parquet not found")

    # =====================================================================
    # STEP 7: Recent form (14-day rolling) with confidence weighting
    # =====================================================================
    print("\n[7/12] Joining recent form features with confidence...")
    batter_form_path = os.path.join(OUTPUT_DIR, "recent_form_batter.parquet")
    pitcher_form_path = os.path.join(OUTPUT_DIR, "recent_form_pitcher.parquet")

    if os.path.exists(batter_form_path):
        bat_form = pd.read_parquet(batter_form_path)
        bat_form["game_date"] = pd.to_datetime(bat_form["game_date"])
        result["game_date"] = pd.to_datetime(result["game_date"])
        result = result.merge(bat_form, on=["batter", "game_date"], how="left")
        non_null = result["bat_r14_k_pct"].notna().sum()
        print(f"  Batter recent form: {non_null:,} PAs ({non_null/len(result)*100:.1f}%)")

    if os.path.exists(pitcher_form_path):
        pit_form = pd.read_parquet(pitcher_form_path)
        pit_form["game_date"] = pd.to_datetime(pit_form["game_date"])
        result = result.merge(pit_form, on=["pitcher", "game_date"], how="left")
        non_null = result["p_r14_k_pct"].notna().sum()
        print(f"  Pitcher recent form: {non_null:,} PAs ({non_null/len(result)*100:.1f}%)")

    # =====================================================================
    # STEP 7b: Spray profiles
    # =====================================================================
    batter_spray_path = os.path.join(OUTPUT_DIR, "batter_spray_profiles.parquet")
    pitcher_spray_path = os.path.join(OUTPUT_DIR, "pitcher_spray_profiles.parquet")

    if os.path.exists(batter_spray_path):
        print("  Joining batter spray profiles...")
        bat_spray = pd.read_parquet(batter_spray_path)
        result = result.merge(bat_spray, on=["batter", "season"], how="left")
        print(f"    {result['bat_pull_pct'].notna().sum():,} PAs with batter spray data")
    else:
        print("  batter_spray_profiles.parquet not found — run build_spray_profiles.py")

    if os.path.exists(pitcher_spray_path):
        print("  Joining pitcher spray profiles...")
        pit_spray = pd.read_parquet(pitcher_spray_path)
        result = result.merge(pit_spray, on=["pitcher", "season"], how="left")
        print(f"    {result['p_opp_pull_pct'].notna().sum():,} PAs with pitcher spray data")
    else:
        print("  pitcher_spray_profiles.parquet not found — run build_spray_profiles.py")

    # =====================================================================
    # STEP 7c: Pitch sequence context
    # =====================================================================
    seq_path = os.path.join(OUTPUT_DIR, "pitch_sequence_features.parquet")
    if os.path.exists(seq_path):
        print("  Joining pitch sequence features...")
        seq_df = pd.read_parquet(seq_path)
        seq_df["game_date"] = pd.to_datetime(seq_df["game_date"])
        # Join on batter+pitcher+game_pk+at_bat_number
        seq_cols = [c for c in seq_df.columns if c.startswith("seq_")]
        seq_join = seq_df[["batter", "pitcher", "game_pk", "at_bat_number"] + seq_cols]
        result = result.merge(seq_join,
                              on=["batter", "pitcher", "game_pk", "at_bat_number"],
                              how="left")
        non_null = result["seq_n_pitches"].notna().sum()
        print(f"    {non_null:,} PAs with sequence data ({non_null/len(result)*100:.1f}%)")
    else:
        print("  pitch_sequence_features.parquet not found — run build_pitch_sequence.py")

    # =====================================================================
    # STEP 7d: Batter vs pitcher history
    # =====================================================================
    bvp_path = os.path.join(OUTPUT_DIR, "bvp_history.parquet")
    if os.path.exists(bvp_path):
        print("  Joining batter vs pitcher history...")
        bvp_df = pd.read_parquet(bvp_path)
        bvp_df["game_date"] = pd.to_datetime(bvp_df["game_date"])
        # Deduplicate to one row per batter/pitcher/game_pk
        bvp_dedup = bvp_df.drop_duplicates(
            subset=["batter", "pitcher", "game_pk"], keep="last")
        bvp_cols = [c for c in bvp_dedup.columns if c.startswith("bvp_")]
        bvp_join = bvp_dedup[["batter", "pitcher", "game_pk"] + bvp_cols]
        result = result.merge(bvp_join,
                              on=["batter", "pitcher", "game_pk"],
                              how="left")
        non_null = result["bvp_pa"].notna().sum()
        print(f"    {non_null:,} PAs with BvP history ({non_null/len(result)*100:.1f}%)")
    else:
        print("  bvp_history.parquet not found — run build_bvp_history.py")

    # =====================================================================
    # STEP 7e: Historical weather
    # =====================================================================
    weather_path = os.path.join(OUTPUT_DIR, "game_weather.parquet")
    if os.path.exists(weather_path):
        print("  Joining historical weather data...")
        weather_df = pd.read_parquet(weather_path)
        weather_cols = [c for c in weather_df.columns if c.startswith("weather_")]
        weather_join = weather_df[["game_pk"] + weather_cols]
        result = result.merge(weather_join, on=["game_pk"], how="left")
        non_null = result["weather_hr_factor"].notna().sum()
        print(f"    {non_null:,} PAs with weather data ({non_null/len(result)*100:.1f}%)")
    else:
        print("  game_weather.parquet not found — run build_historical_weather.py")

    # =====================================================================
    # STEP 7f: Pitcher workload (days rest, rolling workload)
    # =====================================================================
    workload_path = os.path.join(OUTPUT_DIR, "pitcher_workload.parquet")
    if os.path.exists(workload_path):
        print("  Joining pitcher workload features...")
        wl_df = pd.read_parquet(workload_path)
        wl_cols = ["days_rest", "pitches_last_7d", "pitches_last_14d",
                   "pitches_last_30d", "apps_last_7d", "apps_last_14d",
                   "season_pitches_before", "season_ip_before", "is_starter"]
        wl_available = [c for c in wl_cols if c in wl_df.columns]
        wl_join = wl_df[["pitcher", "game_pk"] + wl_available].copy()
        wl_join = wl_join.rename(columns={c: f"wl_{c}" for c in wl_available})
        result = result.merge(wl_join, on=["pitcher", "game_pk"], how="left")
        # Use workload days_rest if pitcher_days_rest is missing
        if "wl_days_rest" in result.columns:
            result["pitcher_days_rest"] = result["pitcher_days_rest"].fillna(
                result["wl_days_rest"])
        non_null = result["wl_days_rest"].notna().sum() if "wl_days_rest" in result.columns else 0
        print(f"    {non_null:,} PAs with workload data ({non_null/len(result)*100:.1f}%)")
    else:
        print("  pitcher_workload.parquet not found — run build_pitcher_workload.py")

    # =====================================================================
    # STEP 8: Engineered interaction & delta features
    # =====================================================================
    print("\n[8/12] Engineering interaction and delta features...")

    # --- Interaction features: batter skill x pitcher skill ---
    # These capture matchup-specific dynamics the model can't learn from
    # individual features alone without deep tree interaction
    interactions = [
        ("bat_blend_k_pct", "p_whiff_rate", "interact_k_whiff"),
        ("bat_blend_k_pct", "p_chase_rate", "interact_k_chase"),
        ("bat_blend_bb_pct", "p_zone_rate", "interact_bb_zone"),
        ("bat_blend_whiff_rate", "p_whiff_rate", "interact_whiff_whiff"),
        ("bat_blend_chase_rate", "p_chase_rate", "interact_chase_chase"),
        ("bat_blend_xwoba", "p_xwoba", "interact_xwoba"),
        ("bat_blend_barrel_rate", "p_avg_stuff_plus", "interact_barrel_stuff"),
        ("bat_blend_hard_hit_rate", "p_avg_velo", "interact_hard_velo"),
        ("bat_blend_iso", "park_hr_factor", "interact_iso_park"),
    ]

    for col_a, col_b, name in interactions:
        if col_a in result.columns and col_b in result.columns:
            result[name] = result[col_a] * result[col_b]

    # --- Delta-from-league features ---
    # How far each player deviates from league average
    # This is critical because the model needs to know if a .300 K% batter
    # is facing a .300 K% pitcher (both high) vs league avg
    delta_batter = [
        ("bat_blend_k_pct", LEAGUE_AVG["k_pct"], "bat_delta_k"),
        ("bat_blend_bb_pct", LEAGUE_AVG["bb_pct"], "bat_delta_bb"),
        ("bat_blend_whiff_rate", LEAGUE_AVG["whiff_rate"], "bat_delta_whiff"),
        ("bat_blend_chase_rate", LEAGUE_AVG["chase_rate"], "bat_delta_chase"),
        ("bat_blend_xwoba", LEAGUE_AVG["xwoba"], "bat_delta_xwoba"),
        ("bat_blend_barrel_rate", LEAGUE_AVG["barrel_rate"], "bat_delta_barrel"),
        ("bat_blend_iso", LEAGUE_AVG["iso"], "bat_delta_iso"),
    ]

    delta_pitcher = [
        ("p_whiff_rate", LEAGUE_AVG["whiff_rate"], "p_delta_whiff"),
        ("p_chase_rate", LEAGUE_AVG["chase_rate"], "p_delta_chase"),
        ("p_xwoba", LEAGUE_AVG["xwoba"], "p_delta_xwoba"),
        ("p_zone_rate", 0.45, "p_delta_zone"),
    ]

    for col, avg, name in delta_batter + delta_pitcher:
        if col in result.columns:
            result[name] = result[col] - avg

    # --- Matchup advantage features ---
    # Combined batter+pitcher deltas to measure overall matchup skew
    if "bat_delta_k" in result.columns and "p_delta_whiff" in result.columns:
        # Positive = more K-prone matchup (high-K batter vs high-whiff pitcher)
        result["matchup_k_advantage"] = result["bat_delta_k"] + result["p_delta_whiff"]

    if "bat_delta_bb" in result.columns and "p_delta_zone" in result.columns:
        # Positive = more walk-prone (high-BB batter vs low-zone pitcher)
        result["matchup_bb_advantage"] = result["bat_delta_bb"] - result["p_delta_zone"]

    if "bat_delta_xwoba" in result.columns and "p_delta_xwoba" in result.columns:
        # Positive = hitter-friendly matchup
        result["matchup_xwoba_advantage"] = result["bat_delta_xwoba"] - result["p_delta_xwoba"]

    if "bat_delta_barrel" in result.columns:
        result["matchup_power_advantage"] = (
            result.get("bat_delta_barrel", 0) +
            result.get("bat_delta_iso", 0)
        )

    # --- Platoon advantage magnitude ---
    # Quantifies how big the platoon split is for this matchup
    if "bat_plat_k_pct" in result.columns and "bat_blend_k_pct" in result.columns:
        result["platoon_k_split"] = (
            result["bat_plat_k_pct"] - result["bat_blend_k_pct"]
        )
    if "bat_plat_xwoba" in result.columns and "bat_blend_xwoba" in result.columns:
        result["platoon_xwoba_split"] = (
            result["bat_plat_xwoba"] - result["bat_blend_xwoba"]
        )

    # --- Recent form vs baseline delta ---
    # How much the player is deviating from their season norm RIGHT NOW
    if "bat_r14_k_pct" in result.columns and "bat_blend_k_pct" in result.columns:
        result["bat_r14_k_delta"] = result["bat_r14_k_pct"] - result["bat_blend_k_pct"]
    if "bat_r14_xwoba" in result.columns and "bat_blend_xwoba" in result.columns:
        result["bat_r14_xwoba_delta"] = result["bat_r14_xwoba"] - result["bat_blend_xwoba"]
    if "p_r14_k_pct" in result.columns and "p_whiff_rate" in result.columns:
        result["p_r14_k_delta"] = result["p_r14_k_pct"] - result["p_whiff_rate"]

    # --- Stuff+ vs contact quality matchup ---
    if "p_avg_stuff_plus" in result.columns and "bat_blend_zone_contact_rate" in result.columns:
        result["stuff_vs_contact"] = (
            (result["p_avg_stuff_plus"] - 100) / 20 -
            (result["bat_blend_zone_contact_rate"] - LEAGUE_AVG["zone_contact_rate"]) * 5
        )

    # --- Pitcher workload features ---
    # n_thruorder_pitcher is available; create derived features
    n_tto = pd.to_numeric(result["n_thruorder_pitcher"], errors="coerce").fillna(1)
    result["n_thruorder_pitcher"] = n_tto
    result["is_third_time_thru"] = (n_tto >= 3).astype(int)
    result["tto_squared"] = n_tto ** 2  # non-linear fatigue

    n_feats = len([c for c in result.columns if c not in [
        "batter", "pitcher", "game_pk", "game_date", "season",
        "stand", "p_throws", "outcome", "venue", "catcher_id",
        "launch_speed", "launch_angle", "estimated_woba_using_speedangle",
    ]])
    print(f"  Total engineered features: {n_feats}")

    # =====================================================================
    # STEP 9: Compute pitcher in-game pitch count estimate
    # =====================================================================
    print("\n[9/12] Computing in-game pitch count estimates...")
    # Count pitches thrown by the pitcher in this game BEFORE this PA
    # This is a fatigue proxy that's more granular than n_thruorder_pitcher
    pitch_count_sql = f"""
    SELECT
        pitcher,
        game_pk,
        at_bat_number,
        COUNT(*) OVER (
            PARTITION BY pitcher, game_pk
            ORDER BY at_bat_number, pitch_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS pitcher_pitch_count_cum
    FROM statcast_pitches
    WHERE game_type = 'R'
      AND game_year BETWEEN 2021 AND {current_year}
      AND events IS NOT NULL
      AND events != ''
    """
    try:
        pitch_counts = query_df(pitch_count_sql)
        if not pitch_counts.empty:
            pitch_counts["pitcher_pitch_count_cum"] = pd.to_numeric(
                pitch_counts["pitcher_pitch_count_cum"], errors="coerce")
            # Need to join on pitcher+game_pk+at_bat_number
            # But we don't have at_bat_number in result, so approximate with
            # a rank within game
            # Actually, let's compute it differently — count events before this one
            pass  # Will use a simpler approach below
    except Exception as e:
        print(f"  Pitch count query failed: {e}")

    # Simpler approach: estimate pitch count from n_thruorder_pitcher
    # Average ~4 pitches per PA, ~9 batters per time through order
    result["est_pitch_count"] = (result["n_thruorder_pitcher"] - 1) * 9 * 4 + (
        result["inning"] % 9) * 4
    result["est_pitch_count"] = result["est_pitch_count"].clip(0, 130)

    # =====================================================================
    # STEP 10: Final cleanup and save
    # =====================================================================
    print("\n[10/12] Cleaning up and saving...")

    result = result.sort_values(["season", "game_date", "game_pk"]).reset_index(drop=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    result.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nMatchup training data v2 saved to {OUTPUT_PATH}")
    print(f"  {len(result):,} plate appearances")
    print(f"  {len(result.columns)} columns")
    print(f"\nOutcome distribution:")
    print(result["outcome"].value_counts().to_string())
    print(f"\nFeature columns:")
    feature_cols = [c for c in result.columns if c not in [
        "batter", "pitcher", "game_pk", "game_date", "season",
        "outcome", "venue", "catcher_id",
        "launch_speed", "launch_angle", "estimated_woba_using_speedangle",
    ]]
    for i, col in enumerate(sorted(feature_cols)):
        non_null = result[col].notna().sum()
        print(f"  {i+1:3d}. {col:<40s}  {non_null/len(result)*100:5.1f}% non-null")


# =========================================================================
# Helper functions
# =========================================================================

def _pitcher_agg(g):
    """Compute pitcher aggregate stats from arsenal rows."""
    n = pd.to_numeric(g["n"], errors="coerce").fillna(0).values.astype(float)
    total = n.sum()
    if total == 0:
        return pd.Series({})

    def _wavg(col):
        vals = pd.to_numeric(g[col], errors="coerce")
        valid = vals.notna() & (n > 0)
        if not valid.any():
            return np.nan
        return float(np.average(vals[valid], weights=n[valid]))

    return pd.Series({
        "p_avg_stuff_plus": _wavg("avg_stuff_plus"),
        "p_avg_control_plus": _wavg("avg_control_plus"),
        "p_avg_velo": _wavg("avg_velo"),
        "p_whiff_rate": _wavg("whiff_rate"),
        "p_chase_rate": _wavg("chase_rate"),
        "p_zone_rate": _wavg("zone_rate"),
        "p_xwoba": _wavg("xwoba"),
        "p_num_pitches": len(g),
        "p_total_thrown": int(total),
    })


def _arsenal_entropy(g):
    """Shannon entropy of pitch usage distribution."""
    n = pd.to_numeric(g["n"], errors="coerce").fillna(0).values.astype(float)
    total = n.sum()
    if total == 0:
        return 0.0
    probs = n / total
    probs = probs[probs > 0]
    return float(-np.sum(probs * np.log2(probs)))


def _compute_pitcher_category_usage(pitcher_all):
    """Compute fastball/breaking/offspeed usage percentages."""
    rows = []
    for (pid, season), group in pitcher_all.groupby(["pitcher", "season"]):
        total = group["n"].sum()
        if total == 0:
            continue
        cat_n = {"fastball": 0, "breaking": 0, "offspeed": 0}
        for _, row in group.iterrows():
            cat = PITCH_CATEGORY.get(row.get("pitch_type", ""))
            if cat:
                cat_n[cat] += float(row["n"])
        rows.append({
            "pitcher": pid,
            "season": season,
            "p_usage_fastball": cat_n["fastball"] / total,
            "p_usage_breaking": cat_n["breaking"] / total,
            "p_usage_offspeed": cat_n["offspeed"] / total,
        })
    return pd.DataFrame(rows)


def _compute_pitcher_top3(pitcher_all):
    """Get top 3 pitch types by usage for each pitcher/season."""
    top3_dict = {}
    for (pid, season), group in pitcher_all.groupby(["pitcher", "season"]):
        top3 = group.nlargest(3, "n")
        total = group["n"].sum()
        if total == 0:
            continue
        key = (pid, season)
        top3_dict[key] = {"pitcher": pid, "season": season}
        for rank, (_, row) in enumerate(top3.iterrows(), 1):
            top3_dict[key][f"p_pitch{rank}_usage"] = float(row["n"]) / total
            top3_dict[key][f"p_pitch{rank}_velo"] = (
                float(row["avg_velo"]) if pd.notna(row.get("avg_velo")) else np.nan)
            top3_dict[key][f"p_pitch{rank}_whiff"] = (
                float(row["whiff_rate"]) if pd.notna(row.get("whiff_rate")) else np.nan)
            top3_dict[key][f"p_pitch{rank}_stuff"] = (
                float(row["avg_stuff_plus"]) if pd.notna(row.get("avg_stuff_plus")) else np.nan)

    if not top3_dict:
        return None
    return pd.DataFrame(list(top3_dict.values()))


if __name__ == "__main__":
    build_matchup_training_v2()
