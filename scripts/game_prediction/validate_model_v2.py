#!/usr/bin/env python3
"""
Multi-level validation for matchup model v2.

Tests predictions at three levels:
1. PLATE APPEARANCE level — calibration, discrimination, outcome rate accuracy
2. INNING level — aggregate K/BB/hit rates per half-inning match real distributions
3. GAME level — total Ks, runs, hits per game match real distributions

Uses the test season holdout to validate with real game data.

This script answers: "If I simulate 1000 games with this model,
do the results look like real baseball?"
"""

import os
import sys
import json
import joblib
import pandas as pd
import numpy as np
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import query_df

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
MODEL_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "models")
MODEL_PATH = os.path.join(MODEL_DIR, "matchup_model_v2.joblib")
META_PATH = os.path.join(MODEL_DIR, "matchup_model_v2_meta.json")
MATCHUP_DATA_PATH = os.path.join(OUTPUT_DIR, "matchup_train_v2.parquet")

# Known league-average benchmarks (2021-2025)
BENCHMARKS = {
    "pa_per_game": 38.5,          # ~38-39 PA per team per game (both teams)
    "k_per_game": 8.5,            # ~8-9 Ks per team per game
    "bb_per_game": 3.2,           # ~3-3.5 BB per team per game
    "hits_per_game": 8.5,         # ~8-9 hits per team per game
    "hr_per_game": 1.15,          # ~1.1-1.2 HR per team per game
    "runs_per_game": 4.5,         # ~4.3-4.7 runs per team per game
    "k_pct": 0.223,              # ~22-23% K rate
    "bb_pct": 0.083,             # ~8-8.5% BB rate
    "hit_pct": 0.235,            # ~23-24% hit rate (per PA)
    "hr_pct": 0.033,             # ~3.2-3.5% HR rate (per PA)
    "k_per_9_innings": 8.5,      # Ks per 9 innings
}


def validate_model():
    print("=" * 70)
    print("MODEL V2 MULTI-LEVEL VALIDATION")
    print("=" * 70)

    # Load model and data
    print("\nLoading model and test data...")
    model = joblib.load(MODEL_PATH)
    with open(META_PATH) as f:
        meta = json.load(f)

    df = pd.read_parquet(MATCHUP_DATA_PATH)
    class_names = meta["classes"]
    feature_names = meta["numeric_features"] + meta["categorical_features"]

    # Use test season
    test_season = meta["test_season"]
    test_df = df[df["season"] == test_season].copy()
    print(f"  Test season: {test_season}")
    print(f"  Test PAs: {len(test_df):,}")

    # Prepare features
    for col in meta["numeric_features"]:
        if col in test_df.columns:
            test_df[col] = pd.to_numeric(test_df[col], errors="coerce").astype("float32")
    for col in meta["categorical_features"]:
        if col in test_df.columns:
            test_df[col] = test_df[col].astype("category").cat.codes.astype("float32")

    X_test = test_df[[f for f in feature_names if f in test_df.columns]].copy()
    # Fill missing features with 0
    for f in feature_names:
        if f not in X_test.columns:
            X_test[f] = 0.0

    X_test = X_test[feature_names]

    # Get predictions
    print("  Generating predictions...")
    y_pred_proba = model.predict_proba(X_test)
    pred_df = pd.DataFrame(y_pred_proba, columns=class_names, index=test_df.index)

    test_df = test_df.join(pred_df, rsuffix="_pred")

    # =====================================================================
    # LEVEL 1: Plate Appearance Validation
    # =====================================================================
    print("\n" + "=" * 70)
    print("LEVEL 1: PLATE APPEARANCE VALIDATION")
    print("=" * 70)

    actual_outcomes = test_df["outcome"].value_counts(normalize=True)
    pred_outcomes = pred_df.mean()

    print(f"\n  Overall outcome rates:")
    print(f"  {'Outcome':<8s}  {'Actual':>8s}  {'Predicted':>10s}  {'Diff':>8s}  {'Status':>8s}")
    print(f"  {'-'*8}  {'-'*8}  {'-'*10}  {'-'*8}  {'-'*8}")

    all_pass = True
    for cls in class_names:
        actual = actual_outcomes.get(cls, 0)
        predicted = pred_outcomes.get(cls, 0)
        diff = predicted - actual
        pct_diff = abs(diff / actual * 100) if actual > 0 else 0
        status = "OK" if pct_diff < 5 else ("WARN" if pct_diff < 10 else "FAIL")
        if status == "FAIL":
            all_pass = False
        print(f"  {cls:<8s}  {actual:>7.3f}  {predicted:>9.3f}  {diff:>+7.3f}  {status:>8s}")

    # K rate deep dive
    print(f"\n  --- Strikeout Rate Deep Dive ---")
    k_actual = actual_outcomes.get("K", 0)
    k_pred = pred_outcomes.get("K", 0)
    print(f"  Actual K%: {k_actual:.3f} ({k_actual*100:.1f}%)")
    print(f"  Predicted K%: {k_pred:.3f} ({k_pred*100:.1f}%)")
    print(f"  MLB benchmark: {BENCHMARKS['k_pct']:.3f}")

    # K rate by pitcher quality tier
    if "p_whiff_rate" in test_df.columns:
        print(f"\n  K rate by pitcher whiff tier:")
        test_df["p_whiff_tier"] = pd.qcut(
            test_df["p_whiff_rate"].fillna(0.245), q=5,
            labels=["Low", "Below Avg", "Average", "Above Avg", "Elite"],
            duplicates="drop"
        )
        for tier in ["Low", "Below Avg", "Average", "Above Avg", "Elite"]:
            tier_mask = test_df["p_whiff_tier"] == tier
            if tier_mask.sum() == 0:
                continue
            tier_actual = (test_df.loc[tier_mask, "outcome"] == "K").mean()
            tier_pred = test_df.loc[tier_mask, "K"].mean() if "K" in test_df.columns else pred_df.loc[tier_mask, "K"].mean()
            print(f"    {tier:<12s}: actual={tier_actual:.3f}  pred={tier_pred:.3f}  "
                  f"diff={tier_pred-tier_actual:+.3f}  n={tier_mask.sum():,}")

    # K rate by batter K% tier
    k_col = "bat_blend_k_pct" if "bat_blend_k_pct" in test_df.columns else "bat_k_pct"
    if k_col in test_df.columns:
        print(f"\n  K rate by batter K% tier:")
        test_df["bat_k_tier"] = pd.qcut(
            test_df[k_col].fillna(0.223), q=5,
            labels=["Low K", "Below Avg", "Average", "Above Avg", "High K"],
            duplicates="drop"
        )
        for tier in ["Low K", "Below Avg", "Average", "Above Avg", "High K"]:
            tier_mask = test_df["bat_k_tier"] == tier
            if tier_mask.sum() == 0:
                continue
            tier_actual = (test_df.loc[tier_mask, "outcome"] == "K").mean()
            tier_pred = pred_df.loc[tier_mask, "K"].mean()
            print(f"    {tier:<12s}: actual={tier_actual:.3f}  pred={tier_pred:.3f}  "
                  f"diff={tier_pred-tier_actual:+.3f}  n={tier_mask.sum():,}")

    # K rate by count
    if "strikes" in test_df.columns and "balls" in test_df.columns:
        print(f"\n  K rate by count:")
        for strikes in [0, 1, 2]:
            for balls in [0, 1, 2, 3]:
                mask = (test_df["strikes"] == strikes) & (test_df["balls"] == balls)
                if mask.sum() < 100:
                    continue
                count_actual = (test_df.loc[mask, "outcome"] == "K").mean()
                count_pred = pred_df.loc[mask, "K"].mean()
                print(f"    {balls}-{strikes}: actual={count_actual:.3f}  "
                      f"pred={count_pred:.3f}  diff={count_pred-count_actual:+.3f}  "
                      f"n={mask.sum():,}")

    # =====================================================================
    # LEVEL 2: Inning-Level Validation
    # =====================================================================
    print("\n" + "=" * 70)
    print("LEVEL 2: INNING-LEVEL VALIDATION")
    print("=" * 70)

    if "game_pk" in test_df.columns and "inning" in test_df.columns:
        # Group by game + inning half
        inning_col = "inning_topbot" if "inning_topbot" in test_df.columns else None

        # Aggregate actual outcomes per half-inning
        group_cols = ["game_pk", "inning"]
        inning_groups = test_df.groupby(group_cols)

        inning_stats = []
        for (game_pk, inning), group in inning_groups:
            n_pa = len(group)
            n_k = (group["outcome"] == "K").sum()
            n_bb = (group["outcome"].isin(["BB", "IBB"])).sum()
            n_hits = (group["outcome"].isin(["1B", "2B", "3B", "HR"])).sum()
            n_hr = (group["outcome"] == "HR").sum()

            # Predicted (sum of probabilities)
            pred_k = pred_df.loc[group.index, "K"].sum()
            pred_bb = pred_df.loc[group.index, ["BB", "IBB"]].sum().sum()
            pred_hits = pred_df.loc[group.index, ["1B", "2B", "3B", "HR"]].sum().sum()
            pred_hr = pred_df.loc[group.index, "HR"].sum()

            inning_stats.append({
                "game_pk": game_pk, "inning": inning,
                "n_pa": n_pa,
                "actual_k": n_k, "pred_k": pred_k,
                "actual_bb": n_bb, "pred_bb": pred_bb,
                "actual_hits": n_hits, "pred_hits": pred_hits,
                "actual_hr": n_hr, "pred_hr": pred_hr,
            })

        inning_df = pd.DataFrame(inning_stats)

        print(f"\n  Per-inning stats (aggregated across {len(inning_df):,} innings):")
        for stat in ["k", "bb", "hits", "hr"]:
            actual_mean = inning_df[f"actual_{stat}"].mean()
            pred_mean = inning_df[f"pred_{stat}"].mean()
            actual_std = inning_df[f"actual_{stat}"].std()
            pred_std = inning_df[f"pred_{stat}"].std()
            print(f"  {stat.upper():<6s} per inning:  actual={actual_mean:.2f} (±{actual_std:.2f})  "
                  f"pred={pred_mean:.2f} (±{pred_std:.2f})")

        # Check by inning number
        print(f"\n  K rate by inning number:")
        for inn in range(1, 10):
            mask = inning_df["inning"] == inn
            if mask.sum() < 50:
                continue
            actual_k_rate = inning_df.loc[mask, "actual_k"].sum() / inning_df.loc[mask, "n_pa"].sum()
            pred_k_rate = inning_df.loc[mask, "pred_k"].sum() / inning_df.loc[mask, "n_pa"].sum()
            print(f"    Inning {inn}: actual={actual_k_rate:.3f}  pred={pred_k_rate:.3f}  "
                  f"diff={pred_k_rate-actual_k_rate:+.3f}")

    # =====================================================================
    # LEVEL 3: Game-Level Validation
    # =====================================================================
    print("\n" + "=" * 70)
    print("LEVEL 3: GAME-LEVEL VALIDATION")
    print("=" * 70)

    if "game_pk" in test_df.columns:
        game_groups = test_df.groupby("game_pk")

        game_stats = []
        for game_pk, group in game_groups:
            n_pa = len(group)

            # Actual per-game stats
            actual = {
                "n_pa": n_pa,
                "k": (group["outcome"] == "K").sum(),
                "bb": (group["outcome"].isin(["BB", "IBB"])).sum(),
                "hits": (group["outcome"].isin(["1B", "2B", "3B", "HR"])).sum(),
                "hr": (group["outcome"] == "HR").sum(),
                "singles": (group["outcome"] == "1B").sum(),
                "doubles": (group["outcome"] == "2B").sum(),
                "triples": (group["outcome"] == "3B").sum(),
            }

            # Predicted (sum of PA-level probabilities for the whole game)
            predicted = {
                "k": pred_df.loc[group.index, "K"].sum(),
                "bb": pred_df.loc[group.index, ["BB", "IBB"]].sum().sum(),
                "hits": pred_df.loc[group.index, ["1B", "2B", "3B", "HR"]].sum().sum(),
                "hr": pred_df.loc[group.index, "HR"].sum(),
                "singles": pred_df.loc[group.index, "1B"].sum(),
                "doubles": pred_df.loc[group.index, "2B"].sum(),
                "triples": pred_df.loc[group.index, "3B"].sum(),
            }

            game_stats.append({"game_pk": game_pk, **{f"actual_{k}": v for k, v in actual.items()},
                               **{f"pred_{k}": v for k, v in predicted.items()}})

        game_df = pd.DataFrame(game_stats)
        n_games = len(game_df)

        print(f"\n  {n_games:,} games in test set")

        print(f"\n  Per-GAME totals (both teams combined):")
        print(f"  {'Stat':<12s}  {'Actual Mean':>12s}  {'Pred Mean':>12s}  "
              f"{'Actual Std':>12s}  {'Pred Std':>12s}  {'Benchmark':>10s}  {'Status':>8s}")
        print(f"  {'-'*12}  {'-'*12}  {'-'*12}  {'-'*12}  {'-'*12}  {'-'*10}  {'-'*8}")

        game_results = {}
        for stat in ["k", "bb", "hits", "hr"]:
            actual_mean = game_df[f"actual_{stat}"].mean()
            pred_mean = game_df[f"pred_{stat}"].mean()
            actual_std = game_df[f"actual_{stat}"].std()
            pred_std = game_df[f"pred_{stat}"].std()

            benchmark_key = f"{stat}_per_game" if stat != "hits" else "hits_per_game"
            # These benchmarks are per-team, game stats are both teams
            benchmark = BENCHMARKS.get(benchmark_key, 0) * 2

            pct_diff = abs(pred_mean - actual_mean) / actual_mean * 100
            status = "OK" if pct_diff < 5 else ("WARN" if pct_diff < 10 else "FAIL")

            print(f"  {stat.upper():<12s}  {actual_mean:>12.2f}  {pred_mean:>12.2f}  "
                  f"{actual_std:>12.2f}  {pred_std:>12.2f}  {benchmark:>10.1f}  {status:>8s}")

            game_results[stat] = {
                "actual_mean": round(actual_mean, 2),
                "pred_mean": round(pred_mean, 2),
                "actual_std": round(actual_std, 2),
                "benchmark": round(benchmark, 1),
            }

        # Correlation between predicted and actual game totals
        print(f"\n  Predicted vs Actual game-level correlation:")
        for stat in ["k", "bb", "hits", "hr"]:
            corr = game_df[f"actual_{stat}"].corr(game_df[f"pred_{stat}"])
            print(f"    {stat.upper()}: r = {corr:.3f}")

        # Distribution check: do predicted game K totals match actual distribution?
        print(f"\n  Game K total distribution (both teams):")
        for k_range, label in [(range(0, 8), "0-7 Ks"), (range(8, 12), "8-11 Ks"),
                                (range(12, 16), "12-15 Ks"), (range(16, 25), "16+ Ks")]:
            actual_pct = game_df["actual_k"].apply(lambda x: x in k_range).mean()
            # For predicted, round to nearest integer
            pred_pct = game_df["pred_k"].round(0).apply(lambda x: int(x) in k_range).mean()
            print(f"    {label:<10s}: actual={actual_pct*100:.1f}%  pred={pred_pct*100:.1f}%")

        # ---- Per-team K validation (half-game) ----
        # This is the most important check for the strikeout projection issue
        print(f"\n  --- PER-TEAM Strikeout Validation ---")
        actual_k_per_team = game_df["actual_k"].mean() / 2
        pred_k_per_team = game_df["pred_k"].mean() / 2
        benchmark_k = BENCHMARKS["k_per_game"]
        print(f"  Actual Ks per team per game: {actual_k_per_team:.2f}")
        print(f"  Predicted Ks per team per game: {pred_k_per_team:.2f}")
        print(f"  MLB benchmark: {benchmark_k:.1f}")
        k_error = abs(pred_k_per_team - actual_k_per_team) / actual_k_per_team * 100
        print(f"  Error vs actual: {k_error:.1f}%  "
              f"{'PASS' if k_error < 5 else 'FAIL'}")

        # ---- Runs estimation (approximate from hits/HR/BB) ----
        print(f"\n  --- Runs Estimation Check ---")
        # Simple run estimator: HR * 1.4 + (hits - HR) * 0.45 + BB * 0.3
        game_df["est_actual_runs"] = (
            game_df["actual_hr"] * 1.4 +
            (game_df["actual_hits"] - game_df["actual_hr"]) * 0.45 +
            game_df["actual_bb"] * 0.3
        )
        game_df["est_pred_runs"] = (
            game_df["pred_hr"] * 1.4 +
            (game_df["pred_hits"] - game_df["pred_hr"]) * 0.45 +
            game_df["pred_bb"] * 0.3
        )
        actual_rpg = game_df["est_actual_runs"].mean() / 2
        pred_rpg = game_df["est_pred_runs"].mean() / 2
        print(f"  Est. actual runs/team/game: {actual_rpg:.2f}")
        print(f"  Est. predicted runs/team/game: {pred_rpg:.2f}")
        print(f"  MLB benchmark: {BENCHMARKS['runs_per_game']:.1f}")

    # =====================================================================
    # EARLY SEASON SIMULATION
    # =====================================================================
    print("\n" + "=" * 70)
    print("EARLY SEASON CHECK (day_of_season <= 20)")
    print("=" * 70)

    if "day_of_season" in test_df.columns:
        early_mask = test_df["day_of_season"] <= 20
        late_mask = test_df["day_of_season"] > 100

        if early_mask.sum() > 0:
            early_actual_k = (test_df.loc[early_mask, "outcome"] == "K").mean()
            early_pred_k = pred_df.loc[early_mask, "K"].mean()
            print(f"\n  Early season (first 20 days):")
            print(f"    N PAs: {early_mask.sum():,}")
            print(f"    Actual K%: {early_actual_k:.3f}")
            print(f"    Predicted K%: {early_pred_k:.3f}")
            print(f"    Diff: {early_pred_k - early_actual_k:+.3f}")

        if late_mask.sum() > 0:
            late_actual_k = (test_df.loc[late_mask, "outcome"] == "K").mean()
            late_pred_k = pred_df.loc[late_mask, "K"].mean()
            print(f"\n  Late season (after day 100):")
            print(f"    N PAs: {late_mask.sum():,}")
            print(f"    Actual K%: {late_actual_k:.3f}")
            print(f"    Predicted K%: {late_pred_k:.3f}")
            print(f"    Diff: {late_pred_k - late_actual_k:+.3f}")

        if early_mask.sum() > 0 and late_mask.sum() > 0:
            early_diff = abs(early_pred_k - early_actual_k)
            late_diff = abs(late_pred_k - late_actual_k)
            if early_diff > late_diff * 2:
                print(f"\n  WARNING: Early season predictions are {early_diff/late_diff:.1f}x "
                      f"worse than late season. This suggests the model still "
                      f"struggles with small-sample early-season data.")
            else:
                print(f"\n  Early season accuracy is within acceptable range "
                      f"({early_diff:.3f} vs {late_diff:.3f} late season)")

    # =====================================================================
    # SAMPLE SIZE SENSITIVITY CHECK
    # =====================================================================
    print("\n" + "=" * 70)
    print("SAMPLE SIZE SENSITIVITY")
    print("=" * 70)

    if "bat_season_pa" in test_df.columns:
        print(f"\n  K rate accuracy by batter sample size:")
        pa_bins = [(0, 30, "<30 PA"), (30, 100, "30-100 PA"),
                   (100, 300, "100-300 PA"), (300, 700, "300+ PA")]
        for lo, hi, label in pa_bins:
            mask = (test_df["bat_season_pa"] >= lo) & (test_df["bat_season_pa"] < hi)
            if mask.sum() < 100:
                continue
            actual_k = (test_df.loc[mask, "outcome"] == "K").mean()
            pred_k = pred_df.loc[mask, "K"].mean()
            print(f"    {label:<12s}: actual={actual_k:.3f}  pred={pred_k:.3f}  "
                  f"diff={pred_k-actual_k:+.3f}  n={mask.sum():,}")

    # =====================================================================
    # SUMMARY
    # =====================================================================
    print("\n" + "=" * 70)
    print("VALIDATION SUMMARY")
    print("=" * 70)
    print(f"\n  This validation used {len(test_df):,} PAs from {test_season}")
    if "game_pk" in test_df.columns:
        print(f"  across {test_df['game_pk'].nunique():,} games")
    print(f"\n  Key metrics to check:")
    print(f"    1. PA-level K rate should be within 2% of actual")
    print(f"    2. Game-level K totals should match ±5%")
    print(f"    3. Early season should not be significantly worse than late season")
    print(f"    4. Low-PA batters should not have extreme prediction bias")
    print(f"\n  If any checks FAIL, investigate the feature importance analysis")
    print(f"  to determine which features are driving the error.")


if __name__ == "__main__":
    validate_model()
