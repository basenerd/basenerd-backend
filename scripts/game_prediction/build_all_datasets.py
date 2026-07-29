#!/usr/bin/env python3
"""
Master script: builds all game prediction datasets in dependency order.

Usage:
  python scripts/game_prediction/build_all_datasets.py [--skip-api]

Execution order (respects dependencies):
  1. Park factors         (from DB)
  2. Pitcher arsenal      (from DB)
  3. Batter profiles      (from DB)
  4. Pitcher workload     (from DB)
  5. Catcher framing      (from DB)
  6. Defensive metrics    (from DB)
  7. Pitch selection data  (from DB)
  8. Game outcomes         (from API) -- skip with --skip-api
  9. Umpire metrics        (from DB + game_outcomes)
  10. Matchup training     (from DB + datasets 1-3)

After datasets are built, train models with:
  python scripts/game_prediction/train_matchup_model.py
  python scripts/game_prediction/train_pitch_selection_model.py
"""

import os
import sys
import time
import argparse
import traceback


def run_step(name, module_name):
    """Import and run a dataset builder."""
    print(f"\n{'='*60}")
    print(f"STEP: {name}")
    print(f"{'='*60}")

    start = time.time()
    try:
        # Dynamic import
        module = __import__(module_name)
        # Each module has a main function matching build_*
        func_name = module_name.replace("build_", "build_")
        func = getattr(module, func_name)
        func()
        elapsed = time.time() - start
        print(f"\n  Completed in {elapsed:.1f}s")
        return True
    except Exception as e:
        elapsed = time.time() - start
        print(f"\n  FAILED after {elapsed:.1f}s: {e}")
        traceback.print_exc()
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-api", action="store_true",
                        help="Skip API-dependent steps (game outcomes)")
    parser.add_argument("--only", type=str, default=None,
                        help="Run only a specific step (e.g. 'park_factors')")
    args = parser.parse_args()

    # Ensure we're in the right directory for imports
    sys.path.insert(0, os.path.dirname(__file__))

    steps = [
        ("Park Factors", "build_park_factors", "build_park_factors", False),
        ("Pitcher Arsenal Profiles", "build_pitcher_arsenal", "build_pitcher_arsenal", False),
        ("Batter Profiles", "build_batter_profiles", "build_batter_profiles", False),
        ("Pitcher Workload", "build_pitcher_workload", "build_pitcher_workload", False),
        ("Catcher Framing", "build_catcher_framing", "build_catcher_framing", False),
        ("Defensive Metrics", "build_defensive_metrics", "build_defensive_metrics", False),
        ("Pitch Selection Data", "build_pitch_selection", "build_pitch_selection_data", False),
        ("Game Outcomes (API)", "build_game_outcomes", "build_game_outcomes", True),
        ("Umpire Metrics", "build_umpire_metrics", "build_umpire_metrics", False),
        ("Matchup Training Data", "build_matchup_training", "build_matchup_training", False),
    ]

    results = {}
    total_start = time.time()

    for step_name, module_name, func_name, requires_api in steps:
        if args.only and args.only not in module_name:
            continue

        if requires_api and args.skip_api:
            print(f"\n  SKIPPED: {step_name} (--skip-api)")
            results[step_name] = "skipped"
            continue

        print(f"\n{'='*60}")
        print(f"STEP: {step_name}")
        print(f"{'='*60}")

        start = time.time()
        try:
            module = __import__(module_name)
            func = getattr(module, func_name)
            func()
            elapsed = time.time() - start
            print(f"\n  Completed in {elapsed:.1f}s")
            results[step_name] = "success"
        except Exception as e:
            elapsed = time.time() - start
            print(f"\n  FAILED after {elapsed:.1f}s: {e}")
            traceback.print_exc()
            results[step_name] = f"failed: {e}"

    total_elapsed = time.time() - total_start

    print(f"\n{'='*60}")
    print(f"BUILD COMPLETE ({total_elapsed:.0f}s total)")
    print(f"{'='*60}")
    for step_name, status in results.items():
        icon = "OK" if status == "success" else "SKIP" if status == "skipped" else "FAIL"
        print(f"  [{icon}] {step_name}")

    # Check output files
    data_dir = os.path.join(os.path.dirname(__file__), "..", "..", "data")
    if os.path.exists(data_dir):
        print(f"\nDataset files in {data_dir}:")
        for f in sorted(os.listdir(data_dir)):
            if f.endswith(".parquet"):
                size = os.path.getsize(os.path.join(data_dir, f))
                print(f"  {f}: {size / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    main()
