#!/usr/bin/env python3
"""
Run the complete v2 model pipeline:
1. Build prerequisite feature datasets (spray, sequence, BvP, weather, workload)
2. Build enhanced training data (join all features)
3. Train model with feature importance analysis
4. Validate at PA, inning, and game levels

Usage:
    python scripts/game_prediction/run_v2_pipeline.py
    python scripts/game_prediction/run_v2_pipeline.py --skip-prereqs  # reuse existing feature parquets
    python scripts/game_prediction/run_v2_pipeline.py --skip-build    # reuse existing training parquet
    python scripts/game_prediction/run_v2_pipeline.py --validate-only # just run validation
"""

import sys
import time
import argparse


def main():
    parser = argparse.ArgumentParser(description="V2 model pipeline")
    parser.add_argument("--skip-prereqs", action="store_true",
                        help="Skip building prerequisite feature datasets")
    parser.add_argument("--skip-build", action="store_true",
                        help="Skip data build, reuse existing training parquet")
    parser.add_argument("--validate-only", action="store_true",
                        help="Only run validation (model must already exist)")
    args = parser.parse_args()

    total_start = time.time()

    if not args.validate_only:
        # Phase 0: Build prerequisite feature datasets
        if not args.skip_prereqs and not args.skip_build:
            print("\n" + "=" * 70)
            print("PHASE 0: Building prerequisite feature datasets")
            print("=" * 70)

            print("\n--- Batter profiles ---")
            from build_batter_profiles import build_batter_profiles
            build_batter_profiles()

            print("\n--- Pitcher arsenal ---")
            from build_pitcher_arsenal import build_pitcher_arsenal
            build_pitcher_arsenal()

            print("\n--- Recent form (14-day rolling) ---")
            from build_recent_form import build_batter_recent_form, build_pitcher_recent_form
            build_batter_recent_form()
            build_pitcher_recent_form()

            print("\n--- Pitcher workload ---")
            from build_pitcher_workload import build_pitcher_workload
            build_pitcher_workload()

            print("\n--- Spray angle profiles ---")
            from build_spray_profiles import build_spray_profiles
            build_spray_profiles()

            print("\n--- Pitch sequence context ---")
            from build_pitch_sequence import build_pitch_sequence_features
            build_pitch_sequence_features()

            print("\n--- Batter vs pitcher history ---")
            from build_bvp_history import build_bvp_history
            build_bvp_history()

            print("\n--- Historical weather ---")
            from build_historical_weather import build_historical_weather
            build_historical_weather()

        # Phase 1: Build training data
        if not args.skip_build:
            print("\n" + "=" * 70)
            print("PHASE 1: Building enhanced training data")
            print("=" * 70)
            from build_matchup_training_v2 import build_matchup_training_v2
            build_matchup_training_v2()

        # Phase 2: Train
        print("\n" + "=" * 70)
        print("PHASE 2: Training model with feature analysis")
        print("=" * 70)
        from train_matchup_model_v2 import train_matchup_model_v2
        train_matchup_model_v2()

    # Phase 3: Validate
    print("\n" + "=" * 70)
    print("PHASE 3: Multi-level validation")
    print("=" * 70)
    from validate_model_v2 import validate_model
    validate_model()

    elapsed = time.time() - total_start
    print(f"\n\nTotal pipeline time: {elapsed:.0f}s ({elapsed/60:.1f} min)")


if __name__ == "__main__":
    main()
