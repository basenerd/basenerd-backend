#!/usr/bin/env python3
"""
Lightweight daily rebuild of player profile parquets.

Runs after update_statcast_pitches so batter/pitcher aggregates
reflect yesterday's games. The web service loads these at startup,
and Render's max-requests setting causes periodic worker restarts
that pick up the fresh files.

Usage:
  python scripts/daily_rebuild_profiles.py
"""

import os
import sys
import time
import traceback

# Allow imports from scripts/game_prediction/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "game_prediction"))


STEPS = [
    ("Batter Profiles", "build_batter_profiles", "build_batter_profiles"),
    ("Pitcher Arsenal", "build_pitcher_arsenal", "build_pitcher_arsenal"),
    ("Batter Pitch-Type Profiles", "build_batter_pitch_type_profiles", "build_batter_pitch_type_profiles"),
]


def main():
    total_start = time.time()
    results = {}

    for step_name, module_name, func_name in STEPS:
        print(f"\n{'='*50}")
        print(f"STEP: {step_name}")
        print(f"{'='*50}")

        start = time.time()
        try:
            module = __import__(module_name)
            func = getattr(module, func_name)
            func()
            elapsed = time.time() - start
            print(f"  Completed in {elapsed:.1f}s")
            results[step_name] = "success"
        except Exception as e:
            elapsed = time.time() - start
            print(f"  FAILED after {elapsed:.1f}s: {e}")
            traceback.print_exc()
            results[step_name] = f"failed: {e}"

    total_elapsed = time.time() - total_start
    print(f"\n{'='*50}")
    print(f"DAILY REBUILD COMPLETE ({total_elapsed:.0f}s)")
    print(f"{'='*50}")
    for step_name, status in results.items():
        icon = "OK" if status == "success" else "FAIL"
        print(f"  [{icon}] {step_name}")

    if any("failed" in str(v) for v in results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
