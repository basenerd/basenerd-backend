#!/usr/bin/env python3
"""Verify the profile DB hand-off: tables exist, and services read from DB."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

from services.profile_store import load_profile

TABLES = {
    "profile_batter": "batter_profiles.parquet",
    "profile_batter_pitch_type": "batter_pitch_type_profiles.parquet",
    "profile_pitcher_arsenal": "pitcher_arsenal.parquet",
}
DATA = os.path.join(os.path.dirname(__file__), "..", "data")

print("=== load_profile (should say 'from DB') ===")
for table, pq in TABLES.items():
    df = load_profile(table, os.path.join(DATA, pq))
    print(f"  {table}: {len(df):,} rows, cols={list(df.columns)[:6]}...")

print("\n=== matchup_predict_v2 loads profiles ===")
import services.matchup_predict_v2 as m
m._load()
print(f"  _batter_profiles:   {0 if m._batter_profiles is None else len(m._batter_profiles):,} rows")
print(f"  _batter_pitch_types:{0 if m._batter_pitch_types is None else len(m._batter_pitch_types):,} rows")
print(f"  _pitcher_arsenal:   {0 if m._pitcher_arsenal is None else len(m._pitcher_arsenal):,} rows")

print("\n=== game_simulation loads pitcher arsenal ===")
import services.game_simulation as g
g._load_data()
print(f"  _pitcher_arsenal_df:{0 if g._pitcher_arsenal_df is None else len(g._pitcher_arsenal_df):,} rows")

print("\nOK")
