#!/usr/bin/env python3
import os
import sys
from io import StringIO
import pandas as pd
import requests
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get("DATABASE_URL")
YEAR = 2026

def update_percentiles():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    # Target the official Percentile Rankings leaderboard for 1-99 values
    url = f"https://baseballsavant.mlb.com/leaderboard/percentile-rankings?year={YEAR}&type=batter&csv=true"
    
    print("Fetching 2026 Official Percentile Rankings...", flush=True)
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))

    if df.empty:
        print("No percentile data found.", flush=True)
        return

    # Clean headers
    df.columns = [c.strip() for c in df.columns]
    
    # Force the season and ensure IDs map to both potential column names
    df['season'] = int(YEAR)
    if 'player_id' in df.columns:
        df['batter_id'] = df['player_id']
    
    # Map Savant's percentile headers to your DB columns
    df = df.rename(columns={
        'player_id': 'player_id',
        'xwoba': 'xwoba',
        'xba': 'xba',
        'xslg': 'xslg',
        'exit_velocity': 'avg_exit_velocity',
        'k_percentile': 'k_pct',
        'bb_percentile': 'bb_pct',
        'barrel_percentile': 'barrel_pct',
        'hard_hit_percentile': 'hardhit_pct',
        'whiff_percentile': 'whiff_pct',
        'chase_percentile': 'chase_pct',
        'sweet_spot_percentile': 'sweet_spot_pct'
    })

    # Connect to Postgres
    db_url = DATABASE_URL.replace("postgres://", "postgresql+pg8000://", 1) if DATABASE_URL.startswith("postgres://") else DATABASE_URL.replace("postgresql://", "postgresql+pg8000://", 1)
    
    import ssl
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    engine = create_engine(db_url, connect_args={"ssl_context": ssl_context}, future=True)
    
    with engine.begin() as conn:
        print("Syncing with database table 'savant_batting_season'...", flush=True)
        # Wipe old 2026 data
        conn.execute(text(f"DELETE FROM savant_batting_season WHERE season = {YEAR}"))
        
        # Get actual DB columns to prevent 'column not found' errors
        db_cols = {r[0] for r in conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='savant_batting_season'")).fetchall()}
        keep_cols = [c for c in df.columns if c in db_cols]
        
        df_final = df[keep_cols].copy()
        
        # Write to database
        df_final.to_sql("savant_batting_season", conn, if_exists="append", index=False)
        print(f"SUCCESS: Loaded {len(df_final)} player profiles into savant_batting_season!", flush=True)

if __name__ == "__main__":
    update_percentiles()
