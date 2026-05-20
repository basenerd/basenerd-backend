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
    
    # 1. Fetch Expected Metrics (xwoba, xba, xslg, etc.)
    url_expected = f"https://baseballsavant.mlb.com/leaderboard/expected_statistics?type=batter&year={YEAR}&position=&team=&min=q&csv=true"
    print("Fetching 2026 Expected Metrics...", flush=True)
    r1 = requests.get(url_expected, headers=headers, timeout=60)
    r1.raise_for_status()
    df_expected = pd.read_csv(StringIO(r1.text))
    
    # 2. Fetch Standard Statcast Metrics (barrel_pct, hardhit_pct, k_pct, bb_pct, whiff_pct, etc.)
    url_standard = f"https://baseballsavant.mlb.com/leaderboard/statcast?type=batter&year={YEAR}&position=&team=&min=q&csv=true"
    print("Fetching 2026 Standard Statcast Metrics...", flush=True)
    r2 = requests.get(url_standard, headers=headers, timeout=60)
    r2.raise_for_status()
    df_standard = pd.read_csv(StringIO(r2.text))

    if df_expected.empty or df_standard.empty:
        print("Error: One of the data feeds returned empty records.", flush=True)
        return

    # Clean up whitespace in column names
    df_expected.columns = [c.strip() for c in df_expected.columns]
    df_standard.columns = [c.strip() for c in df_standard.columns]

    # 3. Rename Savant's CSV headers to match your exact Database Columns
    df_expected = df_expected.rename(columns={
        'player_id': 'player_id',
        'est_woba': 'xwoba',
        'est_ba': 'xba',
        'est_slg': 'xslg'
    })
    
    df_standard = df_standard.rename(columns={
        'player_id': 'player_id',
        'player_name': 'player_name',
        'team_name_alt': 'team',
        'batted_ball_as_percent': 'bbe',
        'strikeout_percent': 'k_pct',
        'walk_percent': 'bb_pct',
        'barrel_batted_rate': 'barrel_pct',
        'hard_hit_percent': 'hardhit_pct',
        'whiff_percent': 'whiff_pct',
        'chase_percent': 'chase_pct',
        'sweet_spot_percent': 'sweet_spot_pct',
        'avg_hit_speed': 'avg_exit_velocity'
    })

    # 4. Combine both datasets on player_id
    print("Merging datasets...", flush=True)
    df_merged = pd.merge(
        df_standard, 
        df_expected[['player_id', 'xwoba', 'xba', 'xslg']], 
        on='player_id', 
        how='left'
    )
    
    # Force global table parameters
    df_merged['season'] = int(YEAR)
    df_merged['pa'] = df_merged.get('attempts', 0) # Fallback to attempts for plate appearances

    # 5. Connect to Postgres
    if not DATABASE_URL:
        print("Error: DATABASE_URL not set.", flush=True)
        return
    db_url = DATABASE_URL.replace("postgres://", "postgresql+pg8000://", 1) if DATABASE_URL.startswith("postgres://") else DATABASE_URL.replace("postgresql://", "postgresql+pg8000://", 1)
    
    import ssl
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    engine = create_engine(db_url, connect_args={"ssl_context": ssl_context}, future=True)
    
    with engine.begin() as conn:
        print("Syncing with database table 'savant_batting_season'...", flush=True)
        conn.execute(text(f"DELETE FROM savant_batting_season WHERE season = {YEAR}"))
        
        # Keep only the columns that actually exist in your Postgres table layout
        db_cols = {r[0] for r in conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='savant_batting_season'")).fetchall()}
        keep_cols = [c for c in df_merged.columns if c in db_cols]
        
        df_final = df_merged[keep_cols].copy()
        
        # Write to database
        df_final.to_sql("savant_batting_season", conn, if_exists="append", index=False)
        print(f"SUCCESS: Loaded percentiles for {len(df_final)} players into savant_batting_season!", flush=True)

if __name__ == "__main__":
    update_percentiles()
