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
    url = f"https://baseballsavant.mlb.com/leaderboard/statcast?type=batter&year={YEAR}&position=&team=&min=q&csv=true"
    print("Fetching 2026 Statcast Percentiles from Savant...", flush=True)
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    
    df = pd.read_csv(StringIO(r.text))
    if df.empty:
        print("No percentile data found.", flush=True)
        return

    if not DATABASE_URL:
        print("Error: DATABASE_URL not set.", flush=True)
        return
    db_url = DATABASE_URL.replace("postgres://", "postgresql+pg8000://", 1) if DATABASE_URL.startswith("postgres://") else DATABASE_URL.replace("postgresql://", "postgresql+pg8000://", 1)
    
    import ssl
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    engine = create_engine(db_url, connect_args={"ssl_context": ssl_context}, future=True)
    
    df.columns = [c.strip() for c in df.columns]
    df['season'] = int(YEAR)
    
    # Map the primary key IDs explicitly
    if 'player_id' in df.columns:
        df['player_id'] = df['player_id']
        df['batter_id'] = df['player_id']

    with engine.begin() as conn:
        print("--- DATABASE SCHEMA DIAGNOSTIC ---", flush=True)
        db_cols = [r[0] for r in conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='savant_batting_season'")).fetchall()]
        print(f"Your DB Columns: {db_cols}", flush=True)
        print(f"Savant CSV Columns: {list(df.columns)}", flush=True)
        print("---------------------------------", flush=True)
        
        # Clear old and test a safe insert of matching columns
        conn.execute(text(f"DELETE FROM savant_batting_season WHERE season = {YEAR}"))
        keep_cols = [c for c in df.columns if c in db_cols]
        df_final = df[keep_cols].copy()
        
        df_final.to_sql("savant_batting_season", conn, if_exists="append", index=False)
        print(f"SUCCESS: Safe loaded {len(df_final)} records.", flush=True)

if __name__ == "__main__":
    update_percentiles()
