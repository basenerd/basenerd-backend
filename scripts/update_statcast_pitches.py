#!/usr/bin/env python3
import os
import ssl
import sys
import math
import hashlib
import argparse
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import create_engine, text

from pybaseball import statcast  # pip install pybaseball

TZ = ZoneInfo("America/Phoenix")

DATABASE_URL = os.environ.get("DATABASE_URL")
TABLE = os.environ.get("STATCAST_TABLE", "statcast_pitches")

# We’ll base pitch_id on this natural key (same concept you already use elsewhere) :contentReference[oaicite:4]{index=4}
KEY_COLS = ["game_pk", "at_bat_number", "pitch_number"]


def safe_none(x):
    # Convert NaN to None for SQLAlchemy executemany
    try:
        if x is None:
            return None
        if isinstance(x, float) and math.isnan(x):
            return None
        return x
    except Exception:
        return None


def build_engine():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL env var not set.")

    db_url = DATABASE_URL
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+pg8000://", 1)
    elif db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+pg8000://", 1)

    # Render SSL behavior (same as your existing cron pattern) :contentReference[oaicite:5]{index=5}
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    return create_engine(db_url, connect_args={"ssl_context": ssl_context}, pool_pre_ping=True)


def make_pitch_id(game_pk, at_bat_number, pitch_number) -> str:
    s = f"{int(game_pk)}-{int(at_bat_number)}-{int(pitch_number)}"
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def phoenix_yesterday() -> date:
    now_phx = datetime.now(TZ)
    return (now_phx - timedelta(days=1)).date()


def get_table_columns(conn, table_name: str) -> set[str]:
    # Pull current table cols so we only insert what exists
    sql = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = :t
    """)
    rows = conn.execute(sql, {"t": table_name}).fetchall()
    return {r[0] for r in rows}


def fetch_statcast(start_date: date, end_date: date) -> pd.DataFrame:
    # pybaseball.statcast wants strings YYYY-MM-DD
    print(f"Fetching Statcast: {start_date} -> {end_date}")
    df = statcast(start_dt=str(start_date), end_dt=str(end_date))
    if df is None:
        return pd.DataFrame()
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)
    return df


def upsert_df(conn, df: pd.DataFrame, table_name: str):
    if df.empty:
        print("No rows fetched.")
        return 0

    # Ensure required key cols exist
    missing = [c for c in KEY_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(f"Statcast df missing required key columns: {missing}")

    # Generate pitch_id
    df["pitch_id"] = df.apply(
        lambda r: make_pitch_id(r["game_pk"], r["at_bat_number"], r["pitch_number"]),
        axis=1
    )

    # Only keep columns that exist in the DB table
    table_cols = get_table_columns(conn, table_name)
    keep_cols = [c for c in df.columns if c in table_cols]

    # Always include pitch_id (must exist in table from migration step)
    if "pitch_id" not in keep_cols:
        raise RuntimeError("pitch_id not found in DB table columns. Run the ALTER TABLE step first.")

    df2 = df[keep_cols].copy()

    # Convert NaN -> None for SQL
    for c in df2.columns:
        df2[c] = df2[c].map(safe_none)

    cols_sql = ", ".join([f'"{c}"' for c in keep_cols])
    vals_sql = ", ".join([f":{c}" for c in keep_cols])

    # Update everything except pitch_id on conflict
    update_cols = [c for c in keep_cols if c != "pitch_id"]
    set_sql = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])

    upsert_sql = text(f"""
        INSERT INTO {table_name} ({cols_sql})
        VALUES ({vals_sql})
        ON CONFLICT (pitch_id)
        DO UPDATE SET {set_sql}
    """)

    payload = df2.to_dict(orient="records")
    conn.execute(upsert_sql, payload)
    print(f"Upserted rows: {len(payload)}")
    return len(payload)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["daily", "backfill"], required=True)
    ap.add_argument("--start", help="YYYY-MM-DD (backfill)")
    ap.add_argument("--end", help="YYYY-MM-DD (backfill)")
    args = ap.parse_args()

    engine = build_engine()

    if args.mode == "daily":
        d = phoenix_yesterday()
        start_date = d
        end_date = d
    else:
        if not args.start or not args.end:
            raise RuntimeError("--start and --end are required for backfill mode")
        start_date = date.fromisoformat(args.start)
        end_date = date.fromisoformat(args.end)

    df = fetch_statcast(start_date, end_date)

    with engine.begin() as conn:
        print("DB check:", conn.execute(text("SELECT 1")).scalar())
        n = upsert_df(conn, df, TABLE)

    print("DONE rows=", n)


if __name__ == "__main__":
    print("Python:", sys.version)
    main()
