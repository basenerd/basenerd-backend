#!/usr/bin/env python3
"""
scripts/update_statcast_pitches.py

Nightly Statcast updater (Render cron friendly):
- Fetches pitch-level Statcast from Baseball Savant "type=details" CSV (bypasses pybaseball offseason skipping)
- Generates pitch_id = md5(game_pk-at_bat_number-pitch_number)
- Upserts into Postgres using ON CONFLICT (pitch_id) DO UPDATE
- Auto-aligns DataFrame columns to the existing DB table columns
- Coerces DataFrame values to match DB column types AND force-casts common Statcast integer columns
  (fixes errors like: invalid input syntax for type integer: "12.0")

Env vars:
- DATABASE_URL (required)
- STATCAST_TABLE (optional, default "statcast_pitches")

Usage:
  Daily (yesterday Phoenix):
    python -u scripts/update_statcast_pitches.py --mode daily

  Daily with rolling window (recommended):
    python -u scripts/update_statcast_pitches.py --mode daily --days 2

  Backfill:
    python -u scripts/update_statcast_pitches.py --mode backfill --start 2026-02-01 --end 2026-02-28

DB migration (run once):
  ALTER TABLE statcast_pitches ADD COLUMN IF NOT EXISTS pitch_id text;
  UPDATE statcast_pitches
    SET pitch_id = md5(COALESCE(game_pk::text,'') || '-' || COALESCE(at_bat_number::text,'') || '-' || COALESCE(pitch_number::text,''))
    WHERE pitch_id IS NULL;
  CREATE UNIQUE INDEX IF NOT EXISTS statcast_pitches_pitch_id_uq ON statcast_pitches (pitch_id);
"""

import os
import ssl
import sys
import re
import math
import hashlib
import argparse
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from io import StringIO

import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine, text

TZ = ZoneInfo("America/Phoenix")

DATABASE_URL = os.environ.get("DATABASE_URL")
TABLE = os.environ.get("STATCAST_TABLE", "statcast_pitches")

# Natural key columns needed to build pitch_id
KEY_COLS = ["game_pk", "at_bat_number", "pitch_number"]

# Regex: allow "-3", "12", "12.0", "12.00" as integer-ish
_INTISH_RE = re.compile(r"^\s*-?\d+(\.0+)?\s*$")

# These Statcast columns are *frequently* integers but arrive as floats due to NaNs in CSV.
# Force-cast them to integers when present in your DB table, regardless of information_schema quirks.
FORCE_INT_COLS = {
    "zone",
    "hit_location",
    "balls",
    "strikes",
    "game_year",
    "outs_when_up",
    "inning",
    "game_pk",
    "at_bat_number",
    "pitch_number",
    "home_score",
    "away_score",
    "bat_score",
    "fld_score",
    "post_away_score",
    "post_home_score",
    "post_bat_score",
    "post_fld_score",
    "on_1b",
    "on_2b",
    "on_3b",
    "launch_speed_angle",
    "woba_denom",
    "fielder_2",
    "fielder_3",
    "fielder_4",
    "fielder_5",
    "fielder_6",
    "fielder_7",
    "fielder_8",
    "fielder_9",
    "n_thruorder_pitcher",
    "n_priorpa_thisgame_player_at_bat",
    "pitcher_days_since_prev_game",
    "batter_days_since_prev_game",
    "pitcher_days_until_next_game",
    "batter_days_until_next_game",
}


def phoenix_yesterday() -> date:
    return (datetime.now(TZ) - timedelta(days=1)).date()


def build_engine():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL env var not set.")

    db_url = DATABASE_URL
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+pg8000://", 1)
    elif db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+pg8000://", 1)

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    return create_engine(
        db_url,
        connect_args={"ssl_context": ssl_context},
        pool_pre_ping=True,
        future=True,
    )


def make_pitch_id(game_pk, at_bat_number, pitch_number) -> str:
    s = f"{int(game_pk)}-{int(at_bat_number)}-{int(pitch_number)}"
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def safe_none(x):
    """Convert pandas/numpy missing values to None; normalize numpy scalars."""
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass

    if isinstance(x, (np.integer,)):
        return int(x)
    if isinstance(x, (np.floating,)):
        return float(x)
    return x


def get_table_columns(conn, table_name: str) -> set[str]:
    rows = conn.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=:t
            """
        ),
        {"t": table_name},
    ).fetchall()
    return {r[0] for r in rows}


def get_table_coltypes(conn, table_name: str) -> dict[str, str]:
    rows = conn.execute(
        text(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=:t
            """
        ),
        {"t": table_name},
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _coerce_series_to_int64_nullable(s: pd.Series) -> pd.Series:
    """
    Coerce a Series to pandas nullable Int64, accepting values like 12.0 / "12.0" / "12".
    Non-integer-ish values become <NA>.
    """
    if s.dtype == "object":
        s2 = s.where(~pd.isna(s), np.nan).astype(str)
        s2 = s2.where(s2.str.match(_INTISH_RE), np.nan)
        s = s2

    s = pd.to_numeric(s, errors="coerce")
    # Keep only whole numbers (avoid accidental truncation of true decimals)
    # If value is 12.3 -> becomes NaN
    whole_mask = s.isna() | (np.isclose(s, np.round(s), atol=1e-9))
    s = s.where(whole_mask, np.nan)

    return s.round(0).astype("Int64")


def coerce_df_to_db_types(df: pd.DataFrame, coltypes: dict[str, str], cols: list[str]) -> pd.DataFrame:
    """
    Coerce DataFrame columns to match DB types + force-cast common Statcast integer columns.
    """
    df = df.copy()

    # DB-driven integer cols
    int_cols_db = [c for c in cols if coltypes.get(c) in ("integer", "smallint", "bigint")]

    # Force integer cols if present in table
    int_cols_forced = [c for c in cols if c in FORCE_INT_COLS]

    int_cols = sorted(set(int_cols_db + int_cols_forced))

    float_cols = [c for c in cols if coltypes.get(c) in ("double precision", "real", "numeric", "decimal")]
    bool_cols = [c for c in cols if coltypes.get(c) == "boolean"]

    # Integers
    for c in int_cols:
        if c not in df.columns:
            continue
        df[c] = _coerce_series_to_int64_nullable(df[c])

    # Floats/numerics (avoid clobbering the forced int cols)
    for c in float_cols:
        if c not in df.columns or c in int_cols:
            continue
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Booleans
    for c in bool_cols:
        if c not in df.columns:
            continue

        def _to_bool(v):
            if v is None:
                return None
            try:
                if pd.isna(v):
                    return None
            except Exception:
                pass
            s = str(v).strip().lower()
            if s in ("1", "true", "t", "yes", "y"):
                return True
            if s in ("0", "false", "f", "no", "n"):
                return False
            return None

        df[c] = df[c].map(_to_bool)

    return df


def fetch_statcast_details_csv(start_date: date, end_date: date, team: str | None = None) -> pd.DataFrame:
    """
    Fetch pitch-level Statcast from Baseball Savant "type=details" CSV.
    Includes Spring Training via hfGT including S.

    Note: Savant uses game_date_gt/game_date_lt; using same date for both works for a single-day pull.
    """
    print(f"Fetching Savant (details CSV): {start_date} -> {end_date}", flush=True)

    base = "https://baseballsavant.mlb.com"
    path = (
        "/statcast_search/csv"
        "?all=true"
        "&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones="
        "&hfGT=R%7CPO%7CS%7C="
        "&hfSea=&hfSit="
        "&player_type=pitcher"
        "&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA="
        f"&game_date_gt={start_date}"
        f"&game_date_lt={end_date}"
        f"&team={(team or '')}"
        "&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn="
        "&min_pitches=0&min_results=0"
        "&group_by=name"
        "&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc"
        "&min_abs=0"
        "&type=details&"
    )
    url = base + path

    r = requests.get(url, timeout=180)
    r.raise_for_status()

    if r.text.lstrip().startswith("<"):
        raise RuntimeError("Savant returned HTML instead of CSV (rate limit / block). Try again later.")

    df = pd.read_csv(StringIO(r.text))
    if not isinstance(df, pd.DataFrame):
        return pd.DataFrame()

    df.columns = [c.strip() for c in df.columns]

    print(f"Fetched rows: {len(df):,}", flush=True)
    if len(df) > 0:
        cols = list(df.columns)
        print("Columns sample:", cols[:25], ("..." if len(cols) > 25 else ""), flush=True)

    return df


def upsert_df(conn, df: pd.DataFrame, table_name: str, chunk_size: int = 5000) -> int:
    if df is None or df.empty:
        print("No rows fetched.", flush=True)
        return 0

    missing = [c for c in KEY_COLS if c not in df.columns]
    if missing:
        print("DF columns were:", df.columns.tolist(), flush=True)
        raise RuntimeError(f"Statcast df missing required key columns: {missing}")

    df = df.copy()
    df["pitch_id"] = df.apply(
        lambda r: make_pitch_id(r["game_pk"], r["at_bat_number"], r["pitch_number"]),
        axis=1,
    )

    table_cols = get_table_columns(conn, table_name)
    keep_cols = [c for c in df.columns if c in table_cols]

    if "pitch_id" not in keep_cols:
        raise RuntimeError("pitch_id not found in DB table columns. Run the ALTER TABLE step first.")

    coltypes = get_table_coltypes(conn, table_name)

    df2 = df[keep_cols].copy()
    df2 = coerce_df_to_db_types(df2, coltypes, keep_cols)

    # Convert NA -> None and normalize numpy scalars
    for c in df2.columns:
        df2[c] = df2[c].map(safe_none)

    # Quick guard: if any forced int cols still look like floats with .0, print and fail fast
    # (this catches the exact 'zone=12.0' case before Postgres throws)
    for c in (set(FORCE_INT_COLS) & set(keep_cols)):
        # if values are python float, they will stringify with "."
        bad = df2[c].apply(lambda v: isinstance(v, float) and (not math.isnan(v)))
        if bad.any():
            ex = df2.loc[bad, c].head(5).tolist()
            raise RuntimeError(f"Column '{c}' still has float values after coercion (examples: {ex}).")

    cols_sql = ", ".join([f'"{c}"' for c in keep_cols])
    vals_sql = ", ".join([f":{c}" for c in keep_cols])

    update_cols = [c for c in keep_cols if c != "pitch_id"]
    set_sql = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])

    upsert_sql = text(f"""
        INSERT INTO {table_name} ({cols_sql})
        VALUES ({vals_sql})
        ON CONFLICT (pitch_id)
        DO UPDATE SET {set_sql}
    """)

    records = df2.to_dict(orient="records")
    total = 0

    for i in range(0, len(records), chunk_size):
        chunk = records[i : i + chunk_size]
        conn.execute(upsert_sql, chunk)
        total += len(chunk)
        if total % (chunk_size * 5) == 0 or total == len(records):
            print(f"Upsert progress: {total:,}/{len(records):,}", flush=True)

    print(f"Upserted rows: {total:,}", flush=True)
    return total


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["daily", "backfill"], required=True)
    ap.add_argument("--start", help="YYYY-MM-DD (backfill)")
    ap.add_argument("--end", help="YYYY-MM-DD (backfill)")
    ap.add_argument("--days", type=int, default=1, help="Daily mode rolling window (default 1 = yesterday only). Recommend 2.")
    ap.add_argument("--team", default=None, help="Optional team filter (e.g., 'NYM').")
    ap.add_argument("--chunk-size", type=int, default=5000, help="Upsert chunk size.")
    return ap.parse_args()


def main():
    args = parse_args()

    print("Python:", sys.version, flush=True)

    engine = build_engine()

    if args.mode == "daily":
        end_d = phoenix_yesterday()
        start_d = end_d - timedelta(days=max(args.days, 1) - 1)
    else:
        if not args.start or not args.end:
            raise RuntimeError("--start and --end are required for backfill mode")
        start_d = date.fromisoformat(args.start)
        end_d = date.fromisoformat(args.end)

    df = fetch_statcast_details_csv(start_d, end_d, team=args.team)

    with engine.begin() as conn:
        print("DB check:", conn.execute(text("SELECT 1")).scalar(), flush=True)
        n = upsert_df(conn, df, TABLE, chunk_size=args.chunk_size)

    print("DONE rows=", n, flush=True)


if __name__ == "__main__":
    main()
