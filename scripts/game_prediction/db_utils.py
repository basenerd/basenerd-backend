"""
Shared database utilities for game prediction scripts.
Uses psycopg (v3) directly, matching the pattern in services/*.py.
"""

import os
import psycopg
import pandas as pd

# Load .env from project root
_env_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
if os.path.exists(_env_path):
    with open(_env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_conn():
    """Get a psycopg connection."""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL env var not set.")
    return psycopg.connect(DATABASE_URL)


def query_df(sql, params=None):
    """Run a SQL query and return a pandas DataFrame."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


def _pg_type(series):
    """Map a pandas dtype to a Postgres column type."""
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE PRECISION"
    return "TEXT"


def _clean_value(v):
    """Convert numpy scalars / NaN to native Python / None for COPY."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return v.item() if hasattr(v, "item") else v


def write_df(df, table):
    """Atomically replace `table` with the contents of `df`.

    Bulk-loads into a staging table via COPY, then swaps it in with a
    rename inside a single transaction, so concurrent readers always see
    a complete table (the old one until commit, the new one after) and
    never an empty or half-written one.
    """
    if df is None or df.empty:
        print(f"  write_df: skipped {table} (empty DataFrame)")
        return

    # Upgrade numeric object columns (e.g. Decimal from AVG) to real numerics
    # so they land as DOUBLE PRECISION rather than TEXT.
    df = df.copy()
    for c in df.columns:
        if df[c].dtype == object:
            try:
                df[c] = pd.to_numeric(df[c])
            except (ValueError, TypeError):
                pass

    cols = list(df.columns)
    col_defs = ", ".join(f'"{c}" {_pg_type(df[c])}' for c in cols)
    col_list = ", ".join(f'"{c}"' for c in cols)
    staging = f"{table}__staging"

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{staging}"')
            cur.execute(f'CREATE TABLE "{staging}" ({col_defs})')
            with cur.copy(f'COPY "{staging}" ({col_list}) FROM STDIN') as copy:
                for row in df.itertuples(index=False, name=None):
                    copy.write_row([_clean_value(v) for v in row])
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            cur.execute(f'ALTER TABLE "{staging}" RENAME TO "{table}"')
        conn.commit()

    print(f"  write_df: wrote {len(df):,} rows -> {table}")
