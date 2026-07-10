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
