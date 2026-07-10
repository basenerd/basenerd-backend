"""
Shared loader for the nightly-rebuilt player profile tables.

The `rebuild-profiles` cron writes each profile into a Postgres table
(profile_batter, profile_batter_pitch_type, profile_pitcher_arsenal) so the
web service can pick up fresh data on worker restart. On Render the cron and
the web service have separate ephemeral filesystems, so the parquet files the
cron writes never reach the web app — the database is the shared hand-off.

Falls back to the committed parquet files if the DB is unavailable or a table
is missing/empty, so local development and cold deploys still work.
"""

import os
import logging

import pandas as pd
import psycopg

log = logging.getLogger(__name__)


def _db_url():
    return os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_URL_PG") or ""


def load_profile(table, parquet_path=None):
    """Load a profile table from Postgres, falling back to parquet.

    Returns an empty DataFrame if neither source is available, so callers
    keep their existing league-average fallbacks.
    """
    url = _db_url()
    if url:
        try:
            with psycopg.connect(url, connect_timeout=10) as conn:
                with conn.cursor() as cur:
                    cur.execute(f'SELECT * FROM "{table}"')
                    cols = [d[0] for d in cur.description]
                    rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=cols)
            if not df.empty:
                log.info("Loaded profile %s from DB (%d rows)", table, len(df))
                return df
            log.warning("Profile table %s is empty; falling back to parquet", table)
        except Exception as e:
            log.warning("DB load of %s failed (%s); falling back to parquet", table, e)

    if parquet_path and os.path.exists(parquet_path):
        try:
            df = pd.read_parquet(parquet_path)
            log.info("Loaded profile %s from parquet (%d rows)", table, len(df))
            return df
        except Exception as e:
            log.warning("Parquet load of %s failed: %s", parquet_path, e)

    return pd.DataFrame()
