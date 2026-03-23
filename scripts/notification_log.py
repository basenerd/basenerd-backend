"""
Shared notification dedup via Postgres.

Provides a simple `notification_log` table that all email scripts use
to prevent sending duplicate notifications across ephemeral cron runs
and daemon restarts.

Table schema:
    notification_log (
        type        TEXT,       -- 'lineup', 'pitcher_report', 'home_run'
        game_pk     INTEGER,
        entity_key  TEXT,       -- lineup hash, pitcher_id, play_index
        created_at  TIMESTAMPTZ DEFAULT now(),
        PRIMARY KEY (type, game_pk, entity_key)
    )
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import psycopg

log = logging.getLogger(__name__)

# Load .env if not already loaded
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

_DATABASE_URL = os.environ.get("DATABASE_URL", "")
_table_ensured = False

# ── Startup check ──────────────────────────────────────────────────────
# Fail LOUD at import time so duplicates are impossible to miss.
if not _DATABASE_URL:
    log.error("FATAL: DATABASE_URL is not set — notification dedup DISABLED. "
              "Emails WILL be duplicated. Set DATABASE_URL in Render env vars.")
    print("FATAL: DATABASE_URL not set — dedup disabled, emails will duplicate!",
          file=sys.stderr)
    # Don't sys.exit — let the caller decide, but make it unmissable in logs.

_db_available = False
if _DATABASE_URL:
    try:
        with psycopg.connect(_DATABASE_URL) as _conn:
            _conn.execute("SELECT 1")
        _db_available = True
        log.info("notification_log: DB connected OK")
    except Exception as _e:
        log.error("FATAL: Cannot connect to DATABASE_URL — dedup DISABLED: %s", _e)
        print(f"FATAL: DB connection failed — dedup disabled: {_e}", file=sys.stderr)


def _get_conn():
    """Return a new psycopg connection."""
    if not _DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg.connect(_DATABASE_URL)


def _ensure_table(conn):
    """Create the notification_log table if it doesn't exist."""
    global _table_ensured
    if _table_ensured:
        return
    conn.execute("""
        CREATE TABLE IF NOT EXISTS notification_log (
            type        TEXT        NOT NULL,
            game_pk     INTEGER     NOT NULL,
            entity_key  TEXT        NOT NULL,
            created_at  TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (type, game_pk, entity_key)
        )
    """)
    conn.commit()
    _table_ensured = True


def already_sent(notif_type: str, game_pk: int, entity_key: str) -> bool:
    """Check if this notification was already sent.

    If DB is unavailable, returns True (block sending) to prevent duplicates.
    """
    if not _db_available:
        log.error("DB unavailable — blocking send to prevent duplicate "
                  "(type=%s, game_pk=%s, key=%s)", notif_type, game_pk, entity_key)
        return True  # BLOCK sending when DB is down
    try:
        with _get_conn() as conn:
            _ensure_table(conn)
            row = conn.execute(
                "SELECT 1 FROM notification_log WHERE type = %s AND game_pk = %s AND entity_key = %s",
                (notif_type, game_pk, str(entity_key)),
            ).fetchone()
            return row is not None
    except Exception as e:
        log.error("notification_log check FAILED — blocking send to prevent "
                  "duplicate: %s", e)
        return True  # BLOCK sending on error


def mark_sent(notif_type: str, game_pk: int, entity_key: str):
    """Record that this notification was sent."""
    if not _db_available:
        return
    try:
        with _get_conn() as conn:
            _ensure_table(conn)
            conn.execute(
                """INSERT INTO notification_log (type, game_pk, entity_key)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (type, game_pk, entity_key) DO NOTHING""",
                (notif_type, game_pk, str(entity_key)),
            )
            conn.commit()
    except Exception as e:
        log.error("notification_log insert FAILED: %s", e)


def delete_entry(notif_type: str, game_pk: int, entity_key: str):
    """Remove an entry (used for lineup change re-sends)."""
    if not _db_available:
        return
    try:
        with _get_conn() as conn:
            _ensure_table(conn)
            conn.execute(
                "DELETE FROM notification_log WHERE type = %s AND game_pk = %s AND entity_key = %s",
                (notif_type, game_pk, str(entity_key)),
            )
            conn.commit()
    except Exception as e:
        log.error("notification_log delete FAILED: %s", e)


def get_entity_key(notif_type: str, game_pk: int) -> str | None:
    """Get the stored entity_key for a type+game_pk (used for lineup hash comparison)."""
    if not _db_available:
        return "DB_UNAVAILABLE"  # non-None → treat as "already sent"
    try:
        with _get_conn() as conn:
            _ensure_table(conn)
            row = conn.execute(
                "SELECT entity_key FROM notification_log WHERE type = %s AND game_pk = %s",
                (notif_type, game_pk),
            ).fetchone()
            return row[0] if row else None
    except Exception as e:
        log.error("notification_log lookup FAILED: %s", e)
        return "DB_ERROR"  # non-None → treat as "already sent"
