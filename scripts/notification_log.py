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
    """Check if this notification was already sent."""
    try:
        with _get_conn() as conn:
            _ensure_table(conn)
            row = conn.execute(
                "SELECT 1 FROM notification_log WHERE type = %s AND game_pk = %s AND entity_key = %s",
                (notif_type, game_pk, str(entity_key)),
            ).fetchone()
            return row is not None
    except Exception as e:
        log.warning("notification_log check failed (will proceed): %s", e)
        return False


def mark_sent(notif_type: str, game_pk: int, entity_key: str):
    """Record that this notification was sent."""
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
        log.warning("notification_log insert failed: %s", e)


def delete_entry(notif_type: str, game_pk: int, entity_key: str):
    """Remove an entry (used for lineup change re-sends)."""
    try:
        with _get_conn() as conn:
            _ensure_table(conn)
            conn.execute(
                "DELETE FROM notification_log WHERE type = %s AND game_pk = %s AND entity_key = %s",
                (notif_type, game_pk, str(entity_key)),
            )
            conn.commit()
    except Exception as e:
        log.warning("notification_log delete failed: %s", e)


def get_entity_key(notif_type: str, game_pk: int) -> str | None:
    """Get the stored entity_key for a type+game_pk (used for lineup hash comparison)."""
    try:
        with _get_conn() as conn:
            _ensure_table(conn)
            row = conn.execute(
                "SELECT entity_key FROM notification_log WHERE type = %s AND game_pk = %s",
                (notif_type, game_pk),
            ).fetchone()
            return row[0] if row else None
    except Exception as e:
        log.warning("notification_log lookup failed: %s", e)
        return None
