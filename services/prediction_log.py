"""Fire-and-forget logging of pregame matchup predictions for later grading.

Records each pregame batter-vs-pitcher predicted PA-outcome distribution so a
scheduled job (scripts/game_prediction/track_predictions.py) can later join it
to the ACTUAL plate-appearance outcomes and measure real production accuracy
over time — no large training parquet needed on the server. Never raises: a
logging failure must never affect user-facing predictions.
"""
import os
import json
import logging

import psycopg

log = logging.getLogger(__name__)
_TABLE_READY = False
_MODEL_VERSION = None


def _db_url():
    return os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_URL_PG") or ""


def _model_version():
    global _MODEL_VERSION
    if _MODEL_VERSION is None:
        try:
            p = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                             "models", "matchup_model_v2_meta.json")
            _MODEL_VERSION = json.load(open(p)).get("model_version", "v2")
        except Exception:
            _MODEL_VERSION = "v2"
    return _MODEL_VERSION


def _ensure(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS prediction_log (
            id BIGSERIAL PRIMARY KEY,
            logged_at TIMESTAMPTZ DEFAULT now(),
            game_pk BIGINT NOT NULL,
            batter BIGINT NOT NULL,
            pitcher BIGINT NOT NULL,
            season INTEGER,
            model_version TEXT,
            probs JSONB,
            UNIQUE (game_pk, batter, pitcher, model_version)
        )
    """)


def log_pregame(game_pk, season, pitcher_id, batters):
    """Batch-log one pitcher's opposing-lineup predictions. Never raises."""
    url = _db_url()
    if not url or not game_pk or not pitcher_id or not batters:
        return
    mv = _model_version()
    rows = []
    for b in batters:
        probs = b.get("probs")
        if b.get("id") and probs:
            rows.append((int(game_pk), int(b["id"]), int(pitcher_id),
                         int(season) if season else None, mv, json.dumps(probs)))
    if not rows:
        return
    try:
        with psycopg.connect(url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                global _TABLE_READY
                if not _TABLE_READY:
                    _ensure(cur)
                    _TABLE_READY = True
                cur.executemany("""
                    INSERT INTO prediction_log
                        (game_pk, batter, pitcher, season, model_version, probs)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (game_pk, batter, pitcher, model_version)
                    DO UPDATE SET probs = EXCLUDED.probs, logged_at = now()
                """, rows)
            conn.commit()
    except Exception as e:
        log.debug("prediction_log skipped: %s", e)
