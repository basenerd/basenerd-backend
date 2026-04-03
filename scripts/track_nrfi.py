#!/usr/bin/env python3
"""
NRFI prediction tracker.

Two phases:
  1. PREDICT — For today's upcoming games, fetch pregame NRFI predictions
     and store them in the `nrfi_predictions` table.
  2. RESOLVE — For games that are final, fetch the first-inning linescore
     and update the actual result.

Designed to run as a cron job (e.g. every 15 minutes during game days).
Safe to re-run: uses ON CONFLICT DO NOTHING for predictions,
ON CONFLICT DO UPDATE for resolution.

Usage:
  python scripts/track_nrfi.py              # predict + resolve for today
  python scripts/track_nrfi.py --date 2026-04-02
  python scripts/track_nrfi.py --resolve-only
  python scripts/track_nrfi.py --backfill 7  # resolve last 7 days
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import psycopg
import requests

log = logging.getLogger("track_nrfi")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

# ---- .env loader ----
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

# ---- DB ----
_RENDER_INTERNAL_DB = (
    "postgresql://basenerd_user:d5LmELIOiEszYPBSLSDT1oIi79gkgDV6"
    "@dpg-d5i0tku3jp1c73f1d3gg-a/basenerd"
)
_DATABASE_URL = os.environ.get("DATABASE_URL", "") or _RENDER_INTERNAL_DB


def _get_conn():
    url = _DATABASE_URL
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg.connect(url)


DDL = """
CREATE TABLE IF NOT EXISTS nrfi_predictions (
    game_pk         INTEGER NOT NULL PRIMARY KEY,
    game_date       DATE NOT NULL,
    away_abbrev     VARCHAR(4),
    home_abbrev     VARCHAR(4),
    away_pitcher_id INTEGER,
    home_pitcher_id INTEGER,
    away_pitcher    VARCHAR(80),
    home_pitcher    VARCHAR(80),
    nrfi_pct        DOUBLE PRECISION,
    yrfi_pct        DOUBLE PRECISION,
    away_score_pct  DOUBLE PRECISION,
    home_score_pct  DOUBLE PRECISION,
    away_exp_runs   DOUBLE PRECISION,
    home_exp_runs   DOUBLE PRECISION,
    american_odds   INTEGER,
    away_1st_runs   SMALLINT,
    home_1st_runs   SMALLINT,
    nrfi_actual     BOOLEAN,
    predicted_at    TIMESTAMPTZ DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_nrfi_game_date ON nrfi_predictions (game_date);
CREATE INDEX IF NOT EXISTS idx_nrfi_actual ON nrfi_predictions (nrfi_actual) WHERE nrfi_actual IS NOT NULL;
"""

_table_ensured = False


def _ensure_table(conn):
    global _table_ensured
    if _table_ensured:
        return
    conn.execute(DDL)
    conn.commit()
    _table_ensured = True


# ---- American odds conversion ----

def prob_to_american(p: float) -> int:
    """Convert probability (0-1) to American odds."""
    if p <= 0 or p >= 1:
        return 0
    if p >= 0.5:
        return round(-(p / (1 - p)) * 100)
    return round(((1 - p) / p) * 100)


# ---- MLB API helpers ----

BASE = "https://statsapi.mlb.com/api/v1"


def get_schedule(date_ymd: str) -> list[dict]:
    """Fetch MLB schedule for a date, returning list of game dicts."""
    url = f"{BASE}/schedule"
    params = {
        "sportId": 1,
        "date": date_ymd,
        "hydrate": "team,linescore,probablePitcher(note)",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json() or {}
    games = []
    for date_obj in data.get("dates", []):
        for g in date_obj.get("games", []):
            games.append(g)
    return games


def get_game_feed(game_pk: int) -> dict:
    url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


# ---- Prediction phase ----

def predict_games(date_ymd: str, conn):
    """Fetch pregame NRFI predictions for all games on a date and store them."""
    # Import the prediction service (add parent to path)
    parent = Path(__file__).resolve().parent.parent
    if str(parent) not in sys.path:
        sys.path.insert(0, str(parent))
    from services.pregame_predictions import get_pregame_predictions

    games = get_schedule(date_ymd)
    scheduled = [
        g for g in games
        if (g.get("status", {}).get("abstractGameState", "").lower()
            in ("preview", "pre-game", "warmup", "scheduled"))
    ]

    log.info("Found %d scheduled games for %s", len(scheduled), date_ymd)

    for g in scheduled:
        game_pk = g["gamePk"]

        # Skip if already predicted
        row = conn.execute(
            "SELECT 1 FROM nrfi_predictions WHERE game_pk = %s", (game_pk,)
        ).fetchone()
        if row:
            continue

        try:
            pred = get_pregame_predictions(game_pk)
        except Exception as e:
            log.warning("Pregame failed for %s: %s", game_pk, e)
            continue

        if not pred.get("ok") or not pred.get("nrfi"):
            log.info("No NRFI data for game %s (lineups not posted?)", game_pk)
            continue

        n = pred["nrfi"]
        away_p = pred.get("home", {}).get("pitcher") or {}
        home_p = pred.get("away", {}).get("pitcher") or {}
        odds = prob_to_american(n["nrfi_pct"])

        conn.execute("""
            INSERT INTO nrfi_predictions
                (game_pk, game_date, away_abbrev, home_abbrev,
                 away_pitcher_id, home_pitcher_id, away_pitcher, home_pitcher,
                 nrfi_pct, yrfi_pct, away_score_pct, home_score_pct,
                 away_exp_runs, home_exp_runs, american_odds)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_pk) DO NOTHING
        """, (
            game_pk, date_ymd,
            pred.get("away_abbrev"), pred.get("home_abbrev"),
            away_p.get("id"), home_p.get("id"),
            away_p.get("name"), home_p.get("name"),
            n["nrfi_pct"], n["yrfi_pct"],
            n["away_score_pct"], n["home_score_pct"],
            n["away_exp_runs"], n["home_exp_runs"],
            odds,
        ))
        conn.commit()
        log.info("Predicted game %s: %s @ %s  NRFI=%.1f%%  odds=%s",
                 game_pk, pred.get("away_abbrev"), pred.get("home_abbrev"),
                 n["nrfi_pct"] * 100, odds)


# ---- Resolution phase ----

def resolve_games(date_ymd: str, conn):
    """For final games on a date, update actual first-inning results."""
    games = get_schedule(date_ymd)
    final_games = {
        g["gamePk"]: g for g in games
        if g.get("status", {}).get("abstractGameState", "").lower() == "final"
    }

    if not final_games:
        log.info("No final games for %s", date_ymd)
        return

    # Get unresolved predictions for this date
    rows = conn.execute(
        "SELECT game_pk FROM nrfi_predictions WHERE game_date = %s AND nrfi_actual IS NULL",
        (date_ymd,)
    ).fetchall()

    for (game_pk,) in rows:
        if game_pk not in final_games:
            continue

        g = final_games[game_pk]
        linescore = g.get("linescore", {})
        innings = linescore.get("innings", [])

        if not innings:
            # Try fetching the full feed for more detail
            try:
                feed = get_game_feed(game_pk)
                live = feed.get("liveData", {})
                linescore = live.get("linescore", {})
                innings = linescore.get("innings", [])
            except Exception:
                pass

        if not innings:
            log.warning("No innings data for final game %s", game_pk)
            continue

        first = innings[0]
        away_r = first.get("away", {}).get("runs")
        home_r = first.get("home", {}).get("runs")

        if away_r is None or home_r is None:
            log.warning("Incomplete 1st inning data for game %s", game_pk)
            continue

        nrfi_actual = (away_r == 0 and home_r == 0)

        conn.execute("""
            UPDATE nrfi_predictions
            SET away_1st_runs = %s, home_1st_runs = %s,
                nrfi_actual = %s, resolved_at = NOW()
            WHERE game_pk = %s
        """, (away_r, home_r, nrfi_actual, game_pk))
        conn.commit()

        result_str = "NRFI" if nrfi_actual else "YRFI"
        log.info("Resolved game %s: %s (away=%d, home=%d)",
                 game_pk, result_str, away_r, home_r)


# ---- Main ----

def main():
    parser = argparse.ArgumentParser(description="Track NRFI predictions")
    parser.add_argument("--date", help="Date (YYYY-MM-DD), default today")
    parser.add_argument("--resolve-only", action="store_true",
                        help="Only resolve existing predictions, don't make new ones")
    parser.add_argument("--backfill", type=int, default=0,
                        help="Resolve predictions for the last N days")
    args = parser.parse_args()

    today = datetime.utcnow().date()
    target = args.date or today.strftime("%Y-%m-%d")

    conn = _get_conn()
    _ensure_table(conn)

    if args.backfill > 0:
        log.info("Backfilling resolution for last %d days", args.backfill)
        for i in range(args.backfill):
            d = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            resolve_games(d, conn)
    else:
        if not args.resolve_only:
            predict_games(target, conn)
        resolve_games(target, conn)

    conn.close()
    log.info("Done.")


if __name__ == "__main__":
    main()
