# scripts/update_standings.py
#
# Minimal "pull MLB standings -> upsert into Postgres" script.
#
# Requirements:
#   pip install requests psycopg[binary]
#
# Env:
#   export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
#   (optional) export MLB_SEASON="2026"
#
# Run:
#   python scripts/update_standings.py

import os
import sys
import json
from datetime import datetime, timezone

import requests

# psycopg v3 preferred; fallback to psycopg2 if needed
try:
    import psycopg
    PSYCOPG3 = True
except Exception:
    import psycopg2 as psycopg  # type: ignore
    PSYCOPG3 = False


MLB_STANDINGS_URL = "https://statsapi.mlb.com/api/v1/standings"


def get_season() -> int:
    s = os.getenv("MLB_SEASON")
    if s:
        return int(s)
    # MLB regular season starts in spring; default to current year
    return datetime.now(timezone.utc).year


def fetch_standings(season: int) -> dict:
    params = {
        "leagueId": "103,104",  # AL, NL
        "season": str(season),
        "standingsTypes": "regularSeason",
        # hydrate adds division/league info in a consistent way
        "hydrate": "team(division,league),standings(note)",
    }
    r = requests.get(MLB_STANDINGS_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def normalize_rows(payload: dict, season: int, pulled_at_utc: datetime) -> list[dict]:
    rows: list[dict] = []
    records = payload.get("records", [])

    for div_block in records:
        # division info (best-effort)
        division = (div_block.get("division") or {}).get("name") or ""
        league = (div_block.get("league") or {}).get("name") or ""

        for tr in div_block.get("teamRecords", []):
            team = tr.get("team") or {}
            team_id = team.get("id")

            # Some fields are nested / can be missing
            wins = tr.get("wins")
            losses = tr.get("losses")
            pct = tr.get("winningPercentage")
            gb = tr.get("gamesBack")
            wc_gb = tr.get("wildCardGamesBack")
            rs = (tr.get("runsScored") if tr.get("runsScored") is not None else None)
            ra = (tr.get("runsAllowed") if tr.get("runsAllowed") is not None else None)
            streak = (tr.get("streak") or {}).get("streakCode") or ""

            # Team identifiers
            team_name = team.get("name") or ""
            team_abbrev = team.get("abbreviation") or ""

            if team_id is None:
                continue

            rows.append(
                {
                    "season": season,
                    "league": league,
                    "division": division,
                    "team_id": int(team_id),
                    "team_abbrev": team_abbrev,
                    "team_name": team_name,
                    "w": int(wins) if wins is not None else None,
                    "l": int(losses) if losses is not None else None,
                    "pct": float(pct) if pct not in (None, "") else None,
                    "gb": str(gb) if gb is not None else "",
                    "wc_gb": str(wc_gb) if wc_gb is not None else "",
                    "rs": int(rs) if rs is not None else None,
                    "ra": int(ra) if ra is not None else None,
                    "streak": streak,
                    "last_updated": pulled_at_utc,
                }
            )

    return rows


def ensure_table(conn):
    # Minimal table for current standings snapshot by team + season.
    ddl = """
    CREATE TABLE IF NOT EXISTS standings (
      season        INT NOT NULL,
      league        TEXT,
      division      TEXT,
      team_id       INT NOT NULL,
      team_abbrev   TEXT,
      team_name     TEXT,
      w             INT,
      l             INT,
      pct           DOUBLE PRECISION,
      gb            TEXT,
      wc_gb         TEXT,
      rs            INT,
      ra            INT,
      streak        TEXT,
      last_updated  TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (season, team_id)
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)


def upsert_rows(conn, rows: list[dict]):
    if not rows:
        raise RuntimeError("No rows to upsert (parsed 0 teams).")

    sql = """
    INSERT INTO standings (
      season, league, division, team_id, team_abbrev, team_name,
      w, l, pct, gb, wc_gb, rs, ra, streak, last_updated
    )
    VALUES (
      %(season)s, %(league)s, %(division)s, %(team_id)s, %(team_abbrev)s, %(team_name)s,
      %(w)s, %(l)s, %(pct)s, %(gb)s, %(wc_gb)s, %(rs)s, %(ra)s, %(streak)s, %(last_updated)s
    )
    ON CONFLICT (season, team_id) DO UPDATE SET
      league = EXCLUDED.league,
      division = EXCLUDED.division,
      team_abbrev = EXCLUDED.team_abbrev,
      team_name = EXCLUDED.team_name,
      w = EXCLUDED.w,
      l = EXCLUDED.l,
      pct = EXCLUDED.pct,
      gb = EXCLUDED.gb,
      wc_gb = EXCLUDED.wc_gb,
      rs = EXCLUDED.rs,
      ra = EXCLUDED.ra,
      streak = EXCLUDED.streak,
      last_updated = EXCLUDED.last_updated
    ;
    """
    with conn.cursor() as cur:
        # executemany works in psycopg3 and psycopg2
        cur.executemany(sql, rows)


def main() -> int:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL env var is not set.", file=sys.stderr)
        return 2

    season = get_season()
    pulled_at = datetime.now(timezone.utc)

    # 1) Fetch + parse
    try:
        payload = fetch_standings(season)
    except Exception as e:
        print(f"ERROR fetching standings: {e}", file=sys.stderr)
        return 3

    rows = normalize_rows(payload, season, pulled_at)

    # Simple sanity check: MLB should have 30 teams
    if len(rows) < 28:
        print("WARNING: Parsed fewer than 28 teams. Dumping a small debug sample:", file=sys.stderr)
        print(json.dumps(payload, indent=2)[:1500], file=sys.stderr)

    # 2) Write to DB
    try:
        if PSYCOPG3:
            with psycopg.connect(db_url) as conn:
                ensure_table(conn)
                upsert_rows(conn, rows)
                conn.commit()
        else:
            conn = psycopg.connect(db_url)
            try:
                ensure_table(conn)
                upsert_rows(conn, rows)
                conn.commit()
            finally:
                conn.close()
    except Exception as e:
        print(f"ERROR writing to database: {e}", file=sys.stderr)
        return 4

    print(f"OK: Upserted {len(rows)} standings rows for season {season} at {pulled_at.isoformat()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
