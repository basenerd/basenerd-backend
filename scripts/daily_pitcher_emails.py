#!/usr/bin/env python3
"""
Pitcher report emailer — runs every 15 minutes, no database needed.

Uses MLB API pitch timestamps as a natural dedup: only generates reports
for pitchers whose last pitch was within the last ~18 minutes (just over
one cron interval). This catches both mid-game pitching changes and
game-final events without any external state.

Usage:
    python scripts/daily_pitcher_emails.py                # check today + yesterday
    python scripts/daily_pitcher_emails.py --date 2026-03-15
    python scripts/daily_pitcher_emails.py --window 30    # wider window (minutes)
"""
from __future__ import annotations

import argparse
import logging
import os
import smtplib
import sys
from datetime import datetime, timedelta, timezone
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from generate_pitcher_report_pdf import (
    _fetch_live_feed,
    _extract_game_info,
    generate_report,
)

# Load .env
env_path = ROOT / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

GMAIL_USER = os.environ.get("GMAIL_USER", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
NOTIFY_EMAIL = os.environ.get("NOTIFY_EMAIL", "nicklabella6@gmail.com")

MLB_API = "https://statsapi.mlb.com/api/v1"
ET = ZoneInfo("America/New_York")

# Window slightly larger than cron interval (15 min) to avoid missed reports.
# Duplicates are nearly impossible because the next run is 15 min later,
# putting the pitcher well outside the 18 min window.
DEFAULT_WINDOW_MIN = 18

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pitcher_emails")


# ---------------------------------------------------------------------------
# MLB schedule
# ---------------------------------------------------------------------------
def fetch_games(date_str: str) -> list[dict]:
    """Return games that are live or recently final."""
    url = f"{MLB_API}/schedule"
    params = {"date": date_str, "sportId": 1, "hydrate": "linescore"}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()

    games = []
    for d in r.json().get("dates") or []:
        for g in d.get("games") or []:
            gp = g.get("gamePk")
            if not gp:
                continue
            status = (g.get("status") or {})
            abstract = (status.get("abstractGameState") or "").lower()
            # We care about games that are live or final
            if abstract not in ("live", "final"):
                continue
            teams = g.get("teams") or {}
            away = (teams.get("away") or {}).get("team", {})
            home = (teams.get("home") or {}).get("team", {})
            games.append({
                "game_pk": int(gp),
                "status": abstract,
                "away_name": away.get("name", "?"),
                "home_name": home.get("name", "?"),
                "away_abbrev": away.get("abbreviation", "?"),
                "home_abbrev": home.get("abbreviation", "?"),
            })
    return games


# ---------------------------------------------------------------------------
# Find pitchers who just finished, using pitch timestamps
# ---------------------------------------------------------------------------
def find_recently_done_pitchers(feed: dict, window_min: int) -> list[dict]:
    """
    Find pitchers whose last pitch was within `window_min` minutes of now.

    A pitcher is "done" if:
      - The game is Final (all pitchers are done), OR
      - They have pitching stats but aren't the current pitcher (pulled mid-game)

    Returns list of {pitcher_id, pitcher_name, last_pitch_utc}.
    """
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=window_min)

    game_data = feed.get("gameData") or {}
    live_data = feed.get("liveData") or {}
    game_state = ((game_data.get("status") or {}).get("abstractGameState") or "").lower()

    # Current pitcher (only relevant for live games)
    current_pitcher_id = None
    if game_state == "live":
        defense = (live_data.get("linescore") or {}).get("defense") or {}
        current_pitcher_id = (defense.get("pitcher") or {}).get("id")

    # Build map: pitcher_id -> last pitch timestamp
    all_plays = (live_data.get("plays") or {}).get("allPlays") or []
    pitcher_last_pitch: dict[int, datetime] = {}
    pitcher_names: dict[int, str] = {}

    for play in all_plays:
        matchup = play.get("matchup") or {}
        pid = (matchup.get("pitcher") or {}).get("id")
        pname = (matchup.get("pitcher") or {}).get("fullName", "")
        if not pid:
            continue

        for ev in play.get("playEvents") or []:
            if not ev.get("isPitch"):
                continue
            ts_str = ev.get("startTime") or ev.get("endTime") or ""
            if not ts_str:
                continue
            try:
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                if pid not in pitcher_last_pitch or ts > pitcher_last_pitch[pid]:
                    pitcher_last_pitch[pid] = ts
                    pitcher_names[pid] = pname
            except (ValueError, TypeError):
                continue

    # Filter to pitchers who are done AND whose last pitch is recent
    results = []
    for pid, last_ts in pitcher_last_pitch.items():
        # Skip the current pitcher in live games (still pitching)
        if game_state == "live" and pid == current_pitcher_id:
            continue

        # Only include if last pitch is within the window
        if last_ts >= cutoff:
            results.append({
                "pitcher_id": pid,
                "pitcher_name": pitcher_names.get(pid, f"ID {pid}"),
                "last_pitch_utc": last_ts,
            })

    return results


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------
def send_pitcher_email(pdf_path: Path, pitcher_name: str, game_info: dict):
    """Send one email per pitcher with their PDF attached."""
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        log.warning("Gmail credentials not set — skipping email")
        return False

    away = game_info.get("away_abbrev", "?")
    home = game_info.get("home_abbrev", "?")
    date_str = game_info.get("date", "")

    msg = MIMEMultipart()
    msg["From"] = GMAIL_USER
    msg["To"] = NOTIFY_EMAIL
    msg["Subject"] = f"\u26be Pitcher Report: {pitcher_name} ({away} @ {home})"

    body = (
        f"Post-game pitcher report for {pitcher_name}.\n\n"
        f"{game_info.get('away_name', '?')} @ {game_info.get('home_name', '?')}\n"
        f"Date: {date_str}\n"
    )
    msg.attach(MIMEText(body, "plain"))

    pdf_bytes = pdf_path.read_bytes()
    att = MIMEApplication(pdf_bytes, _subtype="pdf")
    att.add_header("Content-Disposition", "attachment", filename=pdf_path.name)
    msg.attach(att)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            smtp.send_message(msg)
        log.info("    Email sent: %s", pitcher_name)
        return True
    except Exception as e:
        log.error("    Email failed for %s: %s", pitcher_name, e)
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def process_games(date_str: str, window_min: int) -> int:
    """Check games for a date, generate + email reports for recently done pitchers."""
    games = fetch_games(date_str)
    if not games:
        return 0

    live_count = sum(1 for g in games if g["status"] == "live")
    final_count = sum(1 for g in games if g["status"] == "final")
    log.info("%s: %d live, %d final", date_str, live_count, final_count)

    total_sent = 0
    for game in games:
        gp = game["game_pk"]
        try:
            feed = _fetch_live_feed(gp)
        except Exception as e:
            log.error("Failed to fetch game %d: %s", gp, e)
            continue

        game_info = _extract_game_info(feed)
        recently_done = find_recently_done_pitchers(feed, window_min)

        if not recently_done:
            continue

        log.info(
            "Game %d (%s @ %s): %d pitchers just finished",
            gp, game["away_abbrev"], game["home_abbrev"], len(recently_done),
        )

        for p in recently_done:
            pid = p["pitcher_id"]
            name = p["pitcher_name"]
            mins_ago = (datetime.now(timezone.utc) - p["last_pitch_utc"]).total_seconds() / 60
            log.info("  %s (last pitch %.0f min ago)", name, mins_ago)

            try:
                pdf_path = generate_report(pid, gp)
                if pdf_path:
                    email_info = {
                        "away_abbrev": game["away_abbrev"],
                        "home_abbrev": game["home_abbrev"],
                        "away_name": game["away_name"],
                        "home_name": game["home_name"],
                        "date": game_info.get("date", date_str),
                    }
                    if send_pitcher_email(pdf_path, name, email_info):
                        total_sent += 1
            except Exception as e:
                log.error("    Failed %s: %s", name, e)

    return total_sent


def main():
    parser = argparse.ArgumentParser(description="Pitcher report emails (timestamp-based dedup)")
    parser.add_argument("--date", type=str, default=None,
                        help="YYYY-MM-DD, 'today', or 'yesterday' (default: check both)")
    parser.add_argument("--window", type=int, default=DEFAULT_WINDOW_MIN,
                        help=f"Minutes to look back for finished pitchers (default: {DEFAULT_WINDOW_MIN})")
    args = parser.parse_args()

    now_et = datetime.now(ET)
    if args.date:
        if args.date.lower() == "today":
            dates = [now_et.strftime("%Y-%m-%d")]
        elif args.date.lower() == "yesterday":
            dates = [(now_et - timedelta(days=1)).strftime("%Y-%m-%d")]
        else:
            dates = [args.date]
    else:
        # Check today and yesterday (late-night games cross midnight)
        today = now_et.strftime("%Y-%m-%d")
        yesterday = (now_et - timedelta(days=1)).strftime("%Y-%m-%d")
        dates = [today, yesterday]

    log.info("=== Pitcher report check (window=%d min) ===", args.window)

    total = 0
    for date_str in dates:
        total += process_games(date_str, args.window)

    if total:
        log.info("=== Sent %d report(s) ===", total)
    else:
        log.info("=== No new reports to send ===")


if __name__ == "__main__":
    main()
