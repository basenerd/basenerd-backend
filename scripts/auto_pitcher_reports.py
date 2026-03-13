#!/usr/bin/env python3
"""
Automatically generate pitcher report PDFs after every MLB game finishes.

Polls the MLB schedule API, detects newly completed games, and generates
reports for every pitcher who appeared. Checks for existing report PDFs
on disk to avoid duplicating work across runs.

Usage:
    # Run once for today (designed for Render cron job):
    python scripts/auto_pitcher_reports.py

    # Run once for a specific date:
    python scripts/auto_pitcher_reports.py --date 2026-03-25

Output: reports/<YYYYMMDD>/<PlayerName>_<game_pk>.pdf
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import smtplib
import sys
from datetime import datetime, timedelta, timezone
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import requests

# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

REPORTS_DIR = ROOT / "reports"
MLB_API = "https://statsapi.mlb.com/api/v1"

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
NOTIFY_EMAIL = "nicklabella6@gmail.com"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("auto_reports")


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------
def send_report_email(pdf_path: Path, pitcher_name: str, game_info: dict):
    """Send a pitcher report PDF as an email attachment."""
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        log.warning("Gmail credentials not configured — skipping email")
        return False

    away = game_info.get("away", "?")
    home = game_info.get("home", "?")

    msg = MIMEMultipart()
    msg["From"] = GMAIL_USER
    msg["To"] = NOTIFY_EMAIL
    msg["Subject"] = f"\u26be Pitcher Report: {pitcher_name} ({away} @ {home})"

    body = (
        f"Pitcher report for {pitcher_name} is attached.\n\n"
        f"{away} @ {home}\n"
    )
    msg.attach(MIMEText(body, "plain"))

    pdf_bytes = pdf_path.read_bytes()
    attachment = MIMEApplication(pdf_bytes, _subtype="pdf")
    attachment.add_header(
        "Content-Disposition", "attachment", filename=pdf_path.name,
    )
    msg.attach(attachment)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            smtp.send_message(msg)
        log.info("    Email sent: %s", pdf_path.name)
        return True
    except Exception as e:
        log.error("    Failed to send email: %s", e)
        return False


# ---------------------------------------------------------------------------
# Check existing reports on disk
# ---------------------------------------------------------------------------
def _game_already_processed(game_pk: int, game_date: str) -> bool:
    """Check if any PDF matching *_<game_pk>.pdf exists in the date folder."""
    date_dir = REPORTS_DIR / game_date.replace("-", "")
    if not date_dir.exists():
        return False
    return any(date_dir.glob(f"*_{game_pk}.pdf"))


# ---------------------------------------------------------------------------
# MLB schedule
# ---------------------------------------------------------------------------
def fetch_schedule(date_str: str) -> list[dict[str, Any]]:
    """Return list of games for a date with status info."""
    url = f"{MLB_API}/schedule"
    params = {"date": date_str, "sportId": 1, "hydrate": "linescore"}
    try:
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
                detailed = (status.get("detailedState") or "")
                games.append({
                    "game_pk": int(gp),
                    "status": abstract,
                    "detailed_status": detailed,
                    "away": ((g.get("teams") or {}).get("away") or {}).get("team", {}).get("name", "?"),
                    "home": ((g.get("teams") or {}).get("home") or {}).get("team", {}).get("name", "?"),
                })
        return games
    except Exception as e:
        log.error("Failed to fetch schedule for %s: %s", date_str, e)
        return []


# ---------------------------------------------------------------------------
# Report generation — imports generate_report directly (same Python process)
# ---------------------------------------------------------------------------
def generate_reports_for_game(game_pk: int, game_meta: dict | None = None) -> int:
    """Generate reports for all pitchers in a game. Returns count of successes."""
    from generate_pitcher_report_pdf import (
        _fetch_live_feed,
        _extract_game_info,
        generate_report,
    )

    log.info("Generating reports for game %d ...", game_pk)
    try:
        feed = _fetch_live_feed(game_pk)
        game_info = _extract_game_info(feed)

        # Extract pitcher IDs and names from boxscore
        pitcher_ids = set()
        pitcher_names: dict[int, str] = {}
        boxscore = (feed.get("liveData") or {}).get("boxscore") or {}
        for side in ("away", "home"):
            team = (boxscore.get("teams") or {}).get(side) or {}
            players = team.get("players") or {}
            for key, pdata in players.items():
                stats = (pdata.get("stats") or {}).get("pitching") or {}
                if stats.get("inningsPitched") or stats.get("numberOfPitches"):
                    pid = pdata.get("person", {}).get("id")
                    if pid:
                        pitcher_ids.add(int(pid))
                        pitcher_names[int(pid)] = pdata.get("person", {}).get("fullName", f"ID {pid}")

        if not pitcher_ids:
            log.warning("  No pitchers found in game %d", game_pk)
            return 0

        log.info("  Found %d pitchers in game %d", len(pitcher_ids), game_pk)

        success_count = 0
        for pid in sorted(pitcher_ids):
            try:
                result = generate_report(pid, game_pk)
                if result:
                    success_count += 1
                    log.info("    Generated: %s", result.name)
                    send_report_email(result, pitcher_names.get(pid, result.stem), game_meta or {})
            except Exception as e:
                log.error("    Failed pitcher %d: %s", pid, e)

        log.info("  Game %d: %d/%d reports generated", game_pk, success_count, len(pitcher_ids))
        return success_count

    except Exception as e:
        log.error("  Failed to process game %d: %s", game_pk, e)
        return 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def process_date(date_str: str) -> int:
    """Check for finished games, generate reports for unprocessed ones."""
    games = fetch_schedule(date_str)
    if not games:
        log.info("No games scheduled for %s", date_str)
        return 0

    final_games = [g for g in games if g["status"] == "final"]
    pending = [g for g in final_games if not _game_already_processed(g["game_pk"], date_str)]
    in_progress = [g for g in games if g["status"] != "final"]

    log.info(
        "%s: %d games total, %d final, %d already done, %d to generate, %d in progress",
        date_str, len(games), len(final_games),
        len(final_games) - len(pending), len(pending), len(in_progress),
    )

    if in_progress:
        for g in in_progress:
            log.info("  In progress: %s @ %s (%s)", g["away"], g["home"], g["detailed_status"])

    total = 0
    for g in pending:
        log.info("Processing: %s @ %s (game_pk=%d)", g["away"], g["home"], g["game_pk"])
        total += generate_reports_for_game(g["game_pk"], game_meta=g)

    return total


def _today_et() -> str:
    """Current date in US Eastern time (MLB game day)."""
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")


def resolve_date(date_arg: str | None) -> str:
    if not date_arg or date_arg.lower() == "today":
        return _today_et()
    if date_arg.lower() == "yesterday":
        from zoneinfo import ZoneInfo
        d = datetime.now(ZoneInfo("America/New_York")) - timedelta(days=1)
        return d.strftime("%Y-%m-%d")
    return date_arg


def main():
    parser = argparse.ArgumentParser(description="Auto-generate pitcher reports after games")
    parser.add_argument("--date", type=str, default=None,
                        help="YYYY-MM-DD, 'today', or 'yesterday' (default: today)")
    args = parser.parse_args()

    if args.date:
        # Explicit date — only check that one
        dates = [resolve_date(args.date)]
    else:
        # Check both today and yesterday (ET). Games played on the evening
        # of March 9 are on the March 9 schedule, but the cron may run
        # after midnight ET (March 10). Need both to catch everything.
        from zoneinfo import ZoneInfo
        now_et = datetime.now(ZoneInfo("America/New_York"))
        today = now_et.strftime("%Y-%m-%d")
        yesterday = (now_et - timedelta(days=1)).strftime("%Y-%m-%d")
        dates = [today, yesterday]

    total = 0
    for date_str in dates:
        log.info("=== Checking %s ===", date_str)
        total += process_date(date_str)

    if total:
        log.info("Done — generated %d report(s)", total)
    else:
        log.info("Done — no new reports to generate.")


if __name__ == "__main__":
    main()
