#!/usr/bin/env python3
"""
Automatically generate and email lineup graphics when lineups are announced.

Polls the MLB schedule API for games with lineups posted, generates the
graphic, converts to PNG, and emails it. Checks disk to avoid duplicates.

Usage:
    # Run once for today (designed for Render cron job):
    python scripts/auto_lineup_emails.py

    # Run once for a specific date:
    python scripts/auto_lineup_emails.py --date 2026-03-25

    # Test email with a fake lineup:
    python scripts/auto_lineup_emails.py --test

Output: reports/lineups/<YYYYMMDD>/<Away>_at_<Home>_<game_pk>.pdf/.png
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import smtplib
import sys
from datetime import datetime, timedelta
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(ROOT))

REPORTS_DIR = ROOT / "reports" / "lineups"
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("lineup_emails")


# ---------------------------------------------------------------------------
# Dedup via notification_log DB table (survives ephemeral containers)
# ---------------------------------------------------------------------------
from notification_log import already_sent, mark_sent, get_entity_key, delete_entry


def _lineup_hash(game: dict) -> str:
    """Hash the lineup player IDs to detect changes."""
    lineups = game.get("lineups") or {}
    away_ids = [str((p.get("id") or "")) for p in (lineups.get("awayPlayers") or [])]
    home_ids = [str((p.get("id") or "")) for p in (lineups.get("homePlayers") or [])]
    raw = ",".join(away_ids) + "|" + ",".join(home_ids)
    return hashlib.md5(raw.encode()).hexdigest()


def _game_already_processed(game_pk: int, date_str: str, lineup_hash: str) -> bool:
    """Check DB for this game. Returns True if already sent with same lineup."""
    prev_hash = get_entity_key("lineup", game_pk)
    if prev_hash is None:
        return False  # Never sent
    if prev_hash != lineup_hash:
        # Lineup changed — delete old entry so we re-send
        log.info("  Lineup changed for game %d (hash %s → %s), will re-send",
                 game_pk, prev_hash[:8], lineup_hash[:8])
        delete_entry("lineup", game_pk, prev_hash)
        return False
    return True  # Same lineup already sent


# ---------------------------------------------------------------------------
# Fetch schedule with lineups
# ---------------------------------------------------------------------------
def fetch_schedule_with_lineups(date_str: str) -> list[dict]:
    """Fetch games for a date, hydrating lineup data (MLB + WBC)."""
    games = []
    # sportId 1 = MLB (regular/spring), 51 = World Baseball Classic
    for sport_id in (1, 51):
        url = f"{MLB_API}/schedule"
        params = {
            "date": date_str,
            "sportId": sport_id,
            "hydrate": "lineups,probablePitcher,team",
        }
        try:
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            for d in r.json().get("dates") or []:
                for g in d.get("games") or []:
                    gp = g.get("gamePk")
                    if not gp:
                        continue
                    lineups = g.get("lineups") or {}
                    away_lineup = lineups.get("awayPlayers") or []
                    home_lineup = lineups.get("homePlayers") or []
                    teams = g.get("teams") or {}
                    away_team = (teams.get("away") or {}).get("team") or {}
                    home_team = (teams.get("home") or {}).get("team") or {}
                    status = ((g.get("status") or {}).get("abstractGameState") or "").lower()

                    games.append({
                        "raw": g,
                        "game_pk": int(gp),
                        "status": status,
                        "has_away_lineup": len(away_lineup) > 0,
                        "has_home_lineup": len(home_lineup) > 0,
                        "away_abbrev": away_team.get("abbreviation", "?"),
                        "home_abbrev": home_team.get("abbreviation", "?"),
                        "away_name": away_team.get("teamName", "?"),
                        "home_name": home_team.get("teamName", "?"),
                    })
        except Exception as e:
            log.error("Failed to fetch schedule (sportId=%d) for %s: %s",
                      sport_id, date_str, e)
    return games


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------
def send_lineup_email(png_path: Path, game_info: dict):
    """Send the lineup graphic PNG as an email attachment."""
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        log.warning("Gmail credentials not configured — skipping email")
        return False

    away = game_info.get("away_abbrev", "?")
    home = game_info.get("home_abbrev", "?")

    msg = MIMEMultipart()
    msg["From"] = GMAIL_USER
    msg["To"] = NOTIFY_EMAIL
    msg["Subject"] = f"\u26be Lineups: {away} @ {home}"

    body = (
        f"Starting lineups for {away} @ {home} are attached.\n\n"
        f"Generated by Basenerd\n"
    )
    msg.attach(MIMEText(body, "plain"))

    png_bytes = png_path.read_bytes()
    img = MIMEImage(png_bytes, _subtype="png")
    img.add_header("Content-Disposition", "attachment", filename=png_path.name)
    msg.attach(img)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            smtp.send_message(msg)
        log.info("    Email sent: %s", png_path.name)
        return True
    except Exception as e:
        log.error("    Failed to send email: %s", e)
        return False


# ---------------------------------------------------------------------------
# Generate + convert + email
# ---------------------------------------------------------------------------
def process_game(game_info: dict, date_str: str) -> bool:
    """Generate lineup graphic, convert to PNG, email it."""
    from generate_lineup_graphic import _extract_game_data, generate_lineup_graphic, _pdf_to_png

    gd = _extract_game_data(game_info["raw"])
    if not gd:
        log.warning("  Could not extract game data for %d", game_info["game_pk"])
        return False

    try:
        pdf_path = generate_lineup_graphic(gd)
        if not pdf_path:
            return False
        png_path = _pdf_to_png(pdf_path)
        send_lineup_email(png_path, game_info)
        return True
    except Exception as e:
        log.error("  Error generating lineup for %s @ %s: %s",
                  game_info["away_abbrev"], game_info["home_abbrev"], e)
        import traceback
        traceback.print_exc()
        return False


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------
def process_date(date_str: str) -> int:
    """Check for games with lineups announced, generate and email new ones."""
    games = fetch_schedule_with_lineups(date_str)
    if not games:
        log.info("No games scheduled for %s", date_str)
        return 0

    # Only process games where BOTH teams have complete lineups
    with_lineups = [
        g for g in games
        if g["has_away_lineup"] and g["has_home_lineup"]
    ]

    # Compute lineup hashes and skip games we already processed (same lineup)
    pending = []
    for g in with_lineups:
        lh = _lineup_hash(g["raw"])
        g["_lineup_hash"] = lh
        if not _game_already_processed(g["game_pk"], date_str, lh):
            pending.append(g)

    log.info(
        "%s: %d games total, %d with both lineups, %d already done, %d to generate",
        date_str, len(games), len(with_lineups),
        len(with_lineups) - len(pending), len(pending),
    )

    total = 0
    for g in pending:
        log.info("Processing: %s @ %s (game_pk=%d)",
                 g["away_abbrev"], g["home_abbrev"], g["game_pk"])
        if process_game(g, date_str):
            # Record in DB so we don't re-send
            mark_sent("lineup", g["game_pk"], g["_lineup_hash"])
            total += 1

    return total


def _today_et() -> str:
    """Current date in US Eastern time."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except ImportError:
        return (datetime.utcnow() - timedelta(hours=5)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Test mode
# ---------------------------------------------------------------------------
def send_test_email():
    """Fetch today's schedule, pick the first game with lineups, and email it."""
    date_str = _today_et()
    log.info("Test mode: checking %s for games with lineups...", date_str)
    games = fetch_schedule_with_lineups(date_str)
    with_lineups = [g for g in games if g["has_away_lineup"] or g["has_home_lineup"]]

    if not with_lineups:
        log.info("No lineups posted yet for %s. Trying a test with the first game...", date_str)
        if not games:
            log.error("No games at all for %s — can't run test.", date_str)
            return
        target = games[0]
    else:
        target = with_lineups[0]

    log.info("Generating test lineup for %s @ %s (game_pk=%d)",
             target["away_abbrev"], target["home_abbrev"], target["game_pk"])
    process_game(target, date_str)


# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Auto-generate and email lineup graphics when lineups are announced")
    parser.add_argument("--date", type=str, default=None,
                        help="YYYY-MM-DD, 'today', or 'yesterday' (default: today)")
    parser.add_argument("--test", action="store_true",
                        help="Generate and email a test lineup graphic")
    args = parser.parse_args()

    if args.test:
        send_test_email()
        return

    if args.date:
        if args.date.lower() == "yesterday":
            date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        elif args.date.lower() == "today":
            date_str = _today_et()
        else:
            date_str = args.date
        dates = [date_str]
    else:
        # Check today only — lineups are pre-game, no need for yesterday
        dates = [_today_et()]

    total = 0
    for date_str in dates:
        log.info("=== Checking %s ===", date_str)
        total += process_date(date_str)

    if total:
        log.info("Done — generated and emailed %d lineup graphic(s)", total)
    else:
        log.info("Done — no new lineups to process.")


if __name__ == "__main__":
    main()
