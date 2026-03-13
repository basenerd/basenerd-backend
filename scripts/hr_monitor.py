#!/usr/bin/env python3
"""
Live Home Run Monitor
=====================
Polls MLB live game feeds for home runs, generates HR graphics,
and emails them in near real-time.

Usage:
    python scripts/hr_monitor.py              # Monitor today's games
    python scripts/hr_monitor.py --date 2026-03-25  # Specific date
    python scripts/hr_monitor.py --test       # Send a test email
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import smtplib
import sys
import time
from datetime import datetime, timedelta
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import requests

# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Load .env
env_path = ROOT / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

MLB_API = "https://statsapi.mlb.com/api/v1"
POLL_INTERVAL = 30  # seconds between checks
GMAIL_USER = os.environ.get("GMAIL_USER", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
NOTIFY_EMAIL = "nicklabella6@gmail.com"

# Track which HRs we've already processed: set of (game_pk, play_index)
_seen_hrs: set[tuple[int, int]] = set()

# MLB team ID -> abbreviation mapping
_TEAM_ABBREV: dict[int, str] = {}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("hr_monitor")


# ---------------------------------------------------------------------------
# MLB API helpers
# ---------------------------------------------------------------------------
def _fetch_json(url: str, params: dict = None, timeout: int = 15) -> dict:
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def fetch_schedule(date_str: str) -> list[dict]:
    """Return list of games for a date."""
    data = _fetch_json(f"{MLB_API}/schedule", {
        "date": date_str, "sportId": 1, "hydrate": "linescore,team",
    })
    games = []
    for d in data.get("dates") or []:
        for g in d.get("games") or []:
            gp = g.get("gamePk")
            if not gp:
                continue
            status = (g.get("status") or {}).get("abstractGameState", "").lower()
            away_team = (g.get("teams") or {}).get("away", {}).get("team", {})
            home_team = (g.get("teams") or {}).get("home", {}).get("team", {})
            # Cache team abbreviations
            for t in (away_team, home_team):
                tid = t.get("id")
                abbr = t.get("abbreviation", "")
                if tid and abbr:
                    _TEAM_ABBREV[tid] = abbr
            games.append({
                "game_pk": int(gp),
                "status": status,
                "away_abbr": away_team.get("abbreviation", ""),
                "home_abbr": home_team.get("abbreviation", ""),
                "away_name": away_team.get("teamName", ""),
                "home_name": home_team.get("teamName", ""),
            })
    return games


def fetch_live_feed(game_pk: int) -> dict:
    """Fetch the full live feed for a game."""
    return _fetch_json(
        f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live",
        timeout=30,
    )


def _team_abbr_from_id(team_id: int) -> str:
    """Look up team abbreviation from cached ID mapping."""
    if team_id in _TEAM_ABBREV:
        return _TEAM_ABBREV[team_id]
    # Fallback: fetch from API
    try:
        data = _fetch_json(f"{MLB_API}/teams/{team_id}")
        teams = data.get("teams") or []
        if teams:
            abbr = teams[0].get("abbreviation", "")
            _TEAM_ABBREV[team_id] = abbr
            return abbr
    except Exception:
        pass
    return ""


# MLB abbreviation -> our stadium code mapping (most are the same)
_MLB_TO_STADIUM = {
    "ARI": "AZ", "WSN": "WSH",
}


def _stadium_code(abbr: str) -> str:
    """Convert MLB abbreviation to our stadium code."""
    return _MLB_TO_STADIUM.get(abbr, abbr)


# ---------------------------------------------------------------------------
# Extract home runs from a live feed
# ---------------------------------------------------------------------------
def extract_home_runs(feed: dict, game_pk: int) -> list[dict]:
    """Parse the live feed and return info for each HR not yet seen."""
    hrs = []
    live = feed.get("liveData") or {}
    plays = live.get("plays") or {}
    all_plays = plays.get("allPlays") or []

    game_data = feed.get("gameData") or {}
    dt_info = game_data.get("datetime") or {}
    game_date = dt_info.get("officialDate") or ""
    teams = game_data.get("teams") or {}
    away_info = teams.get("away") or {}
    home_info = teams.get("home") or {}
    away_abbr = away_info.get("abbreviation", "")
    home_abbr = home_info.get("abbreviation", "")
    away_id = away_info.get("id")
    home_id = home_info.get("id")

    # Cache abbreviations
    if away_id and away_abbr:
        _TEAM_ABBREV[away_id] = away_abbr
    if home_id and home_abbr:
        _TEAM_ABBREV[home_id] = home_abbr

    # Current score from linescore
    linescore = live.get("linescore") or {}
    ls_teams = linescore.get("teams") or {}
    away_score = (ls_teams.get("away") or {}).get("runs")
    home_score = (ls_teams.get("home") or {}).get("runs")

    for idx, play in enumerate(all_plays):
        result = play.get("result") or {}
        event = (result.get("eventType") or "").lower()
        if event not in ("home_run",):
            continue

        key = (game_pk, idx)
        if key in _seen_hrs:
            continue

        # Extract batter info
        matchup = play.get("matchup") or {}
        batter = matchup.get("batter") or {}
        batter_name = batter.get("fullName", "Unknown")
        batter_side = matchup.get("batSide", {}).get("code", "")

        # Determine batter's team
        about = play.get("about") or {}
        is_top = about.get("isTopInning", True)
        batter_team_abbr = away_abbr if is_top else home_abbr

        # Inning text
        inning = about.get("inning", "")
        half = "Top" if is_top else "Bot"
        inning_text = f"{half} {inning}" if inning else ""

        # Hit data + pitch data — find the pitch event with hitData
        hit_data = {}
        pitch_type = None
        pitch_speed = None
        plate_x = None
        plate_z = None
        play_events = play.get("playEvents") or []
        for pe_item in reversed(play_events):
            hd = pe_item.get("hitData")
            if hd:
                hit_data = hd
                # Pitch info from the same event
                details = pe_item.get("details") or {}
                ptype = (details.get("type") or {}).get("description")
                if ptype:
                    pitch_type = ptype
                pd = pe_item.get("pitchData") or {}
                pitch_speed = pd.get("startSpeed")
                coords = pd.get("coordinates") or {}
                plate_x = coords.get("pX")
                plate_z = coords.get("pZ")
                break

        ev = hit_data.get("launchSpeed")
        la = hit_data.get("launchAngle")
        spray = hit_data.get("sprayAngle")
        dist = hit_data.get("totalDistance")

        if ev is None or la is None:
            log.warning("  HR by %s missing hitData, skipping", batter_name)
            _seen_hrs.add(key)
            continue

        hrs.append({
            "key": key,
            "batter_name": batter_name,
            "batter_team": batter_team_abbr,
            "exit_velo": float(ev),
            "launch_angle": float(la),
            "spray_angle": float(spray) if spray is not None else 0.0,
            "distance": float(dist) if dist is not None else None,
            "stadium_code": _stadium_code(home_abbr),
            "game_date": game_date,
            "inning_text": inning_text,
            "away_team": away_abbr,
            "home_team": home_abbr,
            "away_score": away_score,
            "home_score": home_score,
            "description": result.get("description", ""),
            "pitch_type": pitch_type,
            "pitch_speed": float(pitch_speed) if pitch_speed is not None else None,
            "plate_x": float(plate_x) if plate_x is not None else None,
            "plate_z": float(plate_z) if plate_z is not None else None,
        })

    return hrs


# ---------------------------------------------------------------------------
# Generate graphic
# ---------------------------------------------------------------------------
def generate_graphic(hr: dict) -> bytes:
    """Generate the HR image PNG bytes."""
    from services.hr_graphic import generate_hr_image
    return generate_hr_image(
        batter_name=hr["batter_name"],
        exit_velo=hr["exit_velo"],
        launch_angle=hr["launch_angle"],
        spray_angle=hr["spray_angle"],
        distance=hr["distance"],
        stadium_code=hr["stadium_code"],
        game_date=hr["game_date"],
        inning_text=hr["inning_text"],
        away_team=hr["away_team"],
        home_team=hr["home_team"],
        away_score=hr["away_score"],
        home_score=hr["home_score"],
        batter_team=hr["batter_team"],
        pitch_type=hr.get("pitch_type"),
        pitch_speed=hr.get("pitch_speed"),
        plate_x=hr.get("plate_x"),
        plate_z=hr.get("plate_z"),
    )


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------
def send_hr_email(hr: dict, png_bytes: bytes):
    """Send the HR graphic as an email attachment."""
    if not GMAIL_USER or not GMAIL_APP_PASSWORD or GMAIL_APP_PASSWORD == "PASTE_YOUR_APP_PASSWORD_HERE":
        log.error("Gmail credentials not configured in .env — skipping email")
        return False

    msg = MIMEMultipart()
    msg["From"] = GMAIL_USER
    msg["To"] = NOTIFY_EMAIL
    msg["Subject"] = (
        f"\u26be HR: {hr['batter_name']} — "
        f"{hr['exit_velo']:.0f}mph, {hr.get('distance') or '?'}ft "
        f"({hr['away_team']} @ {hr['home_team']})"
    )

    # Text body
    body = (
        f"{hr['batter_name']} ({hr['batter_team']}) hit a home run!\n\n"
        f"Exit Velo: {hr['exit_velo']:.1f} mph\n"
        f"Launch Angle: {hr['launch_angle']:.0f}°\n"
        f"Distance: {hr.get('distance') or 'N/A'} ft\n"
        f"Spray Angle: {hr['spray_angle']:.1f}°\n\n"
        f"{hr['inning_text']} — {hr['away_team']} @ {hr['home_team']}\n"
        f"Score: {hr['away_team']} {hr.get('away_score', '?')} - "
        f"{hr['home_team']} {hr.get('home_score', '?')}\n\n"
        f"{hr.get('description', '')}"
    )
    msg.attach(MIMEText(body, "plain"))

    # Attach PNG
    img = MIMEImage(png_bytes, _subtype="png")
    safe_name = hr["batter_name"].replace(" ", "_")
    img.add_header("Content-Disposition", "attachment",
                   filename=f"HR_{safe_name}.png")
    msg.attach(img)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            smtp.send_message(msg)
        log.info("  Email sent to %s", NOTIFY_EMAIL)
        return True
    except Exception as e:
        log.error("  Failed to send email: %s", e)
        return False


# ---------------------------------------------------------------------------
# Main poll loop
# ---------------------------------------------------------------------------
def poll_once(date_str: str) -> int:
    """Check all live/final games for new HRs. Returns count of new HRs found."""
    games = fetch_schedule(date_str)
    active = [g for g in games if g["status"] in ("live", "final")]
    if not active:
        return 0

    new_hr_count = 0
    for game in active:
        try:
            feed = fetch_live_feed(game["game_pk"])
        except Exception as e:
            log.debug("Failed to fetch feed for %d: %s", game["game_pk"], e)
            continue

        hrs = extract_home_runs(feed, game["game_pk"])
        for hr in hrs:
            log.info(
                "NEW HR: %s (%s) — %s mph, %s ft | %s @ %s",
                hr["batter_name"], hr["batter_team"],
                hr["exit_velo"], hr.get("distance", "?"),
                hr["away_team"], hr["home_team"],
            )
            try:
                png = generate_graphic(hr)
                log.info("  Graphic generated (%d bytes)", len(png))
                send_hr_email(hr, png)
            except Exception as e:
                log.error("  Failed to generate/send graphic: %s", e)

            _seen_hrs.add(hr["key"])
            new_hr_count += 1

    return new_hr_count


def _today_et() -> str:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except ImportError:
        # Python < 3.9 fallback: UTC - 5 (approximate ET)
        return (datetime.utcnow() - timedelta(hours=5)).strftime("%Y-%m-%d")


def run_monitor(date_str: str):
    """Single-date poller. Returns when all games for the date are done."""
    log.info("Monitoring %s ...", date_str)

    consecutive_empty = 0
    max_empty = 120  # ~1 hour of no live/upcoming games → move on

    while True:
        try:
            games = fetch_schedule(date_str)
            live_games = [g for g in games if g["status"] == "live"]
            preview_games = [g for g in games if g["status"] == "preview"]

            if not live_games and not preview_games:
                consecutive_empty += 1
                if consecutive_empty >= max_empty:
                    log.info("No more games for %s. Done.", date_str)
                    return
            else:
                consecutive_empty = 0

            if live_games:
                new = poll_once(date_str)
                if new:
                    log.info("Found %d new HR(s) this cycle", new)

        except KeyboardInterrupt:
            raise
        except Exception as e:
            log.error("Poll error: %s", e)

        time.sleep(POLL_INTERVAL)


def run_daemon():
    """Persistent service that runs forever, auto-tracking the current date.
    Designed for deployment as a Render Background Worker."""
    log.info("=" * 60)
    log.info("HR Monitor DAEMON started")
    log.info("Polling every %ds. Email: %s", POLL_INTERVAL, NOTIFY_EMAIL)
    log.info("=" * 60)

    while True:
        try:
            today = _today_et()
            log.info("=== Date: %s ===", today)
            _seen_hrs.clear()  # fresh day, fresh tracking

            # Check if there are any games today
            games = fetch_schedule(today)
            if not games:
                log.info("No games scheduled for %s. Sleeping 30 min.", today)
                time.sleep(1800)
                continue

            live = [g for g in games if g["status"] == "live"]
            preview = [g for g in games if g["status"] == "preview"]
            final = [g for g in games if g["status"] == "final"]
            log.info(
                "%d games: %d live, %d preview, %d final",
                len(games), len(live), len(preview), len(final),
            )

            if live or preview:
                # Games are active or upcoming — poll until done
                run_monitor(today)
            else:
                # All games already final (late start?)
                # Still check for any HRs we haven't seen
                poll_once(today)
                log.info("All games final for %s. Sleeping 30 min.", today)
                time.sleep(1800)
                continue

            # After games finish, check if date changed
            if _today_et() == today:
                # Same day, all games done — sleep until midnight-ish
                log.info("All games done for %s. Sleeping 30 min.", today)
                time.sleep(1800)

        except KeyboardInterrupt:
            log.info("Interrupted — shutting down.")
            break
        except Exception as e:
            log.error("Daemon error: %s", e)
            time.sleep(60)  # back off on errors

    log.info("HR Monitor daemon stopped. Total HRs: %d", len(_seen_hrs))


# ---------------------------------------------------------------------------
# Test mode
# ---------------------------------------------------------------------------
def send_test_email():
    """Generate a test graphic and send it to verify email works."""
    log.info("Generating test HR graphic...")
    from services.hr_graphic import generate_hr_image
    png = generate_hr_image(
        batter_name="Test Player",
        exit_velo=105.0,
        launch_angle=28.0,
        spray_angle=-10.0,
        distance=410.0,
        stadium_code="NYY",
        game_date=_today_et(),
        inning_text="Top 5th",
        away_team="BOS",
        home_team="NYY",
        away_score=3,
        home_score=2,
        batter_team="BOS",
        pitch_type="4-Seam Fastball",
        pitch_speed=96.3,
        plate_x=-0.25,
        plate_z=2.8,
    )
    log.info("Test graphic generated (%d bytes)", len(png))

    hr = {
        "batter_name": "Test Player",
        "batter_team": "BOS",
        "exit_velo": 105.0,
        "launch_angle": 28.0,
        "spray_angle": -10.0,
        "distance": 410.0,
        "away_team": "BOS",
        "home_team": "NYY",
        "away_score": 3,
        "home_score": 2,
        "inning_text": "Top 5th",
        "description": "This is a test email from the HR Monitor.",
    }
    ok = send_hr_email(hr, png)
    if ok:
        log.info("Test email sent successfully!")
    else:
        log.error("Test email failed. Check your GMAIL_APP_PASSWORD in .env")


# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Live HR Monitor — generates graphics and emails them")
    parser.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (default: today ET)")
    parser.add_argument("--test", action="store_true", help="Send a test email and exit")
    parser.add_argument("--daemon", action="store_true",
                        help="Run forever as a persistent service (for Render/hosting)")
    args = parser.parse_args()

    if args.test:
        send_test_email()
        return

    if args.daemon:
        run_daemon()
        return

    date_str = args.date or _today_et()
    run_monitor(date_str)


if __name__ == "__main__":
    main()
