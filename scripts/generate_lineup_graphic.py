#!/usr/bin/env python3
"""
Generate a pre-game starting lineup graphic (Twitter-optimized 1080x1350 portrait).

Pulls lineup, probable pitcher, and season stats from the MLB Stats API.
Uses team logos from static/team_logos/ and player headshots from mlbstatic.com.

Usage:
    python scripts/generate_lineup_graphic.py --game_pk 831616
    python scripts/generate_lineup_graphic.py --date 2026-03-13
    python scripts/generate_lineup_graphic.py --date today

Output: reports/lineups/<date>/<Away_vs_Home>_<game_pk>.pdf
"""
from __future__ import annotations

import argparse
import io
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from PIL import Image as PILImage, ImageDraw

from reportlab.lib.colors import Color, HexColor, white
from reportlab.pdfgen import canvas as rl_canvas
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# Register Impact font for bold condensed look
IMPACT_PATH = Path("C:/Windows/Fonts/impact.ttf")
if IMPACT_PATH.exists():
    pdfmetrics.registerFont(TTFont("Impact", str(IMPACT_PATH)))
    FONT_BOLD = "Impact"
else:
    FONT_BOLD = "Helvetica-Bold"

FONT_REG = "Helvetica"
FONT_REG_BOLD = "Helvetica-Bold"

# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
LOGO_PATH = ROOT / "static" / "basenerd-logo-official.png"
TEAM_LOGOS_DIR = ROOT / "static" / "team_logos"
REPORTS_DIR = ROOT / "reports" / "lineups"

MLB_API = "https://statsapi.mlb.com/api/v1"

# ---------------------------------------------------------------------------
# Page dimensions — 1080x1350 portrait (4:5, optimal for Twitter/X mobile)
# ---------------------------------------------------------------------------
W = 1080
H = 1350
PAD = 28
CARD_R = 12
GAP = 10

# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------
BG = HexColor("#0b1220")
BG_CARD = HexColor("#131c2e")
BG_TABLE_ROW = HexColor("#182338")
ACCENT = HexColor("#38bdf8")
TEXT_PRIMARY = HexColor("#f1f5f9")
TEXT_SECONDARY = HexColor("#94a3b8")
TEXT_MUTED = HexColor("#64748b")
BORDER = HexColor("#1e3a5f")
WHITE = white
GREEN_GOOD = HexColor("#22c55e")
RED_BAD = HexColor("#ef4444")

# Team primary colors
TEAM_COLORS = {
    "AZ":  "#a71930", "ATL": "#ce1141", "BAL": "#df4601", "BOS": "#bd3039",
    "CHC": "#0e3386", "CIN": "#c6011f", "CLE": "#00385d", "COL": "#333366",
    "CWS": "#27251f", "DET": "#0c2340", "HOU": "#002d62", "KC":  "#004687",
    "LAA": "#ba0021", "LAD": "#005a9c", "MIA": "#00a3e0", "MIL": "#ffc52f",
    "MIN": "#002b5c", "NYM": "#002d72", "NYY": "#003087", "OAK": "#003831",
    "PHI": "#e81828", "PIT": "#fdb827", "SD":  "#2f241d", "SF":  "#fd5a1e",
    "SEA": "#0c2c56", "STL": "#c41e3a", "TB":  "#092c5c", "TEX": "#003278",
    "TOR": "#134a8e", "WSH": "#ab0003",
    # WBC teams
    "USA": "#002868", "CAN": "#d52b1e", "JPN": "#bc002d", "KOR": "#003478",
    "MEX": "#006847", "COL": "#333366", "DOM": "#002d62", "VEN": "#ffcc00",
    "PUR": "#ed0a3f", "CUB": "#002a8f", "PAN": "#d21034", "AUS": "#00843d",
    "TPE": "#00247d", "CZE": "#d7141a", "NED": "#ff6600", "GBR": "#00247d",
}

# Brighter/lighter accent versions of team colors for text on dark bg
TEAM_ACCENT_COLORS = {
    "AZ":  "#e8264a", "ATL": "#f23d5f", "BAL": "#ff6b1a", "BOS": "#e8424a",
    "CHC": "#2563eb", "CIN": "#ef4444", "CLE": "#3b82f6", "COL": "#6366f1",
    "CWS": "#a8a29e", "DET": "#3b82f6", "HOU": "#f97316", "KC":  "#60a5fa",
    "LAA": "#ef4444", "LAD": "#3b82f6", "MIA": "#22d3ee", "MIL": "#fbbf24",
    "MIN": "#3b82f6", "NYM": "#3b82f6", "NYY": "#3b82f6", "OAK": "#22c55e",
    "PHI": "#ef4444", "PIT": "#fbbf24", "SD":  "#a16207", "SF":  "#fb923c",
    "SEA": "#22d3ee", "STL": "#ef4444", "TB":  "#60a5fa", "TEX": "#60a5fa",
    "TOR": "#60a5fa", "WSH": "#ef4444",
    # WBC teams
    "USA": "#3b82f6", "CAN": "#ef4444", "JPN": "#ef4444", "KOR": "#60a5fa",
    "MEX": "#22c55e", "DOM": "#3b82f6", "VEN": "#fbbf24",
    "PUR": "#ef4444", "CUB": "#3b82f6", "PAN": "#ef4444", "AUS": "#22c55e",
    "TPE": "#3b82f6", "CZE": "#ef4444", "NED": "#fb923c", "GBR": "#3b82f6",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _rounded_rect(c, x, y, w, h, r, fill_color=None, stroke_color=None):
    c.saveState()
    if fill_color:
        c.setFillColor(fill_color)
    if stroke_color:
        c.setStrokeColor(stroke_color)
        c.setLineWidth(1)
    r = min(r, w / 2, h / 2)
    c.roundRect(x, y, w, h, r,
                fill=1 if fill_color else 0,
                stroke=1 if stroke_color else 0)
    c.restoreState()


def _get_last_name(full_name: str) -> str:
    """Extract last name, handling suffixes like Jr., III, etc."""
    parts = full_name.strip().split()
    if len(parts) <= 1:
        return full_name.upper()
    # Check for suffixes
    suffixes = {"jr.", "jr", "sr.", "sr", "ii", "iii", "iv", "v"}
    if len(parts) >= 3 and parts[-1].lower().rstrip(".") in suffixes:
        return f"{parts[-2]} {parts[-1]}".upper()
    return parts[-1].upper()


# ---------------------------------------------------------------------------
# MLB API data fetching
# ---------------------------------------------------------------------------
def _fetch_schedule(date_str: str, sport_ids: list = None) -> List[dict]:
    """Fetch games for a date with lineups and probable pitchers."""
    if sport_ids is None:
        sport_ids = [1]
    games = []
    for sid in sport_ids:
        url = f"{MLB_API}/schedule"
        params = {
            "date": date_str,
            "sportId": sid,
            "hydrate": "lineups,probablePitcher,team",
        }
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        for d in r.json().get("dates") or []:
            for g in d.get("games") or []:
                games.append(g)
    return games


def _fetch_headshot(pid: int, size: int = 180) -> Optional[PILImage.Image]:
    url = f"https://img.mlbstatic.com/mlb-photos/image/upload/w_{size},q_100/v1/people/{pid}/headshot/67/current"
    try:
        r = requests.get(url, timeout=8)
        if r.status_code == 200 and len(r.content) > 1000:
            return PILImage.open(io.BytesIO(r.content)).convert("RGBA")
    except Exception:
        pass
    return None


ABBREV_MAP = {"ATH": "OAK"}  # API returns ATH for Athletics, logos use OAK

def _load_team_logo(abbrev: str, team_id: int = None) -> Optional[PILImage.Image]:
    """Load team logo PNG from static/team_logos/, or fetch from mlbstatic by team_id."""
    abbrev = ABBREV_MAP.get(abbrev, abbrev)
    png_path = TEAM_LOGOS_DIR / f"{abbrev}.png"
    if png_path.exists():
        try:
            return PILImage.open(str(png_path)).convert("RGBA")
        except Exception:
            pass
    # Fallback: fetch SVG from mlbstatic and convert via cairosvg or Pillow
    if team_id:
        try:
            url = f"https://www.mlbstatic.com/team-logos/{team_id}.svg"
            r = requests.get(url, timeout=8)
            if r.status_code == 200 and len(r.content) > 100:
                try:
                    import cairosvg
                    png_data = cairosvg.svg2png(bytestring=r.content, output_width=200, output_height=200)
                    return PILImage.open(io.BytesIO(png_data)).convert("RGBA")
                except ImportError:
                    pass
                # Try Pillow SVG (limited support)
                try:
                    return PILImage.open(io.BytesIO(r.content)).convert("RGBA")
                except Exception:
                    pass
        except Exception:
            pass
    return None


def _fetch_player_stats(player_ids: List[int], season: int) -> Dict[int, dict]:
    """Fetch season hitting/pitching stats for a batch of players."""
    if not player_ids:
        return {}
    ids_str = ",".join(str(pid) for pid in player_ids)
    url = f"{MLB_API}/people"
    params = {
        "personIds": ids_str,
        "hydrate": f"stats(group=[hitting,pitching],type=[season],season={season})",
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return {}

    result = {}
    for person in data.get("people") or []:
        pid = person.get("id")
        if not pid:
            continue
        info = {
            "fullName": person.get("fullName", ""),
            "primaryNumber": person.get("primaryNumber", ""),
            "batSide": (person.get("batSide") or {}).get("code", ""),
            "pitchHand": (person.get("pitchHand") or {}).get("code", ""),
            "primaryPosition": (person.get("primaryPosition") or {}).get("abbreviation", ""),
            "hitting": {},
            "pitching": {},
        }
        for stat_group in person.get("stats") or []:
            group_name = (stat_group.get("group") or {}).get("displayName", "")
            splits = stat_group.get("splits") or []
            if not splits:
                continue
            stat = splits[-1].get("stat") or {}
            if group_name == "hitting":
                info["hitting"] = {
                    "avg": stat.get("avg", "--"),
                    "ops": stat.get("ops", "--"),
                    "hr": stat.get("homeRuns", "--"),
                    "rbi": stat.get("rbi", "--"),
                    "sb": stat.get("stolenBases", "--"),
                    "gamesPlayed": stat.get("gamesPlayed", 0),
                }
            elif group_name == "pitching":
                info["pitching"] = {
                    "era": stat.get("era", "--"),
                    "w": stat.get("wins", "--"),
                    "l": stat.get("losses", "--"),
                    "so": stat.get("strikeOuts", "--"),
                    "whip": stat.get("whip", "--"),
                    "ip": stat.get("inningsPitched", "--"),
                    "gamesPlayed": stat.get("gamesPlayed", 0),
                }
        result[pid] = info
    return result


def _extract_game_data(game: dict) -> Optional[dict]:
    """Extract structured game data from schedule API response."""
    game_pk = game.get("gamePk")
    if not game_pk:
        return None

    gd = game.get("gameDate", "")
    official_date = game.get("officialDate", "")
    venue = (game.get("venue") or {}).get("name", "")
    status = (game.get("status") or {}).get("detailedState", "")

    teams = game.get("teams") or {}
    away_team = (teams.get("away") or {}).get("team") or {}
    home_team = (teams.get("home") or {}).get("team") or {}

    away_pp = (teams.get("away") or {}).get("probablePitcher") or {}
    home_pp = (teams.get("home") or {}).get("probablePitcher") or {}

    lineups = game.get("lineups") or {}
    away_lineup = lineups.get("awayPlayers") or []
    home_lineup = lineups.get("homePlayers") or []

    # Parse game time
    game_time = ""
    if gd:
        try:
            dt = datetime.fromisoformat(gd.replace("Z", "+00:00"))
            et = dt + timedelta(hours=-4)  # EDT offset
            hour = et.hour % 12 or 12
            ampm = "PM" if et.hour >= 12 else "AM"
            game_time = f"{hour}:{et.minute:02d} {ampm} ET"
        except Exception:
            try:
                dt = datetime.fromisoformat(gd.replace("Z", "+00:00"))
                game_time = dt.strftime("%H:%M UTC")
            except Exception:
                pass

    return {
        "game_pk": int(game_pk),
        "date": official_date,
        "game_time": game_time,
        "venue": venue,
        "status": status,
        "away": {
            "name": away_team.get("teamName", ""),
            "full_name": away_team.get("name", ""),
            "abbrev": away_team.get("abbreviation", ""),
            "id": away_team.get("id"),
        },
        "home": {
            "name": home_team.get("teamName", ""),
            "full_name": home_team.get("name", ""),
            "abbrev": home_team.get("abbreviation", ""),
            "id": home_team.get("id"),
        },
        "away_pitcher": {
            "id": away_pp.get("id"),
            "name": away_pp.get("fullName", "TBD"),
        },
        "home_pitcher": {
            "id": home_pp.get("id"),
            "name": home_pp.get("fullName", "TBD"),
        },
        "away_lineup": [
            {
                "id": p.get("id"),
                "name": p.get("fullName", ""),
                "position": (p.get("primaryPosition") or {}).get("abbreviation", ""),
            }
            for p in away_lineup
        ],
        "home_lineup": [
            {
                "id": p.get("id"),
                "name": p.get("fullName", ""),
                "position": (p.get("primaryPosition") or {}).get("abbreviation", ""),
            }
            for p in home_lineup
        ],
    }


# ---------------------------------------------------------------------------
# Drawing helpers
# ---------------------------------------------------------------------------
def _draw_circular_headshot(c, img, x, y, size):
    """Draw a circular-cropped headshot on the canvas."""
    if img is None:
        c.saveState()
        c.setFillColor(BG_TABLE_ROW)
        c.circle(x + size / 2, y + size / 2, size / 2, fill=1, stroke=0)
        c.restoreState()
        return
    try:
        sz = min(img.size)
        left = (img.width - sz) // 2
        top = (img.height - sz) // 2
        img = img.crop((left, top, left + sz, top + sz))
        res = max(int(size * 2), 128)
        img = img.resize((res, res),
                         PILImage.LANCZOS if hasattr(PILImage, 'LANCZOS') else PILImage.ANTIALIAS)
        mask = PILImage.new("L", img.size, 0)
        ImageDraw.Draw(mask).ellipse([0, 0, img.size[0] - 1, img.size[1] - 1], fill=255)
        img.putalpha(mask)
        c.drawImage(ImageReader(img), x, y, width=size, height=size, mask="auto")
    except Exception:
        pass


def _draw_team_logo(c, abbrev, x, y, size, team_id=None):
    """Draw team logo on canvas, or fallback to abbreviation text."""
    logo = _load_team_logo(abbrev, team_id=team_id)
    if logo:
        try:
            c.drawImage(ImageReader(logo), x, y, width=size, height=size, mask="auto")
            return
        except Exception:
            pass
    # Fallback: draw abbreviation in a circle
    cx, cy = x + size / 2, y + size / 2
    c.saveState()
    mapped = ABBREV_MAP.get(abbrev, abbrev)
    clr = HexColor(TEAM_COLORS.get(mapped, "#1e3a5f"))
    c.setFillColor(clr)
    c.circle(cx, cy, size / 2, fill=1, stroke=0)
    c.setFillColor(white)
    fsz = size * 0.38
    c.setFont(FONT_BOLD, fsz)
    c.drawCentredString(cx, cy - fsz * 0.35, mapped)
    c.restoreState()


# ---------------------------------------------------------------------------
# Main graphic generation
# ---------------------------------------------------------------------------
def _color_with_alpha(hex_color, alpha):
    """Create a Color with alpha from a HexColor."""
    return Color(hex_color.red, hex_color.green, hex_color.blue, alpha)


def _draw_gradient_bar(c, x, y, w, h, color, direction="right"):
    """Draw a faux-gradient bar using 20 slices fading to transparent."""
    steps = 20
    sw = w / steps
    for i in range(steps):
        if direction == "right":
            a = 0.35 * (1 - i / steps)
        else:
            a = 0.35 * (i / steps)
        c.setFillColor(Color(color.red, color.green, color.blue, a))
        c.rect(x + i * sw, y, sw + 0.5, h, fill=1, stroke=0)


def generate_lineup_graphic(game_data: dict, out_dir: Optional[Path] = None) -> Optional[Path]:
    """Generate the lineup graphic PDF for a single game — bold poster style."""
    game_pk = game_data["game_pk"]
    date = game_data.get("date", "")
    away = game_data["away"]
    home = game_data["home"]

    print(f"  Generating lineup graphic: {away['abbrev']} @ {home['abbrev']} (game {game_pk})")

    # Collect all player IDs for batch stats fetch
    all_ids = []
    for p in game_data.get("away_lineup") or []:
        if p.get("id"):
            all_ids.append(p["id"])
    for p in game_data.get("home_lineup") or []:
        if p.get("id"):
            all_ids.append(p["id"])
    if game_data["away_pitcher"].get("id"):
        all_ids.append(game_data["away_pitcher"]["id"])
    if game_data["home_pitcher"].get("id"):
        all_ids.append(game_data["home_pitcher"]["id"])

    season = int(date[:4]) if date else datetime.now().year
    prev_season = season - 1

    print(f"  Fetching stats for {len(all_ids)} players...")
    stats_map = _fetch_player_stats(all_ids, prev_season)
    current_stats = _fetch_player_stats(all_ids, season)
    for pid, s in current_stats.items():
        has_hitting = (s.get("hitting") or {}).get("gamesPlayed", 0) > 0
        has_pitching = (s.get("pitching") or {}).get("gamesPlayed", 0) > 0
        if has_hitting or has_pitching:
            stats_map[pid] = s

    if out_dir is None:
        out_dir = REPORTS_DIR / (date.replace("-", "") or "unknown")
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{away['abbrev']}_at_{home['abbrev']}_{game_pk}.pdf"
    out_path = out_dir / filename

    away_abbr = ABBREV_MAP.get(away["abbrev"], away["abbrev"])
    home_abbr = ABBREV_MAP.get(home["abbrev"], home["abbrev"])
    away_accent = HexColor(TEAM_ACCENT_COLORS.get(away_abbr, "#38bdf8"))
    home_accent = HexColor(TEAM_ACCENT_COLORS.get(home_abbr, "#38bdf8"))
    away_dark = HexColor(TEAM_COLORS.get(away_abbr, "#1e3a5f"))
    home_dark = HexColor(TEAM_COLORS.get(home_abbr, "#1e3a5f"))

    c = rl_canvas.Canvas(str(out_path), pagesize=(W, H))
    M = 20  # tight margins

    # === BACKGROUND ===
    c.setFillColor(BG)
    c.rect(0, 0, W, H, fill=1, stroke=0)

    # Subtle diagonal texture
    c.saveState()
    c.setStrokeColor(Color(1, 1, 1, 0.015))
    c.setLineWidth(0.5)
    for tx in range(-H, W + H, 50):
        c.line(tx, 0, tx + H, H)
    c.restoreState()

    cw = W - M * 2
    col_w = (cw - 8) / 2  # 8px gap between columns

    # ============================================================
    # HEADER — full-width matchup banner
    # ============================================================
    hdr_h = 100
    hdr_y = H - M - hdr_h

    # Dark card background
    _rounded_rect(c, M, hdr_y, cw, hdr_h, 10, fill_color=BG_CARD)

    # Team color bars across top
    c.saveState()
    c.setFillColor(away_accent)
    c.rect(M, hdr_y + hdr_h - 4, cw / 2, 4, fill=1, stroke=0)
    c.setFillColor(home_accent)
    c.rect(M + cw / 2, hdr_y + hdr_h - 4, cw / 2, 4, fill=1, stroke=0)
    c.restoreState()

    # Away side — logo + team name filling full height
    logo_sz = 72
    logo_pad = 14
    hdr_mid = hdr_y + hdr_h / 2
    _draw_team_logo(c, away["abbrev"], M + logo_pad, hdr_mid - logo_sz / 2, logo_sz, team_id=away.get("id"))

    # Auto-size team name to fill vertical space
    away_name = away["name"].upper()
    tx_l = M + logo_pad + logo_sz + 10
    away_name_max_w = cw / 2 - logo_pad - logo_sz - 60  # leave room for center
    away_fsz = 52
    while away_fsz > 20:
        if c.stringWidth(away_name, FONT_BOLD, away_fsz) <= away_name_max_w:
            break
        away_fsz -= 1
    c.setFillColor(WHITE)
    c.setFont(FONT_BOLD, away_fsz)
    c.drawString(tx_l, hdr_mid - away_fsz * 0.35, away_name)

    # Home side — right-aligned, filling full height
    _draw_team_logo(c, home["abbrev"], M + cw - logo_pad - logo_sz, hdr_mid - logo_sz / 2, logo_sz, team_id=home.get("id"))

    home_name = home["name"].upper()
    tx_r = M + cw - logo_pad - logo_sz - 10
    home_name_max_w = cw / 2 - logo_pad - logo_sz - 60
    home_fsz = 52
    while home_fsz > 20:
        if c.stringWidth(home_name, FONT_BOLD, home_fsz) <= home_name_max_w:
            break
        home_fsz -= 1
    c.setFillColor(WHITE)
    c.setFont(FONT_BOLD, home_fsz)
    c.drawRightString(tx_r, hdr_mid - home_fsz * 0.35, home_name)

    # Center — game details prominent
    cx = M + cw / 2
    game_info = game_data.get("game_time", "")
    venue = game_data.get("venue", "")
    date_display = ""
    if date:
        try:
            dt = datetime.strptime(date, "%Y-%m-%d")
            date_display = dt.strftime("%b %d, %Y").upper()
        except Exception:
            date_display = date

    # Time big and bold
    if game_info:
        c.setFillColor(WHITE)
        c.setFont(FONT_BOLD, 28)
        c.drawCentredString(cx, hdr_mid + 14, game_info)

    # Date below
    if date_display:
        c.setFillColor(TEXT_SECONDARY)
        c.setFont(FONT_BOLD, 14)
        c.drawCentredString(cx, hdr_mid - 8, date_display)

    # Venue below date
    if venue:
        c.setFillColor(TEXT_MUTED)
        c.setFont(FONT_REG, 11)
        c.drawCentredString(cx, hdr_mid - 26, venue)

    # Basenerd logo bottom-center of header
    bn_sz = 20
    if LOGO_PATH.exists():
        try:
            lg = PILImage.open(str(LOGO_PATH)).convert("RGBA")
            c.drawImage(ImageReader(lg), cx - bn_sz / 2, hdr_y + 4, width=bn_sz, height=bn_sz, mask="auto")
        except Exception:
            pass

    # ============================================================
    # TWO COLUMNS — each is a self-contained lineup card
    # ============================================================
    col_top = hdr_y - 5
    col_bot = M + 22  # footer space
    col_h = col_top - col_bot

    # Inside each column: header(36) + 9 rows + SP section(90)
    col_hdr_h = 36
    sp_h = 90
    row_area = col_h - col_hdr_h - sp_h
    row_h = row_area / 9

    for side, (lineup, tinfo, accent, dark, pitcher) in enumerate([
        (game_data.get("away_lineup") or [], away, away_accent, away_dark, game_data["away_pitcher"]),
        (game_data.get("home_lineup") or [], home, home_accent, home_dark, game_data["home_pitcher"]),
    ]):
        x0 = M if side == 0 else M + col_w + 8

        # Card background
        _rounded_rect(c, x0, col_bot, col_w, col_h, 10, fill_color=BG_CARD)

        # Clipped team-color gradient wash at top
        c.saveState()
        p = c.beginPath()
        p.roundRect(x0, col_bot, col_w, col_h, 10)
        c.clipPath(p, stroke=0)
        _draw_gradient_bar(c, x0, col_top - 80, col_w, 80, accent,
                           "right" if side == 0 else "left")
        c.restoreState()

        # Accent edge bar
        c.saveState()
        c.setFillColor(accent)
        bx = x0 if side == 0 else x0 + col_w - 5
        c.rect(bx, col_bot, 5, col_h, fill=1, stroke=0)
        c.restoreState()

        # --- Column header bar ---
        ch_y = col_top - col_hdr_h
        # Dark team color fill
        c.saveState()
        _rounded_rect(c, x0, ch_y, col_w, col_hdr_h, 10, fill_color=dark)
        c.setFillColor(dark)
        c.rect(x0, ch_y, col_w, 10, fill=1, stroke=0)  # square bottom
        c.restoreState()

        # Logo + team name in header
        slg = 26
        slg_y = ch_y + (col_hdr_h - slg) / 2
        if side == 0:
            _draw_team_logo(c, tinfo["abbrev"], x0 + 10, slg_y, slg, team_id=tinfo.get("id"))
            c.setFillColor(WHITE)
            c.setFont(FONT_BOLD, 17)
            c.drawString(x0 + 10 + slg + 6, ch_y + 11,
                         f"{tinfo['name'].upper()} LINEUP")
        else:
            _draw_team_logo(c, tinfo["abbrev"], x0 + col_w - 10 - slg, slg_y, slg, team_id=tinfo.get("id"))
            c.setFillColor(WHITE)
            c.setFont(FONT_BOLD, 17)
            c.drawRightString(x0 + col_w - 10 - slg - 6, ch_y + 11,
                              f"{tinfo['name'].upper()} LINEUP")

        if not lineup:
            c.setFillColor(TEXT_MUTED)
            c.setFont(FONT_BOLD, 24)
            c.drawCentredString(x0 + col_w / 2, col_bot + col_h / 2, "LINEUP TBD")
            continue

        # --- 9 Batter rows ---
        rows_top = ch_y

        # Stats zone takes right ~45% of column, name takes left ~55%
        stats_zone_w = col_w * 0.44
        name_zone_w = col_w - stats_zone_w

        for i, player in enumerate(lineup[:9]):
            pid = player.get("id")
            ry = rows_top - (i + 1) * row_h

            # Alternating row bg
            if i % 2 == 1:
                c.saveState()
                c.setFillColor(Color(0.09, 0.14, 0.22, 0.7))
                c.rect(x0 + 5, ry, col_w - 10, row_h, fill=1, stroke=0)
                c.restoreState()

            # Separator line
            if i > 0:
                c.setStrokeColor(HexColor("#1e2d4a"))
                c.setLineWidth(0.5)
                c.line(x0 + 10, ry + row_h, x0 + col_w - 10, ry + row_h)

            pstats = stats_map.get(pid) or {}
            pos = player.get("position", "")
            if not pos and pstats.get("primaryPosition"):
                pos = pstats["primaryPosition"]
            bat_side = pstats.get("batSide", "")
            row_mid = ry + row_h / 2

            # ======== LEFT SIDE: order + pos + name ========

            # Order number — vertically centered
            c.setFillColor(accent)
            c.setFont(FONT_BOLD, 28)
            c.drawString(x0 + 10, row_mid - 10, str(i + 1))

            # Position badge — vertically centered
            pos_badge_x = x0 + 36
            pos_badge_w = 36
            pos_badge_h = 22
            pos_badge_y = row_mid - 10
            c.saveState()
            c.setFillColor(Color(accent.red, accent.green, accent.blue, 0.18))
            c.roundRect(pos_badge_x, pos_badge_y, pos_badge_w, pos_badge_h, 4,
                        fill=1, stroke=0)
            c.restoreState()
            c.setFillColor(accent)
            c.setFont(FONT_BOLD, 14)
            c.drawCentredString(pos_badge_x + pos_badge_w / 2, pos_badge_y + 4, pos)

            # Last name — big, vertically centered
            name_left = pos_badge_x + pos_badge_w + 6
            last_name = _get_last_name(player.get("name", ""))
            name_avail = name_zone_w - (name_left - x0) - 4
            fsz = 42
            while fsz > 18:
                if c.stringWidth(last_name, FONT_BOLD, fsz) <= name_avail:
                    break
                fsz -= 1

            # Vertically center name with order number / position badge
            # Order num & badge are centered around row_mid - 10 to row_mid + 12 (midpoint ~ row_mid + 1)
            # For the name, set baseline so its visual center aligns with that midpoint
            badge_mid = pos_badge_y + pos_badge_h / 2
            name_y = badge_mid - fsz * 0.35
            bat_y = name_y - 14

            c.setFillColor(WHITE)
            c.setFont(FONT_BOLD, fsz)
            c.drawString(name_left, name_y, last_name)

            # Bat side small under name
            if bat_side:
                c.setFillColor(TEXT_MUTED)
                c.setFont(FONT_REG, 9)
                c.drawString(name_left, bat_y, f"Bats {bat_side}")

            # ======== RIGHT SIDE: stats grid ========
            hitting = pstats.get("hitting") or {}
            sx0 = x0 + name_zone_w  # stats zone left edge
            sx_r = x0 + col_w - 8   # stats zone right edge
            s_w = sx_r - sx0         # total stats width

            if hitting and hitting.get("gamesPlayed", 0) > 0:
                avg = str(hitting.get("avg", "--"))
                ops = str(hitting.get("ops", "--"))
                hr = str(hitting.get("hr", "--"))
                rbi = str(hitting.get("rbi", "--"))
                sb = str(hitting.get("sb", "--"))

                # 3 columns across, 2 rows deep
                # Top row: AVG (big), OPS, HR
                # Bottom row: label under each, plus RBI, SB
                n_cols = 5
                cell_w = s_w / n_cols

                stats_list = [
                    (avg, "AVG", 20, WHITE),
                    (ops, "OPS", 18, TEXT_PRIMARY),
                    (hr, "HR", 18, TEXT_SECONDARY),
                    (rbi, "RBI", 18, TEXT_SECONDARY),
                    (sb, "SB", 18, TEXT_SECONDARY),
                ]

                for j, (val, lbl, fsize, clr) in enumerate(stats_list):
                    cx_stat = sx0 + cell_w * j + cell_w / 2

                    # Value — big, centered on badge midpoint
                    c.setFillColor(clr)
                    c.setFont(FONT_BOLD, fsize)
                    c.drawCentredString(cx_stat, badge_mid - fsize * 0.35 + 4, val)

                    # Label — small below
                    c.setFillColor(TEXT_MUTED)
                    c.setFont(FONT_REG, 8)
                    c.drawCentredString(cx_stat, badge_mid - fsize * 0.35 - 10, lbl)

        # --- Starting Pitcher section at bottom ---
        sp_top = col_bot + sp_h

        # Accent separator
        c.setStrokeColor(accent)
        c.setLineWidth(2.5)
        c.line(x0 + 10, sp_top, x0 + col_w - 10, sp_top)

        # "STARTING PITCHER" label
        c.setFillColor(accent)
        c.setFont(FONT_REG_BOLD, 12)
        c.drawString(x0 + 14, sp_top - 18, "STARTING PITCHER")

        p_name = pitcher.get("name", "TBD")
        p_last = _get_last_name(p_name)
        p_first = p_name.split()[0] if " " in p_name else ""
        p_sd = stats_map.get(pitcher.get("id")) or {}
        p_pit = p_sd.get("pitching") or {}
        hand = p_sd.get("pitchHand", "")
        hand_lbl = "LHP" if hand == "L" else ("RHP" if hand == "R" else "")

        # SP row layout: left = hand badge + name, right = stats grid
        sp_row_mid = col_bot + (sp_top - 18 - col_bot) / 2

        # Hand badge (like position badge)
        sp_badge_x = x0 + 14
        sp_badge_w = 40
        sp_badge_h = 22
        sp_badge_y = sp_row_mid - 6
        if hand_lbl:
            c.saveState()
            c.setFillColor(Color(accent.red, accent.green, accent.blue, 0.18))
            c.roundRect(sp_badge_x, sp_badge_y, sp_badge_w, sp_badge_h, 4,
                        fill=1, stroke=0)
            c.restoreState()
            c.setFillColor(accent)
            c.setFont(FONT_BOLD, 14)
            c.drawCentredString(sp_badge_x + sp_badge_w / 2, sp_badge_y + 4, hand_lbl)

        # Pitcher name — big
        sp_name_left = sp_badge_x + sp_badge_w + 6
        sp_name_avail = name_zone_w - (sp_name_left - x0) - 4
        sp_fsz = 36
        while sp_fsz > 16:
            if c.stringWidth(p_last, FONT_BOLD, sp_fsz) <= sp_name_avail:
                break
            sp_fsz -= 1

        sp_badge_mid = sp_badge_y + sp_badge_h / 2
        sp_name_y = sp_badge_mid - sp_fsz * 0.35

        c.setFillColor(WHITE)
        c.setFont(FONT_BOLD, sp_fsz)
        c.drawString(sp_name_left, sp_name_y, p_last)

        # First name small below
        c.setFillColor(TEXT_MUTED)
        c.setFont(FONT_REG, 9)
        c.drawString(sp_name_left, sp_name_y - 14, p_first)

        # Stats grid on right side (matching batter layout)
        sp_sx0 = x0 + name_zone_w
        sp_sx_r = x0 + col_w - 8
        sp_s_w = sp_sx_r - sp_sx0

        if p_pit and p_pit.get("gamesPlayed", 0) > 0:
            era = str(p_pit.get("era", "--"))
            w_l = f"{p_pit.get('w', '-')}-{p_pit.get('l', '-')}"
            so = str(p_pit.get("so", "--"))
            whip = str(p_pit.get("whip", "--"))
            ip = str(p_pit.get("ip", "--"))

            sp_stats = [
                (era, "ERA", 20, WHITE),
                (w_l, "W-L", 18, TEXT_PRIMARY),
                (so, "K", 18, TEXT_SECONDARY),
                (whip, "WHIP", 16, TEXT_SECONDARY),
                (ip, "IP", 18, TEXT_SECONDARY),
            ]
            sp_n_cols = len(sp_stats)
            sp_cell_w = sp_s_w / sp_n_cols

            for j, (val, lbl, fsize, clr) in enumerate(sp_stats):
                cx_stat = sp_sx0 + sp_cell_w * j + sp_cell_w / 2
                c.setFillColor(clr)
                c.setFont(FONT_BOLD, fsize)
                c.drawCentredString(cx_stat, sp_badge_mid - fsize * 0.35 + 4, val)
                c.setFillColor(TEXT_MUTED)
                c.setFont(FONT_REG, 8)
                c.drawCentredString(cx_stat, sp_badge_mid - fsize * 0.35 - 10, lbl)

    # ============================================================
    # FOOTER
    # ============================================================
    c.setFillColor(TEXT_MUTED)
    c.setFont(FONT_REG, 9)
    c.drawString(M, 6, f"basenerd.com  |  {season} stats (or {prev_season})")
    c.setFillColor(ACCENT)
    c.setFont(FONT_REG_BOLD, 10)
    c.drawRightString(W - M, 6, "@basenerd")

    c.save()
    print(f"  Saved: {out_path}")
    return out_path


def _pdf_to_png(pdf_path: Path, dpi: int = 200) -> Path:
    """Convert a PDF to a high-res PNG using PyMuPDF."""
    import fitz
    doc = fitz.open(str(pdf_path))
    page = doc[0]
    zoom = dpi / 72
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    png_path = pdf_path.with_suffix(".png")
    pix.save(str(png_path))
    doc.close()
    print(f"  PNG:   {png_path}")
    return png_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate starting lineup graphic PDFs")
    parser.add_argument("--game_pk", type=int, help="Specific game PK")
    parser.add_argument("--date", type=str, help="YYYY-MM-DD, 'today', or 'yesterday'")
    parser.add_argument("--outdir", type=str, default=None)
    parser.add_argument("--format", type=str, default="pdf", choices=["pdf", "png"],
                        help="Output format: pdf (default) or png")
    args = parser.parse_args()

    out_dir = Path(args.outdir) if args.outdir else None
    want_png = args.format.lower() == "png"

    if args.date:
        if args.date.lower() == "yesterday":
            d = datetime.now(timezone.utc) - timedelta(days=1)
            date_str = d.strftime("%Y-%m-%d")
        elif args.date.lower() == "today":
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        else:
            date_str = args.date

        print(f"Fetching games for {date_str}...")
        games = _fetch_schedule(date_str)
        if not games:
            print("No games found for this date.")
            return

        count = 0
        for g in games:
            gd = _extract_game_data(g)
            if not gd:
                continue
            if not gd.get("away_lineup") and not gd.get("home_lineup"):
                print(f"  Skipping {gd['away']['abbrev']} @ {gd['home']['abbrev']} — no lineups announced")
                continue
            try:
                result = generate_lineup_graphic(gd, out_dir=out_dir)
                if result:
                    if want_png:
                        _pdf_to_png(result)
                    count += 1
            except Exception as e:
                print(f"  Error: {gd['away']['abbrev']} @ {gd['home']['abbrev']}: {e}")
                import traceback
                traceback.print_exc()
        print(f"\nDone. Generated {count} lineup graphics.")

    elif args.game_pk:
        found = False
        for offset in [0, -1, 1, -2, 2]:
            d = datetime.now(timezone.utc) + timedelta(days=offset)
            date_str = d.strftime("%Y-%m-%d")
            games = _fetch_schedule(date_str, sport_ids=[1, 51])
            for g in games:
                if g.get("gamePk") == args.game_pk:
                    gd = _extract_game_data(g)
                    if gd:
                        result = generate_lineup_graphic(gd, out_dir=out_dir)
                        if result and want_png:
                            _pdf_to_png(result)
                        found = True
                    break
            if found:
                break
        if not found:
            print(f"Game {args.game_pk} not found in recent schedule.")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
