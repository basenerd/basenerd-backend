#!/usr/bin/env python3
"""
Generate a post-game pitcher report PDF (Twitter-optimized 1200x1500).

Usage:
    python scripts/generate_pitcher_report_pdf.py --pitcher_id 669373 --game_pk 747131
    python scripts/generate_pitcher_report_pdf.py --date 2026-03-05          # all starters
    python scripts/generate_pitcher_report_pdf.py --date yesterday           # yesterday's games

Output: reports/<date>/<PlayerName>_<game_pk>.pdf
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
from PIL import Image as PILImage

from reportlab.lib.colors import Color, HexColor, white, black
from reportlab.lib.pagesizes import landscape
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas as rl_canvas
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

os.environ.setdefault("DATABASE_URL", os.environ.get("DATABASE_URL_PG", ""))

from services.pitching_report import (
    pitching_report_summary,
    pitching_scatter,
)

LOGO_PATH = ROOT / "static" / "basenerd-logo-official.png"
REPORTS_DIR = ROOT / "reports"

# ---------------------------------------------------------------------------
# Twitter card: 1200x1500 px  (4:5 portrait, good for Twitter/X + IG)
# We'll use points. 1 pt = 1 px at 72 dpi.
# ---------------------------------------------------------------------------
W = 1200
H = 1500

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

PITCH_COLORS = {
    "FF": "#d9534f", "FT": "#f0ad4e", "SI": "#f0ad4e", "FC": "#5bc0de",
    "SL": "#ffd54f", "ST": "#ffd54f", "SV": "#9b59b6", "CU": "#5dade2",
    "KC": "#5dade2", "CH": "#5cb85c", "FS": "#5cb85c", "KN": "#95a5a6",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
MLB_API = "https://statsapi.mlb.com/api/v1"


def _fmt(v, decimals=1, fallback="--"):
    if v is None or (isinstance(v, float) and not math.isfinite(v)):
        return fallback
    if decimals == 0:
        return str(int(round(v)))
    return f"{v:.{decimals}f}"


def _pct(v, fallback="--"):
    if v is None:
        return fallback
    return f"{v:.1f}%"


def _fetch_player(pid: int) -> dict:
    r = requests.get(f"{MLB_API}/people/{pid}", params={"hydrate": "currentTeam"}, timeout=15)
    r.raise_for_status()
    return (r.json().get("people") or [{}])[0]


def _fetch_headshot(pid: int, size: int = 360) -> Optional[PILImage.Image]:
    url = (
        f"https://img.mlbstatic.com/mlb-photos/image/upload/"
        f"w_{size},q_100/v1/people/{pid}/headshot/67/current"
    )
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200 and len(r.content) > 1000:
            return PILImage.open(io.BytesIO(r.content)).convert("RGBA")
    except Exception:
        pass
    return None


def _fetch_game_info(game_pk: int) -> dict:
    """Get basic game info (teams, score, date) from MLB API."""
    url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return {}
        data = r.json()
        gd = (data.get("gameData") or {})
        dt = gd.get("datetime") or {}
        teams = gd.get("teams") or {}
        ls = ((data.get("liveData") or {}).get("linescore") or {}).get("teams") or {}

        away = teams.get("away") or {}
        home = teams.get("home") or {}
        return {
            "date": (dt.get("officialDate") or "")[:10],
            "away_name": away.get("teamName") or away.get("name", ""),
            "home_name": home.get("teamName") or home.get("name", ""),
            "away_abbrev": away.get("abbreviation", ""),
            "home_abbrev": home.get("abbreviation", ""),
            "away_runs": (ls.get("away") or {}).get("runs"),
            "home_runs": (ls.get("home") or {}).get("runs"),
        }
    except Exception:
        return {}


def _games_for_date(date_str: str) -> List[dict]:
    """Return list of {game_pk, away_pitcher_id, home_pitcher_id} for a date."""
    url = f"{MLB_API}/schedule"
    params = {
        "date": date_str,
        "sportId": 1,
        "hydrate": "probablePitcher,linescore",
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        dates = r.json().get("dates") or []
        out = []
        for d in dates:
            for g in d.get("games") or []:
                gp = g.get("gamePk")
                if not gp:
                    continue
                state = ((g.get("status") or {}).get("abstractGameState") or "").lower()
                if state != "final":
                    continue
                teams = g.get("teams") or {}
                away_p = ((teams.get("away") or {}).get("probablePitcher") or {}).get("id")
                home_p = ((teams.get("home") or {}).get("probablePitcher") or {}).get("id")
                out.append({
                    "game_pk": int(gp),
                    "away_pitcher_id": int(away_p) if away_p else None,
                    "home_pitcher_id": int(home_p) if home_p else None,
                })
        return out
    except Exception as e:
        print(f"Error fetching schedule for {date_str}: {e}")
        return []


# ---------------------------------------------------------------------------
# Drawing helpers
# ---------------------------------------------------------------------------
def _rounded_rect(c, x, y, w, h, r, fill_color=None, stroke_color=None):
    """Draw a rounded rectangle."""
    p = c.beginPath()
    p.moveTo(x + r, y)
    p.lineTo(x + w - r, y)
    p.arcTo(x + w - r, y, x + w, y + r, r)
    p.lineTo(x + w, y + h - r)
    p.arcTo(x + w, y + h - r, x + w - r, y + h, r)
    p.lineTo(x + r, y + h)
    p.arcTo(x + r, y + h, x, y + h - r, r)
    p.lineTo(x, y + r)
    p.arcTo(x, y + r, x + r, y, r)
    p.closePath()
    if fill_color:
        c.setFillColor(fill_color)
    if stroke_color:
        c.setStrokeColor(stroke_color)
        c.setLineWidth(1)
        c.drawPath(p, fill=1 if fill_color else 0, stroke=1 if stroke_color else 0)
    elif fill_color:
        c.drawPath(p, fill=1, stroke=0)


def _draw_pitch_pill(c, x, y, pt_code, size=22):
    """Draw a colored pitch-type pill."""
    color = HexColor(PITCH_COLORS.get(pt_code, "#94a3b8"))
    _rounded_rect(c, x, y, size + 12, size, size // 2, fill_color=color)
    c.setFillColor(HexColor("#0b1220"))
    c.setFont("Helvetica-Bold", 11)
    c.drawCentredString(x + (size + 12) / 2, y + 6, pt_code)


def _draw_movement_chart(c, x, y, size, shapes, scatter_data=None):
    """Draw pitch movement chart with axes and dots."""
    cx_chart = x + size / 2
    cy_chart = y + size / 2
    R = size / 2 - 30

    # Background
    _rounded_rect(c, x, y, size, size, 10, fill_color=BG_CARD)

    # Grid lines
    c.setStrokeColor(HexColor("#1e3a5f"))
    c.setLineWidth(0.5)
    c.line(cx_chart - R, cy_chart, cx_chart + R, cy_chart)
    c.line(cx_chart, cy_chart - R, cx_chart, cy_chart + R)

    # Rings at 6", 12", 18"
    max_v = 24.0
    c.setDash(2, 3)
    for ring_val in [6, 12, 18]:
        rr = (ring_val / max_v) * R
        c.circle(cx_chart, cy_chart, rr, fill=0, stroke=1)
    c.setDash()

    # Ring labels
    c.setFillColor(TEXT_MUTED)
    c.setFont("Helvetica", 9)
    for ring_val in [6, 12, 18]:
        rr = (ring_val / max_v) * R
        c.drawString(cx_chart + 4, cy_chart + rr + 2, f'{ring_val}"')

    # Axis labels
    c.setFillColor(TEXT_SECONDARY)
    c.setFont("Helvetica", 10)
    c.drawCentredString(cx_chart, y + size - 8, "HB (in)")
    c.saveState()
    c.translate(x + 10, cy_chart)
    c.rotate(90)
    c.drawCentredString(0, 0, "IVB (in)")
    c.restoreState()

    # Draw individual pitch dots if we have scatter data
    if scatter_data:
        c.saveState()
        for pt_data in scatter_data:
            pt = pt_data.get("pitch_type", "")
            hb = pt_data.get("hb")
            ivb = pt_data.get("ivb")
            if hb is None or ivb is None:
                continue
            px = cx_chart + (hb / max_v) * R
            py = cy_chart + (ivb / max_v) * R
            color = HexColor(PITCH_COLORS.get(pt, "#94a3b8"))
            c.setFillColor(color)
            c.setStrokeColor(Color(0, 0, 0, alpha=0.3))
            c.setLineWidth(0.5)
            c.circle(px, py, 3, fill=1, stroke=1)
        c.restoreState()

    # Draw average dots (larger, labeled)
    if shapes:
        ordered = sorted(shapes, key=lambda d: d.get("n", 0), reverse=True)
        n_max = max(1, max(d.get("n", 1) for d in ordered))

        for d in ordered:
            pt = d.get("pitch_type", "UNK")
            pfx_x = d.get("pfx_x")
            pfx_z = d.get("pfx_z")
            if pfx_x is None or pfx_z is None:
                continue

            hb = pfx_x * 12.0
            ivb = pfx_z * 12.0
            hb = max(-max_v, min(max_v, hb))
            ivb = max(-max_v, min(max_v, ivb))

            px = cx_chart + (hb / max_v) * R
            py = cy_chart + (ivb / max_v) * R

            t = (d.get("n", 0)) / n_max
            rad = 8 + 12 * t

            color = HexColor(PITCH_COLORS.get(pt, "#94a3b8"))
            c.setFillColor(color)
            c.setStrokeColor(white)
            c.setLineWidth(2)
            c.circle(px, py, rad, fill=1, stroke=1)

            # Label
            c.setFillColor(HexColor("#0b1220"))
            c.setFont("Helvetica-Bold", 10)
            c.drawCentredString(px, py - 3.5, pt)


def _draw_usage_bars(c, x, y, w, h, usage_lr, side_totals):
    """Draw horizontal stacked bars for vs LHH / vs RHH usage."""
    _rounded_rect(c, x, y, w, h, 10, fill_color=BG_CARD)

    pad = 14
    inner_x = x + pad
    inner_w = w - pad * 2
    bar_h = 22

    c.setFont("Helvetica-Bold", 13)
    c.setFillColor(TEXT_PRIMARY)
    c.drawString(inner_x, y + h - 28, "Usage Split")

    c.setFont("Helvetica", 11)
    c.setFillColor(TEXT_SECONDARY)

    for idx, label in enumerate(["vs LHH", "vs RHH"]):
        by = y + h - 60 - idx * 48
        c.setFillColor(TEXT_SECONDARY)
        c.drawString(inner_x, by + bar_h + 6, f"{label}")

        # Draw stacked bar
        bx = inner_x
        key = "l_usage" if idx == 0 else "r_usage"
        for row in usage_lr:
            pct = row.get(key, 0)
            if pct <= 0:
                continue
            seg_w = (pct / 100.0) * inner_w
            color = HexColor(PITCH_COLORS.get(row.get("pitch_type", ""), "#94a3b8"))
            _rounded_rect(c, bx, by, max(seg_w, 2), bar_h, 3, fill_color=color)
            if seg_w > 30:
                c.setFillColor(HexColor("#0b1220"))
                c.setFont("Helvetica-Bold", 9)
                c.drawCentredString(bx + seg_w / 2, by + 7, f"{row.get('pitch_type', '')}")
            bx += seg_w


def _color_for_metric(val, center=100.0, good_high=True):
    """Color scale: green=good, red=bad, white=neutral."""
    if val is None:
        return TEXT_MUTED
    diff = val - center
    if not good_high:
        diff = -diff
    if abs(diff) < 3:
        return TEXT_PRIMARY
    if diff > 0:
        intensity = min(1.0, abs(diff) / 30.0)
        return Color(
            GREEN_GOOD.red * intensity + TEXT_PRIMARY.red * (1 - intensity),
            GREEN_GOOD.green * intensity + TEXT_PRIMARY.green * (1 - intensity),
            GREEN_GOOD.blue * intensity + TEXT_PRIMARY.blue * (1 - intensity),
        )
    else:
        intensity = min(1.0, abs(diff) / 30.0)
        return Color(
            RED_BAD.red * intensity + TEXT_PRIMARY.red * (1 - intensity),
            RED_BAD.green * intensity + TEXT_PRIMARY.green * (1 - intensity),
            RED_BAD.blue * intensity + TEXT_PRIMARY.blue * (1 - intensity),
        )


# ---------------------------------------------------------------------------
# Main PDF generation
# ---------------------------------------------------------------------------
def generate_report(
    pitcher_id: int,
    game_pk: int,
    season: Optional[int] = None,
    out_dir: Optional[Path] = None,
) -> Optional[Path]:
    """Generate a single pitcher report PDF. Returns the output path."""
    if season is None:
        season = datetime.now(timezone.utc).year

    print(f"  Fetching report for pitcher {pitcher_id}, game {game_pk}, season {season}...")

    # Fetch data
    report = pitching_report_summary(pitcher_id, season, game_pk=game_pk)
    if not report or not report.get("ok"):
        print(f"  No pitch data for pitcher {pitcher_id} in game {game_pk}")
        return None

    scatter = pitching_scatter(pitcher_id, season, game_pk=game_pk)
    player = _fetch_player(pitcher_id)
    headshot = _fetch_headshot(pitcher_id, size=360)
    game_info = _fetch_game_info(game_pk)

    player_name = player.get("fullName", f"Player {pitcher_id}")
    team_name = ((player.get("currentTeam") or {}).get("name") or "")
    game_date = game_info.get("date") or report.get("game_date") or str(season)

    # Build output path
    if out_dir is None:
        out_dir = REPORTS_DIR / game_date.replace("-", "")
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_name = player_name.replace(" ", "_").replace(".", "")
    out_path = out_dir / f"{safe_name}_{game_pk}.pdf"

    mix = report.get("mix") or []
    basic = report.get("basic") or {}
    shapes = report.get("shapes") or []
    usage_lr = report.get("usage_lr") or []
    side_totals = report.get("side_totals") or {}
    total_pitches = report.get("total") or 0

    # --- Create PDF ---
    c = rl_canvas.Canvas(str(out_path), pagesize=(W, H))

    # Background
    c.setFillColor(BG)
    c.rect(0, 0, W, H, fill=1, stroke=0)

    # ===== HEADER BAR =====
    header_h = 130
    header_y = H - header_h
    _rounded_rect(c, 20, header_y, W - 40, header_h - 10, 14, fill_color=BG_CARD)

    # Logo (left side)
    if LOGO_PATH.exists():
        try:
            logo_img = PILImage.open(str(LOGO_PATH)).convert("RGBA")
            logo_size = 80
            logo_reader = ImageReader(logo_img)
            c.drawImage(logo_reader, 36, header_y + 18, width=logo_size, height=logo_size, mask="auto")
        except Exception as e:
            print(f"  Warning: Could not load logo: {e}")

    # Headshot (right side)
    if headshot:
        try:
            hs_size = 90
            hs_reader = ImageReader(headshot)
            c.drawImage(hs_reader, W - 60 - hs_size, header_y + 12, width=hs_size, height=hs_size, mask="auto")
        except Exception:
            pass

    # Player name + details
    name_x = 130
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 32)
    c.drawString(name_x, header_y + 78, player_name)

    # Game context line
    away_abbrev = game_info.get("away_abbrev", "")
    home_abbrev = game_info.get("home_abbrev", "")
    away_runs = game_info.get("away_runs")
    home_runs = game_info.get("home_runs")
    score_str = ""
    if away_runs is not None and home_runs is not None:
        score_str = f"  |  {away_abbrev} {away_runs} - {home_abbrev} {home_runs}"

    c.setFillColor(TEXT_SECONDARY)
    c.setFont("Helvetica", 16)
    context_line = f"{team_name}  |  {game_date}{score_str}  |  {total_pitches} pitches"
    c.drawString(name_x, header_y + 48, context_line)

    # "Post-Game Pitcher Report" tag
    c.setFillColor(ACCENT)
    c.setFont("Helvetica-Bold", 13)
    c.drawString(name_x, header_y + 22, "POST-GAME PITCHER REPORT")

    # ===== TOP-LINE STATS BAR =====
    stat_bar_y = header_y - 70
    stat_bar_h = 56
    _rounded_rect(c, 20, stat_bar_y, W - 40, stat_bar_h, 10, fill_color=BG_CARD)

    stat_items = [
        ("K%", _pct(basic.get("k_pct"))),
        ("BB%", _pct(basic.get("bb_pct"))),
        ("Whiff%", _pct(basic.get("whiff_pct"))),
        ("Zone%", _pct(basic.get("zone_pct"))),
        ("Chase%", _pct(basic.get("chase_pct"))),
        ("xwOBA", _fmt(basic.get("xwoba"), 3)),
    ]

    stat_w = (W - 80) / len(stat_items)
    for i, (label, val) in enumerate(stat_items):
        sx = 40 + i * stat_w
        c.setFillColor(TEXT_MUTED)
        c.setFont("Helvetica", 11)
        c.drawCentredString(sx + stat_w / 2, stat_bar_y + stat_bar_h - 16, label)
        c.setFillColor(WHITE)
        c.setFont("Helvetica-Bold", 20)
        c.drawCentredString(sx + stat_w / 2, stat_bar_y + 10, val)

    # ===== ARSENAL TABLE =====
    table_y = stat_bar_y - 30
    cols = ["Pitch", "#", "Use%", "Velo", "HB", "IVB", "Spin", "xwOBA", "Whiff%", "Zone%", "Chase%", "BNStuff+", "BNCtrl+"]
    col_widths = [70, 40, 55, 55, 50, 50, 55, 65, 60, 55, 60, 75, 70]
    total_table_w = sum(col_widths)
    table_x = (W - total_table_w) / 2
    row_h = 28

    # Table header
    table_header_y = table_y - row_h
    _rounded_rect(c, table_x - 8, table_header_y - 2, total_table_w + 16, row_h + 4, 6, fill_color=BORDER)

    c.setFont("Helvetica-Bold", 11)
    cx_pos = table_x
    for ci, col_name in enumerate(cols):
        cw = col_widths[ci]
        c.setFillColor(ACCENT if col_name.startswith("BN") else TEXT_SECONDARY)
        if ci == 0:
            c.drawString(cx_pos + 4, table_header_y + 8, col_name)
        else:
            c.drawCentredString(cx_pos + cw / 2, table_header_y + 8, col_name)
        cx_pos += cw

    # Table rows
    for ri, row in enumerate(mix):
        ry = table_header_y - (ri + 1) * row_h
        if ri % 2 == 0:
            _rounded_rect(c, table_x - 8, ry - 2, total_table_w + 16, row_h, 4, fill_color=BG_TABLE_ROW)

        pt = row.get("pitch_type", "UNK")
        values = [
            pt,
            _fmt(row.get("n"), 0),
            _fmt(row.get("usage"), 1),
            _fmt(row.get("velo"), 1),
            _fmt(row.get("hb"), 1),
            _fmt(row.get("ivb"), 1),
            _fmt(row.get("spin"), 0),
            _fmt(row.get("xwoba"), 3),
            _pct(row.get("whiff")),
            _pct(row.get("zone_pct")),
            _pct(row.get("chase_pct")),
            _fmt(row.get("stuff_plus"), 0),
            _fmt(row.get("control_plus"), 0),
        ]

        cx_pos = table_x
        for ci, val in enumerate(values):
            cw = col_widths[ci]
            if ci == 0:
                # Pitch type pill
                color = HexColor(PITCH_COLORS.get(pt, "#94a3b8"))
                _rounded_rect(c, cx_pos + 2, ry + 2, 56, 20, 10, fill_color=color)
                c.setFillColor(HexColor("#0b1220"))
                c.setFont("Helvetica-Bold", 11)
                c.drawCentredString(cx_pos + 30, ry + 6, pt)
            elif ci in (11, 12):
                # BNStuff+ / BNCtrl+ with color coding
                raw = row.get("stuff_plus") if ci == 11 else row.get("control_plus")
                c.setFillColor(_color_for_metric(raw, center=100, good_high=True))
                c.setFont("Helvetica-Bold", 12)
                c.drawCentredString(cx_pos + cw / 2, ry + 7, val)
            else:
                c.setFillColor(TEXT_PRIMARY)
                c.setFont("Helvetica", 12)
                c.drawCentredString(cx_pos + cw / 2, ry + 7, val)
            cx_pos += cw

    # ===== BOTTOM SECTION: Movement chart (left) + Usage bars (right) =====
    bottom_y = table_header_y - (len(mix) + 1) * row_h - 30
    chart_size = min(460, bottom_y - 80)
    chart_y = bottom_y - chart_size

    if chart_size > 200:
        # Movement chart
        _draw_movement_chart(c, 30, chart_y, chart_size, shapes, scatter_data=scatter)

        # Title above chart
        c.setFillColor(TEXT_PRIMARY)
        c.setFont("Helvetica-Bold", 14)
        c.drawString(30, chart_y + chart_size + 8, "Pitch Movement")

        # Usage split bars (right of chart)
        usage_x = 30 + chart_size + 30
        usage_w = W - usage_x - 30
        usage_h = 170
        if usage_lr:
            _draw_usage_bars(c, usage_x, chart_y + chart_size - usage_h, usage_w, usage_h, usage_lr, side_totals)

        # ===== PITCH NOTES (right side, below usage) =====
        notes_y = chart_y + chart_size - usage_h - 30
        notes_h = chart_size - usage_h - 10
        if notes_h > 80:
            _rounded_rect(c, usage_x, chart_y, usage_w, max(notes_h, 100), 10, fill_color=BG_CARD)
            c.setFillColor(TEXT_PRIMARY)
            c.setFont("Helvetica-Bold", 13)
            c.drawString(usage_x + 14, chart_y + notes_h - 20, "Pitch Insights")

            ny = chart_y + notes_h - 44
            c.setFont("Helvetica", 11)
            c.setFillColor(TEXT_SECONDARY)

            # Generate some insights from the data
            insights = _generate_insights(mix, basic, total_pitches)
            for insight in insights[:6]:
                if ny < chart_y + 10:
                    break
                # Wrap long lines
                lines = _wrap_text(insight, 55)
                for line in lines:
                    if ny < chart_y + 10:
                        break
                    c.drawString(usage_x + 14, ny, line)
                    ny -= 16
                ny -= 6

    # ===== FOOTER =====
    c.setFillColor(TEXT_MUTED)
    c.setFont("Helvetica", 10)
    c.drawString(30, 18, f"basenerd.com  |  Data from Baseball Savant  |  Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

    c.setFillColor(ACCENT)
    c.setFont("Helvetica-Bold", 11)
    c.drawRightString(W - 30, 18, "@basenerd")

    c.save()
    print(f"  Saved: {out_path}")
    return out_path


def _generate_insights(mix, basic, total_pitches) -> List[str]:
    """Auto-generate short text insights from the data."""
    insights = []

    # Best stuff+ pitch
    sp_pitches = [(r.get("pitch_name", r.get("pitch_type", "?")), r.get("stuff_plus")) for r in mix if r.get("stuff_plus") is not None]
    if sp_pitches:
        best = max(sp_pitches, key=lambda x: x[1])
        if best[1] >= 110:
            insights.append(f"Best BNStuff+: {best[0]} at {best[1]:.0f}")

    # Highest whiff pitch
    whiff_pitches = [(r.get("pitch_name", r.get("pitch_type", "?")), r.get("whiff"), r.get("n", 0)) for r in mix if r.get("whiff") is not None and (r.get("n") or 0) >= 5]
    if whiff_pitches:
        best_w = max(whiff_pitches, key=lambda x: x[1])
        if best_w[1] >= 30:
            insights.append(f"Top whiff: {best_w[0]} at {best_w[1]:.1f}%")

    # Chase rate
    chase = basic.get("chase_pct")
    if chase is not None:
        if chase >= 35:
            insights.append(f"Elite chase rate: {chase:.1f}%")
        elif chase <= 20:
            insights.append(f"Low chase rate: {chase:.1f}%")

    # K% / BB%
    k_pct = basic.get("k_pct")
    bb_pct = basic.get("bb_pct")
    if k_pct is not None and bb_pct is not None and bb_pct > 0:
        k_bb = k_pct / bb_pct
        insights.append(f"K/BB ratio: {k_bb:.1f}")

    # xwOBA
    xw = basic.get("xwoba")
    if xw is not None:
        if xw <= 0.280:
            insights.append(f"Dominant xwOBA: {xw:.3f}")
        elif xw >= 0.370:
            insights.append(f"Rough outing: {xw:.3f} xwOBA")

    # Pitch count
    insights.append(f"Total pitches: {total_pitches}")

    return insights


def _wrap_text(text, max_chars):
    """Simple word-wrap."""
    words = text.split()
    lines = []
    current = ""
    for w in words:
        if len(current) + len(w) + 1 > max_chars:
            lines.append(current)
            current = w
        else:
            current = f"{current} {w}" if current else w
    if current:
        lines.append(current)
    return lines


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate post-game pitcher report PDFs")
    parser.add_argument("--pitcher_id", type=int, help="Single pitcher MLBAM ID")
    parser.add_argument("--game_pk", type=int, help="Single game PK")
    parser.add_argument("--season", type=int, default=None)
    parser.add_argument("--date", type=str, help="Date (YYYY-MM-DD or 'yesterday') to generate all starters")
    parser.add_argument("--outdir", type=str, default=None, help="Custom output directory")
    args = parser.parse_args()

    out_dir = Path(args.outdir) if args.outdir else None

    if args.date:
        if args.date.lower() == "yesterday":
            d = datetime.now(timezone.utc) - timedelta(days=1)
            date_str = d.strftime("%Y-%m-%d")
        elif args.date.lower() == "today":
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        else:
            date_str = args.date

        print(f"Generating reports for {date_str}...")
        games = _games_for_date(date_str)
        if not games:
            print("No final games found for this date.")
            return

        season = args.season or int(date_str[:4])
        count = 0
        for g in games:
            gp = g["game_pk"]
            for pid in [g.get("away_pitcher_id"), g.get("home_pitcher_id")]:
                if pid:
                    try:
                        result = generate_report(pid, gp, season=season, out_dir=out_dir)
                        if result:
                            count += 1
                    except Exception as e:
                        print(f"  Error generating report for pitcher {pid}, game {gp}: {e}")

        print(f"\nDone. Generated {count} reports.")

    elif args.pitcher_id and args.game_pk:
        season = args.season or datetime.now(timezone.utc).year
        generate_report(args.pitcher_id, args.game_pk, season=season, out_dir=out_dir)

    else:
        parser.print_help()
        print("\nExamples:")
        print("  python scripts/generate_pitcher_report_pdf.py --pitcher_id 669373 --game_pk 747131")
        print("  python scripts/generate_pitcher_report_pdf.py --date yesterday")
        print("  python scripts/generate_pitcher_report_pdf.py --date 2026-03-05")


if __name__ == "__main__":
    main()
