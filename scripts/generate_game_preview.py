#!/usr/bin/env python3
"""
Generate a game preview PNG (1080x1350 portrait, Twitter/X optimized).

Combines:
  - Starting lineups with team logos, colors
  - Matchup predictions (grades, hit%, HR%, total bases, H+R+RBI)
  - Monte Carlo win probability & expected score
  - Stadium SVG with directional HR zone factors
  - Weather conditions

Replaces generate_lineup_graphic.py as the lineup-announced report.

Usage:
    python scripts/generate_game_preview.py --game_pk 788096
    python scripts/generate_game_preview.py --date 2026-04-20
    python scripts/generate_game_preview.py --date today

Output: reports/previews/<YYYYMMDD>/<Away_vs_Home>_<game_pk>.png
"""
from __future__ import annotations

import argparse
import io
import math
import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import numpy as np
from PIL import Image, ImageDraw, ImageFont

# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

LOGO_PATH = ROOT / "static" / "basenerd-logo-official.png"
TEAM_LOGOS_DIR = ROOT / "static" / "team_logos"
STADIUM_SVGS_DIR = ROOT / "static" / "stadium_svgs"
REPORTS_DIR = ROOT / "reports" / "previews"

MLB_API = "https://statsapi.mlb.com/api/v1"

# ---------------------------------------------------------------------------
# Page dimensions
# ---------------------------------------------------------------------------
W = 1080
H = 1350
PAD = 20
CARD_R = 10
GAP = 6

# ---------------------------------------------------------------------------
# Colors (RGB tuples for PIL)
# ---------------------------------------------------------------------------
BG = (11, 18, 32)
BG_CARD = (19, 28, 46)
BG_TABLE_ROW = (24, 35, 56)
BG_TABLE_HDR = (30, 45, 70)
ACCENT = (56, 189, 248)
TEXT_PRIMARY = (241, 245, 249)
TEXT_SECONDARY = (148, 163, 184)
TEXT_MUTED = (100, 116, 139)
BORDER = (30, 58, 95)
WHITE = (255, 255, 255)
GREEN_GOOD = (34, 197, 94)
RED_BAD = (239, 68, 68)
YELLOW_WARN = (250, 204, 21)

GRADE_COLORS = {
    "A+": GREEN_GOOD, "A": GREEN_GOOD, "A-": (74, 197, 114),
    "B+": (120, 200, 130), "B": (148, 200, 140), "B-": (170, 200, 160),
    "C+": (200, 200, 170), "C": TEXT_SECONDARY, "C-": (190, 170, 150),
    "D+": (220, 150, 120), "D": (230, 120, 100), "D-": (239, 100, 80),
    "F": RED_BAD,
}

TEAM_COLORS = {
    "AZ": "#a71930", "ATL": "#ce1141", "BAL": "#df4601", "BOS": "#bd3039",
    "CHC": "#0e3386", "CIN": "#c6011f", "CLE": "#00385d", "COL": "#333366",
    "CWS": "#27251f", "DET": "#0c2340", "HOU": "#002d62", "KC": "#004687",
    "LAA": "#ba0021", "LAD": "#005a9c", "MIA": "#00a3e0", "MIL": "#ffc52f",
    "MIN": "#002b5c", "NYM": "#002d72", "NYY": "#003087", "OAK": "#003831",
    "PHI": "#e81828", "PIT": "#fdb827", "SD": "#2f241d", "SF": "#fd5a1e",
    "SEA": "#0c2c56", "STL": "#c41e3a", "TB": "#092c5c", "TEX": "#003278",
    "TOR": "#134a8e", "WSH": "#ab0003",
}

TEAM_ACCENT_COLORS = {
    "AZ": "#e8264a", "ATL": "#f23d5f", "BAL": "#ff6b1a", "BOS": "#e8424a",
    "CHC": "#2563eb", "CIN": "#ef4444", "CLE": "#3b82f6", "COL": "#6366f1",
    "CWS": "#a8a29e", "DET": "#3b82f6", "HOU": "#f97316", "KC": "#60a5fa",
    "LAA": "#ef4444", "LAD": "#3b82f6", "MIA": "#22d3ee", "MIL": "#fbbf24",
    "MIN": "#3b82f6", "NYM": "#3b82f6", "NYY": "#3b82f6", "OAK": "#22c55e",
    "PHI": "#ef4444", "PIT": "#fbbf24", "SD": "#a16207", "SF": "#fb923c",
    "SEA": "#22d3ee", "STL": "#ef4444", "TB": "#60a5fa", "TEX": "#60a5fa",
    "TOR": "#60a5fa", "WSH": "#ef4444",
}

ABBREV_MAP = {"ATH": "OAK"}

# Team abbreviation → stadium SVG filename
TEAM_SVG_MAP = {
    "NYY": "yankees", "BOS": "red_sox", "NYM": "mets", "BAL": "orioles",
    "TB": "rays", "TOR": "blue_jays", "CWS": "white_sox", "CLE": "guardians",
    "DET": "tigers", "KC": "royals", "MIN": "twins", "HOU": "astros",
    "LAA": "angels", "OAK": "athletics", "SEA": "mariners", "TEX": "rangers",
    "ATL": "braves", "MIA": "marlins", "PHI": "phillies", "WSH": "nationals",
    "CHC": "cubs", "CIN": "reds", "MIL": "brewers", "PIT": "pirates",
    "STL": "cardinals", "AZ": "diamondbacks", "COL": "rockies", "LAD": "dodgers",
    "SF": "giants", "SD": "padres",
}


# ---------------------------------------------------------------------------
# Font helpers
# ---------------------------------------------------------------------------
def _get_font(size, bold=False):
    if bold:
        names = ["impact.ttf", "Impact.ttf", "arialbd.ttf", "Arial Bold.ttf"]
    else:
        names = ["arial.ttf", "Arial.ttf"]
    for name in names:
        for prefix in ["C:/Windows/Fonts/", "/usr/share/fonts/truetype/", ""]:
            try:
                return ImageFont.truetype(prefix + name, size)
            except (OSError, IOError):
                continue
    return ImageFont.load_default()


def _get_arial(size, bold=False):
    names = ["arialbd.ttf", "Arial Bold.ttf"] if bold else ["arial.ttf", "Arial.ttf"]
    for name in names:
        for prefix in ["C:/Windows/Fonts/", "/usr/share/fonts/truetype/", ""]:
            try:
                return ImageFont.truetype(prefix + name, size)
            except (OSError, IOError):
                continue
    return ImageFont.load_default()


def _hex_to_rgb(h: str) -> Tuple[int, int, int]:
    h = h.lstrip("#")
    return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))


def _team_color(abbrev: str) -> Tuple[int, int, int]:
    abbrev = ABBREV_MAP.get(abbrev, abbrev)
    return _hex_to_rgb(TEAM_COLORS.get(abbrev, "#1e3a5f"))


def _team_accent(abbrev: str) -> Tuple[int, int, int]:
    abbrev = ABBREV_MAP.get(abbrev, abbrev)
    return _hex_to_rgb(TEAM_ACCENT_COLORS.get(abbrev, "#38bdf8"))


# ---------------------------------------------------------------------------
# Image helpers
# ---------------------------------------------------------------------------
def _rounded_rect(draw, xy, r, fill=None, outline=None):
    draw.rounded_rectangle(xy, radius=r, fill=fill, outline=outline)


def _load_team_logo(abbrev: str) -> Optional[Image.Image]:
    abbrev = ABBREV_MAP.get(abbrev, abbrev)
    png_path = TEAM_LOGOS_DIR / f"{abbrev}.png"
    if png_path.exists():
        try:
            return Image.open(str(png_path)).convert("RGBA")
        except Exception:
            pass
    return None


def _paste_logo(img: Image.Image, abbrev: str, x: int, y: int, size: int):
    logo = _load_team_logo(abbrev)
    if logo:
        logo = logo.resize((size, size), Image.Resampling.LANCZOS)
        img.paste(logo, (x, y), logo)


def _get_last_name(full_name: str) -> str:
    parts = full_name.strip().split()
    if len(parts) <= 1:
        return full_name.upper()
    suffixes = {"jr.", "jr", "sr.", "sr", "ii", "iii", "iv", "v"}
    if len(parts) >= 3 and parts[-1].lower().rstrip(".") in suffixes:
        return f"{parts[-2]} {parts[-1]}".upper()
    return parts[-1].upper()


def _text_width(draw, text, font):
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0]


def _draw_centered(draw, text, cx, y, font, fill):
    w = _text_width(draw, text, font)
    draw.text((cx - w // 2, y), text, fill=fill, font=font)


def _draw_right(draw, text, rx, y, font, fill):
    w = _text_width(draw, text, font)
    draw.text((rx - w, y), text, fill=fill, font=font)


# ---------------------------------------------------------------------------
# Stadium SVG rendering
# ---------------------------------------------------------------------------
def _render_stadium_svg(home_abbrev: str, size: int = 220) -> Optional[Image.Image]:
    """Parse stadium SVG polylines and render as a PIL image.
    Flipped so outfield is at top and home plate at bottom (bird's eye view)."""
    slug = TEAM_SVG_MAP.get(ABBREV_MAP.get(home_abbrev, home_abbrev), "generic")
    svg_path = STADIUM_SVGS_DIR / f"{slug}.svg"
    if not svg_path.exists():
        svg_path = STADIUM_SVGS_DIR / "generic.svg"
    if not svg_path.exists():
        return None

    try:
        tree = ET.parse(str(svg_path))
        root = tree.getroot()

        # Parse viewBox
        vb = root.get("viewBox", "0 0 576 576")
        vb_parts = vb.replace(",", " ").split()
        vb_w = float(vb_parts[2])
        vb_h = float(vb_parts[3])

        # Parse all polylines
        polylines = root.findall(".//{http://www.w3.org/2000/svg}polyline")
        if not polylines:
            polylines = root.findall(".//polyline")
        if not polylines:
            return None

        img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        scale = size / max(vb_w, vb_h)

        for poly in polylines:
            pts_str = poly.get("points", "")
            if not pts_str.strip():
                continue
            coords = []
            for token in pts_str.strip().split():
                parts = token.split(",")
                if len(parts) == 2:
                    try:
                        x = float(parts[0]) * scale
                        # Flip Y: scaleY(-1) — outfield at top, home plate at bottom
                        y_flipped = size - float(parts[1]) * scale
                        coords.append((x, y_flipped))
                    except ValueError:
                        continue
            if len(coords) >= 2:
                draw.line(coords, fill=(180, 200, 220, 90), width=2)

        return img
    except Exception:
        return None


def _zone_chip_color(factor: float) -> Tuple[int, int, int]:
    """Color for HR zone factor chip."""
    if factor >= 1.07:
        return (14, 100, 42)   # strong positive green
    elif factor >= 1.03:
        return (14, 85, 36)    # positive green
    elif factor <= 0.93:
        return (130, 18, 18)   # strong negative red
    elif factor <= 0.97:
        return (110, 22, 22)   # negative red
    return (30, 30, 30)        # neutral


def _fmt_factor(f: float) -> str:
    pct = round((f - 1.0) * 100)
    if pct > 0:
        return f"+{pct}%"
    if pct < 0:
        return f"{pct}%"
    return "0%"


# ---------------------------------------------------------------------------
# MLB API
# ---------------------------------------------------------------------------
def _fetch_game_feed(game_pk: int) -> dict:
    url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()


def _fetch_schedule(date_str: str) -> list:
    games = []
    for sport_id in [1, 51]:
        try:
            r = requests.get(f"{MLB_API}/schedule", params={
                "sportId": sport_id, "date": date_str,
                "hydrate": "lineups,probablePitcher,team,venue",
            }, timeout=15)
            r.raise_for_status()
            for d in r.json().get("dates", []):
                for g in d.get("games", []):
                    games.append(g)
        except Exception:
            pass
    return games


def _extract_game_data(feed: dict) -> dict:
    """Extract lineups, pitchers, venue from game feed."""
    gd = feed.get("gameData") or {}
    live = feed.get("liveData") or {}
    boxscore = live.get("boxscore") or {}
    teams = gd.get("teams") or {}
    players = gd.get("players") or {}
    probables = gd.get("probablePitchers") or {}
    dt = gd.get("datetime") or {}

    home = teams.get("home") or {}
    away = teams.get("away") or {}
    venue = gd.get("venue") or {}
    game_type = (gd.get("game") or {}).get("type", "R")

    def _player_hand(pid, hand_type="batSide"):
        p = players.get(f"ID{pid}") or {}
        return (p.get(hand_type) or {}).get("code", "R")

    def _player_name(pid):
        p = players.get(f"ID{pid}") or {}
        return p.get("fullName") or str(pid)

    def _player_pos(pid, team_box):
        ps = (team_box.get("players") or {}).get(f"ID{pid}") or {}
        return (ps.get("position") or {}).get("abbreviation") or "?"

    def _get_pitcher(side):
        pp = probables.get(side) or {}
        pid = pp.get("id")
        if not pid:
            return None
        return {"id": int(pid), "name": _player_name(pid), "throws": _player_hand(pid, "pitchHand")}

    def _get_lineup(side):
        team_box = (boxscore.get("teams") or {}).get(side) or {}
        bo = team_box.get("battingOrder") or []
        lineup = []
        for i, pid in enumerate(bo[:9], 1):
            pid = int(pid)
            lineup.append({
                "id": pid, "name": _player_name(pid),
                "stand": _player_hand(pid, "batSide"),
                "pos": _player_pos(pid, team_box), "spot": i,
            })
        return lineup

    def _fix_abbrev(a):
        return ABBREV_MAP.get((a or "").upper(), (a or "").upper())

    return {
        "game_pk": (gd.get("game") or {}).get("pk"),
        "away_name": away.get("teamName") or away.get("name", "Away"),
        "home_name": home.get("teamName") or home.get("name", "Home"),
        "away_abbrev": _fix_abbrev(away.get("abbreviation")),
        "home_abbrev": _fix_abbrev(home.get("abbreviation")),
        "away_record": f"{away.get('record', {}).get('wins', 0)}-{away.get('record', {}).get('losses', 0)}",
        "home_record": f"{home.get('record', {}).get('wins', 0)}-{home.get('record', {}).get('losses', 0)}",
        "venue_name": venue.get("name", ""),
        "venue_id": venue.get("id"),
        "venue_abbrev": _fix_abbrev(home.get("abbreviation")),
        "game_date": (dt.get("officialDate") or "")[:10],
        "game_date_iso": dt.get("dateTime") or "",
        "game_time": f"{dt.get('time', '')} {dt.get('ampm', '')}".strip(),
        "game_type": game_type,
        "away_lineup": _get_lineup("away"),
        "home_lineup": _get_lineup("home"),
        "away_pitcher": _get_pitcher("away"),
        "home_pitcher": _get_pitcher("home"),
    }


# ---------------------------------------------------------------------------
# Weather + Park Factors
# ---------------------------------------------------------------------------
def _fetch_weather(venue_id, game_date_iso, game_pk) -> Optional[dict]:
    if not venue_id:
        return None
    try:
        from services.weather import fetch_game_weather
        return fetch_game_weather(venue_id, game_date_iso, game_pk=game_pk)
    except Exception as e:
        print(f"  Weather fetch failed: {e}")
        return None


def _load_park_factor(venue_abbrev: str, season: int) -> Optional[float]:
    try:
        import pandas as pd
        pf_path = ROOT / "data" / "park_factors.parquet"
        if not pf_path.exists():
            return None
        pf = pd.read_parquet(pf_path)
        mask = (pf["venue"] == venue_abbrev) & (pf["stand"] == "ALL")
        row = pf[mask & (pf["season"] == season)]
        if row.empty:
            row = pf[mask].sort_values("season", ascending=False).head(1)
        if row.empty:
            return None
        return float(row.iloc[0]["run_factor"])
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Predictions
# ---------------------------------------------------------------------------
def _run_predictions(lineup, pitcher, venue, season):
    from services.matchup_predict import predict_matchup
    results = []
    if not lineup or not pitcher:
        return results
    for batter in lineup:
        est_inning = (batter["spot"] - 1) // 3 + 1
        pred = predict_matchup(
            batter_id=batter["id"], pitcher_id=pitcher["id"],
            stand=batter["stand"], p_throws=pitcher["throws"],
            venue=venue, season=season, inning=est_inning,
        )
        if pred.get("ok"):
            results.append({**batter, **pred})
        else:
            results.append({**batter, "probs": {}, "summary": {}})
    return results


def _grade(summary):
    if not summary:
        return "C"
    score = (
        summary.get("obp", 0.300) * 2.0
        + summary.get("xslg", 0.380) * 1.5
        + summary.get("hit_pct", 0.230) * 1.0
        - summary.get("k_pct", 0.225) * 1.5
    )
    for t, g in [
        (1.75, "A+"), (1.62, "A"), (1.50, "A-"), (1.40, "B+"),
        (1.27, "B"), (1.12, "B-"), (0.99, "C+"), (0.87, "C"),
        (0.77, "C-"), (0.66, "D+"), (0.53, "D"), (0.35, "D-"),
    ]:
        if score >= t:
            return g
    return "F"


# ---------------------------------------------------------------------------
# Monte Carlo sim
# ---------------------------------------------------------------------------
def _simulate_game(away_results, home_results, sims=5000):
    import random
    outcomes = ["1B", "2B", "3B", "HR", "BB", "HBP", "K", "OUT"]

    def _build_probs(results):
        lineup = []
        for b in results:
            probs = b.get("probs", {})
            if not probs:
                probs = {"1B": 0.15, "2B": 0.045, "3B": 0.004, "HR": 0.033,
                         "BB": 0.08, "HBP": 0.01, "K": 0.22, "OUT": 0.458}
            p = [probs.get(o, 0) for o in outcomes]
            total = sum(p)
            if total > 0:
                p = [x / total for x in p]
            lineup.append(p)
        return lineup

    away_probs = _build_probs(away_results)
    home_probs = _build_probs(home_results)
    away_wins = home_wins = 0
    away_total = home_total = 0.0
    away_run_counts = {}
    home_run_counts = {}

    for _ in range(sims):
        ar, hr_ = _sim_one_game(away_probs, home_probs, outcomes)
        away_total += ar; home_total += hr_
        away_run_counts[ar] = away_run_counts.get(ar, 0) + 1
        home_run_counts[hr_] = home_run_counts.get(hr_, 0) + 1
        if ar > hr_: away_wins += 1
        elif hr_ > ar: home_wins += 1
        else:
            if random.random() < 0.5: away_wins += 1
            else: home_wins += 1

    # Build run frequency distributions (cap at 12)
    max_r = min(12, max(max(away_run_counts.keys(), default=0),
                        max(home_run_counts.keys(), default=0)))
    away_dist = [away_run_counts.get(i, 0) / sims for i in range(max_r + 1)]
    home_dist = [home_run_counts.get(i, 0) / sims for i in range(max_r + 1)]

    # First-inning simulation for NRFI
    nrfi_count = away_1st_scores = home_1st_scores = 0
    for _ in range(sims):
        a1 = _sim_half_inning(away_probs, 0, outcomes)
        h1 = _sim_half_inning(home_probs, 0, outcomes)
        if a1 == 0 and h1 == 0:
            nrfi_count += 1
        if a1 > 0:
            away_1st_scores += 1
        if h1 > 0:
            home_1st_scores += 1

    return {
        "away_win_pct": away_wins / sims, "home_win_pct": home_wins / sims,
        "away_exp_runs": away_total / sims, "home_exp_runs": home_total / sims,
        "away_run_dist": away_dist, "home_run_dist": home_dist,
        "nrfi_pct": nrfi_count / sims,
        "away_1st_score_pct": away_1st_scores / sims,
        "home_1st_score_pct": home_1st_scores / sims,
    }


def _sim_half_inning(lineup_probs, start_idx, outcomes):
    """Simulate one half-inning, return runs scored."""
    import random
    outs = runs = runners = 0
    idx = start_idx
    while outs < 3:
        probs = lineup_probs[idx % len(lineup_probs)]; idx += 1
        r = random.random(); cumulative = 0; outcome = "OUT"
        for i, p in enumerate(probs):
            cumulative += p
            if r < cumulative: outcome = outcomes[i]; break
        if outcome in ("K", "OUT"): outs += 1
        elif outcome in ("BB", "HBP"):
            if runners & 1:
                if runners & 2:
                    if runners & 4: runs += 1
                    runners |= 4
                runners |= 2
            runners |= 1
        elif outcome == "1B":
            if runners & 4: runs += 1; runners &= ~4
            if runners & 2:
                if random.random() < 0.8: runs += 1; runners &= ~2
                else: runners |= 4; runners &= ~2
            if runners & 1: runners |= 2
            runners |= 1
        elif outcome == "2B":
            if runners & 4: runs += 1; runners &= ~4
            if runners & 2: runs += 1; runners &= ~2
            if runners & 1:
                if random.random() < 0.6: runs += 1
                else: runners |= 4
                runners &= ~1
            runners |= 2
        elif outcome == "3B":
            runs += bin(runners).count('1'); runners = 4
        elif outcome == "HR":
            runs += bin(runners).count('1') + 1; runners = 0
    return runs


def _sim_one_game(away_probs, home_probs, outcomes):
    import random

    def _sim_half(lineup_probs, idx):
        outs = runs = runners = 0
        while outs < 3:
            probs = lineup_probs[idx % len(lineup_probs)]; idx += 1
            r = random.random(); cumulative = 0; outcome = "OUT"
            for i, p in enumerate(probs):
                cumulative += p
                if r < cumulative: outcome = outcomes[i]; break
            if outcome in ("K", "OUT"): outs += 1
            elif outcome in ("BB", "HBP"):
                if runners & 1:
                    if runners & 2:
                        if runners & 4: runs += 1
                        runners |= 4
                    runners |= 2
                runners |= 1
            elif outcome == "1B":
                if runners & 4: runs += 1; runners &= ~4
                if runners & 2:
                    if random.random() < 0.8: runs += 1; runners &= ~2
                    else: runners |= 4; runners &= ~2
                if runners & 1: runners |= 2
                runners |= 1
            elif outcome == "2B":
                if runners & 4: runs += 1; runners &= ~4
                if runners & 2: runs += 1; runners &= ~2
                if runners & 1:
                    if random.random() < 0.6: runs += 1
                    else: runners |= 4
                    runners &= ~1
                runners |= 2
            elif outcome == "3B":
                runs += bin(runners).count('1'); runners = 4
            elif outcome == "HR":
                runs += bin(runners).count('1') + 1; runners = 0
        return runs, idx

    away_runs = home_runs = away_idx = home_idx = 0
    for inning in range(1, 10):
        r, away_idx = _sim_half(away_probs, away_idx); away_runs += r
        if inning == 9 and home_runs > away_runs: break
        r, home_idx = _sim_half(home_probs, home_idx); home_runs += r
        if inning >= 9 and home_runs > away_runs: break
    return away_runs, home_runs


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def generate_preview(game_pk: int, out_path: str = None) -> Optional[str]:
    print(f"Fetching game {game_pk}...")
    feed = _fetch_game_feed(game_pk)
    gd = _extract_game_data(feed)

    if not gd["away_lineup"] or not gd["home_lineup"]:
        print("  Lineups not posted yet. Skipping."); return None
    if not gd["away_pitcher"] or not gd["home_pitcher"]:
        print("  Probable pitchers not set. Skipping."); return None

    season = int(gd["game_date"][:4]) if gd["game_date"] else 2026
    print(f"  {gd['away_abbrev']} ({gd['away_record']}) @ {gd['home_abbrev']} ({gd['home_record']})")
    print(f"  {gd['away_pitcher']['name']} vs {gd['home_pitcher']['name']}")

    print("  Fetching weather...")
    weather = _fetch_weather(gd.get("venue_id"), gd.get("game_date_iso", ""), game_pk)
    park_factor = _load_park_factor(gd["home_abbrev"], season)

    print("  Running matchup predictions...")
    away_results = _run_predictions(gd["away_lineup"], gd["home_pitcher"], gd["venue_abbrev"], season)
    home_results = _run_predictions(gd["home_lineup"], gd["away_pitcher"], gd["venue_abbrev"], season)

    print("  Running Monte Carlo simulation (5000 games)...")
    sim = _simulate_game(away_results, home_results)

    print("  Rendering preview...")
    img = _render(gd, away_results, home_results, sim, weather, park_factor)

    if not out_path:
        date_dir = REPORTS_DIR / gd["game_date"].replace("-", "")
        date_dir.mkdir(parents=True, exist_ok=True)
        out_path = str(date_dir / f"{gd['away_abbrev']}_vs_{gd['home_abbrev']}_{game_pk}.png")

    img.save(out_path, "PNG")
    print(f"  Saved: {out_path}")
    return out_path


def _prob_to_american(p):
    if p <= 0 or p >= 1: return 0
    if p >= 0.5: return round(-(p / (1 - p)) * 100)
    return round(((1 - p) / p) * 100)

def _fmt_odds(odds):
    return f"+{odds}" if odds >= 0 else str(odds)

def _env_badge_color(label):
    if "Very hitter" in label: return (22, 128, 54)
    if "Hitter" in label: return (22, 110, 46)
    if "Very pitcher" in label: return (20, 45, 140)
    if "Pitcher" in label: return (30, 60, 160)
    if "Dome" in label: return (50, 50, 50)
    return (30, 30, 30)

def _render(gd, away_results, home_results, sim, weather, park_factor):
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    f_mid = _get_font(20, bold=True)
    f_sm = _get_arial(11, bold=False); f_smb = _get_arial(11, bold=True)
    f_xs = _get_arial(9, bold=False); f_xsb = _get_arial(9, bold=True)
    f_xxs = _get_arial(8, bold=False); f_xxsb = _get_arial(8, bold=True)
    f_pit = _get_arial(12, bold=True)
    f_pct = _get_font(24, bold=True); f_score = _get_font(18, bold=True)
    f_zone = _get_arial(10, bold=True); f_zlbl = _get_arial(7, bold=True)
    f_gauge = _get_arial(12, bold=True)
    f_dlbl = _get_arial(8, bold=True); f_dval = _get_arial(7, bold=False)
    f_thdr = _get_arial(10, bold=True); f_tbl = _get_arial(14, bold=False); f_tblb = _get_arial(14, bold=True)
    f_tname = _get_arial(16, bold=True)
    f_nrfi = _get_arial(11, bold=True); f_nrfi_sm = _get_arial(9, bold=True)
    f_odds = _get_arial(10, bold=True)

    away_abbr = gd["away_abbrev"]; home_abbr = gd["home_abbrev"]
    away_accent = _team_accent(away_abbr); home_accent = _team_accent(home_abbr)
    away_dark = _team_color(away_abbr); home_dark = _team_color(home_abbr)
    mid = W // 2; y = PAD; col_gap = 8
    half_w = (W - 2*PAD - col_gap) // 2

    # ===================================================================
    # HEADER (62px)
    # ===================================================================
    hdr_h = 62
    _rounded_rect(draw, (PAD, y, W-PAD, y+hdr_h), CARD_R, fill=BG_CARD)
    draw.rectangle((PAD, y, mid, y+3), fill=away_accent)
    draw.rectangle((mid, y, W-PAD, y+3), fill=home_accent)
    lsz = 42; ly = y+(hdr_h-lsz)//2
    _paste_logo(img, away_abbr, PAD+10, ly, lsz)
    _paste_logo(img, home_abbr, W-PAD-10-lsz, ly, lsz)
    mnw = mid - PAD - 10 - lsz - 30
    for ns, rec, lx, al in [
        (gd["away_name"].upper(), gd.get("away_record",""), PAD+10+lsz+6, "L"),
        (gd["home_name"].upper(), gd.get("home_record",""), W-PAD-10-lsz-6, "R"),
    ]:
        fsz = 28
        while fsz > 14 and _text_width(draw, ns, _get_font(fsz,True)) > mnw: fsz -= 1
        fn = _get_font(fsz, True)
        if al == "L":
            draw.text((lx, y+10), ns, fill=WHITE, font=fn)
            draw.text((lx, y+10+fsz), rec, fill=TEXT_SECONDARY, font=f_xsb)
        else:
            _draw_right(draw, ns, lx, y+10, fn, WHITE)
            _draw_right(draw, rec, lx, y+10+fsz, f_xsb, TEXT_SECONDARY)
    _draw_centered(draw, gd.get("game_time",""), mid, y+8, f_mid, WHITE)
    ds = gd.get("game_date","")
    try: ds = datetime.strptime(ds, "%Y-%m-%d").strftime("%b %d, %Y").upper()
    except Exception: pass
    _draw_centered(draw, ds, mid, y+28, f_xsb, TEXT_SECONDARY)
    _draw_centered(draw, gd.get("venue_name",""), mid, y+40, f_xs, TEXT_MUTED)
    try:
        bn = Image.open(LOGO_PATH).convert("RGBA").resize((12,12), Image.Resampling.LANCZOS)
        img.paste(bn, (mid-6, y+hdr_h-15), bn)
    except Exception: pass
    y += hdr_h + GAP

    # ===================================================================
    # ROW 2: SEMICIRCLE GAUGE (left) + RUNS DISTRIBUTION (right) — 150px
    # ===================================================================
    row2_h = 150
    away_pct = sim["away_win_pct"]; home_pct = sim["home_win_pct"]
    away_p = gd["away_pitcher"]; home_p = gd["home_pitcher"]

    # -- Left card: gauge --
    _rounded_rect(draw, (PAD, y, PAD+half_w, y+row2_h), CARD_R, fill=BG_CARD)
    gcx = PAD + half_w//2; gcy = y + row2_h - 28
    arc_r = 50; arc_w = 12
    abox = (gcx-arc_r, gcy-arc_r, gcx+arc_r, gcy+arc_r)
    draw.arc(abox, 180, 360, fill=(40,50,70), width=arc_w)
    sp = 180 + int(180*away_pct)
    if away_pct > 0.02: draw.arc(abox, 180, sp, fill=away_accent, width=arc_w)
    if home_pct > 0.02: draw.arc(abox, sp, 360, fill=home_accent, width=arc_w)
    na = math.radians(sp); nx = gcx+int((arc_r-4)*math.cos(na)); ny = gcy+int((arc_r-4)*math.sin(na))
    draw.line((gcx,gcy,nx,ny), fill=WHITE, width=2)
    draw.ellipse((gcx-3,gcy-3,gcx+3,gcy+3), fill=WHITE)
    draw.text((gcx-arc_r-8, gcy+4), f"{away_pct:.0%}", fill=away_accent, font=f_pct)
    draw.text((gcx-arc_r-8, gcy+26), away_abbr, fill=TEXT_MUTED, font=f_xsb)
    _draw_right(draw, f"{home_pct:.0%}", gcx+arc_r+8, gcy+4, f_pct, home_accent)
    _draw_right(draw, home_abbr, gcx+arc_r+8, gcy+26, f_xsb, TEXT_MUTED)
    _draw_centered(draw, "WIN PROBABILITY", gcx, y+4, f_xsb, TEXT_MUTED)
    _draw_centered(draw, f"{sim['away_exp_runs']:.1f} - {sim['home_exp_runs']:.1f}", gcx, y+16, f_score, ACCENT)
    _draw_centered(draw, "EXPECTED SCORE", gcx, y+36, f_xs, TEXT_MUTED)

    # Pitcher info flanking gauge
    px = PAD+8
    draw.text((px, y+6), away_p["name"], fill=TEXT_PRIMARY, font=f_pit)
    draw.text((px, y+19), f"{away_p['throws']}HP", fill=away_accent, font=f_xxsb)
    prx = PAD+half_w-8
    _draw_right(draw, home_p["name"], prx, y+6, f_pit, TEXT_PRIMARY)
    _draw_right(draw, f"{home_p['throws']}HP", prx, y+19, f_xxsb, home_accent)

    # -- Right card: runs distribution --
    rx0 = PAD + half_w + col_gap
    _rounded_rect(draw, (rx0, y, W-PAD, y+row2_h), CARD_R, fill=BG_CARD)
    _draw_centered(draw, "RUNS DISTRIBUTION", rx0+half_w//2, y+4, f_xsb, TEXT_MUTED)
    ad = sim.get("away_run_dist",[]); hd = sim.get("home_run_dist",[])
    ms = min(max(len(ad),len(hd)), 10)
    if ms > 0:
        mp = max(max(ad[:ms],default=0), max(hd[:ms],default=0), 0.01)
        bt = y+18; bh = row2_h-42; mbh = bh-14
        ctw = half_w/ms; bwe = max(3,int(ctw*0.35)); bg = max(1,int(ctw*0.06))
        for i in range(ms):
            cx = rx0+int(ctw*(i+0.5)); av = ad[i] if i<len(ad) else 0; hv = hd[i] if i<len(hd) else 0
            ah = max(2,int(av/mp*mbh)); hh = max(2,int(hv/mp*mbh))
            ax = cx-bwe-bg//2; hx = cx+bg//2; bb = bt+mbh
            _rounded_rect(draw,(ax,bb-ah,ax+bwe,bb),2,fill=away_accent)
            _rounded_rect(draw,(hx,bb-hh,hx+bwe,bb),2,fill=home_accent)
            if av>=0.04: _draw_centered(draw,f"{av*100:.0f}",ax+bwe//2,bb-ah-9,f_dval,away_accent)
            if hv>=0.04: _draw_centered(draw,f"{hv*100:.0f}",hx+bwe//2,bb-hh-9,f_dval,home_accent)
            _draw_centered(draw,str(i),cx,bb+2,f_dlbl,TEXT_MUTED)
        lgy = y+row2_h-12; lgc = rx0+half_w//2
        draw.rectangle((lgc-50,lgy,lgc-42,lgy+6),fill=away_accent)
        draw.text((lgc-39,lgy-2),away_abbr,fill=TEXT_MUTED,font=f_dval)
        draw.rectangle((lgc+10,lgy,lgc+18,lgy+6),fill=home_accent)
        draw.text((lgc+21,lgy-2),home_abbr,fill=TEXT_MUTED,font=f_dval)
    y += row2_h + GAP

    # ===================================================================
    # ROW 3: STADIUM+WEATHER (left) + NRFI CARD (right) — 170px
    # ===================================================================
    row3_h = 170
    impact = (weather or {}).get("impact") or {}

    # -- Left: Stadium --
    _rounded_rect(draw, (PAD, y, PAD+half_w, y+row3_h), CARD_R, fill=BG_CARD)
    svg_size = 130; scx = PAD+half_w//2; st = y+4
    svg_img = _render_stadium_svg(home_abbr, svg_size)
    if svg_img: img.paste(svg_img, (scx-svg_size//2, st), svg_img)
    for zl,zf,zx,zy in [
        ("LF",  impact.get("lf_hr_factor",1.0),  scx-svg_size*0.38, st+svg_size*0.38),
        ("LCF", impact.get("lcf_hr_factor",1.0), scx-svg_size*0.20, st+svg_size*0.16),
        ("CF",  impact.get("cf_hr_factor",1.0),  scx,               st+svg_size*0.04),
        ("RCF", impact.get("rcf_hr_factor",1.0), scx+svg_size*0.20, st+svg_size*0.16),
        ("RF",  impact.get("rf_hr_factor",1.0),  scx+svg_size*0.38, st+svg_size*0.38),
    ]:
        cw_c,ch_c=34,20; cx_c=int(zx-cw_c//2); cy_c=int(zy)
        cc=_zone_chip_color(zf)
        ci=Image.new("RGBA",(cw_c,ch_c),(*cc,180))
        ImageDraw.Draw(ci).rounded_rectangle((0,0,cw_c-1,ch_c-1),radius=3,fill=(*cc,180),outline=(255,255,255,25))
        img.paste(ci,(cx_c,cy_c),ci)
        _draw_centered(draw,zl,int(zx),cy_c+1,f_zlbl,(200,200,200))
        _draw_centered(draw,_fmt_factor(zf),int(zx),cy_c+9,f_zone,WHITE)

    el = impact.get("label","Neutral"); by = y+row3_h-36
    ebg = _env_badge_color(el)
    bw = _text_width(draw,el,f_xsb)+12; bx = scx-bw//2
    _rounded_rect(draw,(bx,by,bx+bw,by+14),4,fill=ebg)
    _draw_centered(draw,el,scx,by+1,f_xsb,WHITE)
    wy = by+18
    if weather:
        dome = weather.get("dome",False) and not weather.get("roof_open",False)
        if dome: ws="72\u00b0F | No Wind | Dome"
        else:
            co=weather.get("condition",""); tp=weather.get("temp_f"); wi=weather.get("wind_mph",0)
            cf=impact.get("components",{}).get("cf_wind_mph",0)
            wd=f"{wi}mph out" if cf>2 else (f"{wi}mph in" if cf<-2 else f"{wi}mph cross")
            ts=f"{tp}\u00b0F" if tp is not None else ""
            ws=" | ".join(p for p in [co,ts,wd] if p)
        _draw_centered(draw,ws,scx,wy,f_xs,TEXT_MUTED)
    if park_factor:
        pc=GREEN_GOOD if park_factor>1.05 else (RED_BAD if park_factor<0.95 else TEXT_MUTED)
        _draw_centered(draw,f"PF {park_factor:.2f}",scx,wy+12 if weather else wy,f_xsb,pc)

    # -- Right: NRFI card --
    nx0 = PAD + half_w + col_gap
    _rounded_rect(draw, (nx0, y, W-PAD, y+row3_h), CARD_R, fill=BG_CARD)

    nrfi_pct = sim.get("nrfi_pct", 0.65); yrfi_pct = 1 - nrfi_pct
    nrfi_odds = _prob_to_american(nrfi_pct); yrfi_odds = _prob_to_american(yrfi_pct)

    # Badge
    if nrfi_pct >= 0.58: badge_txt = "Lean NRFI"; badge_bg = (26,50,26); badge_fg = (81,207,102)
    elif nrfi_pct <= 0.42: badge_txt = "Lean YRFI"; badge_bg = (50,26,26); badge_fg = (255,107,107)
    else: badge_txt = "Toss-Up"; badge_bg = (42,42,26); badge_fg = (255,212,59)

    draw.text((nx0+8, y+4), "1st Inning Scoring", fill=TEXT_PRIMARY, font=f_nrfi)
    bw2 = _text_width(draw, badge_txt, f_xsb) + 12
    bx2 = W - PAD - 8 - bw2
    _rounded_rect(draw, (bx2, y+4, bx2+bw2, y+18), 4, fill=badge_bg)
    _draw_centered(draw, badge_txt, bx2+bw2//2, y+5, f_xsb, badge_fg)

    # Gauge bar
    gx = nx0+8; gr = W-PAD-8; gw = gr-gx; gy = y+24; gh = 20
    nw = max(30, int(gw*nrfi_pct))
    _rounded_rect(draw,(gx,gy,gx+nw,gy+gh),5,fill=(45,106,45))
    _rounded_rect(draw,(gx+nw,gy,gr,gy+gh),5,fill=(180,50,50))
    draw.rectangle((gx+nw-1,gy,gx+nw+1,gy+gh),fill=BG_CARD)
    draw.text((gx+6,gy+3),f"NRFI {nrfi_pct*100:.0f}%",fill=WHITE,font=f_nrfi)
    _draw_right(draw,f"YRFI {yrfi_pct*100:.0f}%",gr-6,gy+3,f_nrfi,WHITE)

    # Per-half breakdown
    a_score = sim.get("away_1st_score_pct",0.3); h_score = sim.get("home_1st_score_pct",0.3)
    a_clean = 1 - a_score; h_clean = 1 - h_score

    half_y = y + 50
    for label, pname, batting, clean, scoring, hx0 in [
        ("Top 1st", home_p["name"], away_abbr, a_clean, a_score, nx0+8),
        ("Bot 1st", away_p["name"], home_abbr, h_clean, h_score, nx0+8+half_w//2),
    ]:
        hw = half_w//2 - 10
        draw.text((hx0, half_y), label, fill=TEXT_PRIMARY, font=f_nrfi_sm)
        draw.text((hx0, half_y+12), f"{pname} vs {batting}", fill=TEXT_MUTED, font=f_xxs)
        # Clean / Score / Exp
        sy = half_y + 24
        cln_color = GREEN_GOOD if clean >= 0.75 else (RED_BAD if clean <= 0.60 else TEXT_PRIMARY)
        scr_color = RED_BAD if scoring >= 0.40 else (GREEN_GOOD if scoring <= 0.25 else TEXT_PRIMARY)
        for lbl, val, clr, ox in [("Clean", f"{clean*100:.0f}%", cln_color, 0),
                                   ("Score", f"{scoring*100:.0f}%", scr_color, 55),
                                   ("xRuns", f"{scoring*0.8:.2f}", TEXT_PRIMARY, 110)]:
            draw.text((hx0+ox, sy), lbl, fill=TEXT_MUTED, font=f_xxs)
            draw.text((hx0+ox, sy+10), val, fill=clr, font=f_nrfi_sm)

    # Odds recommendation
    odds_y = y + row3_h - 30
    if nrfi_pct >= 0.55:
        odds_bg = (26,50,26); odds_fg = (81,207,102)
        odds_txt = f"Bet NRFI at {_fmt_odds(nrfi_odds)} or better"
    elif yrfi_pct >= 0.55:
        odds_bg = (50,26,26); odds_fg = (255,107,107)
        odds_txt = f"Bet YRFI at {_fmt_odds(yrfi_odds)} or better"
    else:
        odds_bg = (35,35,25); odds_fg = (255,212,59)
        odds_txt = f"No edge \u2014 NRFI {_fmt_odds(nrfi_odds)} / YRFI {_fmt_odds(yrfi_odds)}"
    ow = _text_width(draw, odds_txt, f_odds) + 16
    ox = nx0 + (half_w - ow)//2
    _rounded_rect(draw,(ox, odds_y, ox+ow, odds_y+20), 5, fill=odds_bg)
    _draw_centered(draw, odds_txt, nx0+half_w//2, odds_y+3, f_odds, odds_fg)

    y += row3_h + GAP

    # ===================================================================
    # LINEUP TABLES — clean, big font
    # Columns: # | Name | POS | GRD | 1+H | 1+HR | TB | H+R+RBI
    # ===================================================================
    col_w = half_w; footer_h = 16; avail = H - y - footer_h - GAP
    thdr = 22; chdr = 16; tot = 22; oh = thdr+chdr+tot; rh = (avail-oh)//9

    def _est_pa(spot):
        return {1:4.5,2:4.4,3:4.3,4:4.2,5:4.1,6:4.0,7:3.9,8:3.8,9:3.7}.get(spot,4.0)

    def _poisson_at_least_1(lam):
        """P(X >= 1) = 1 - e^(-lambda)"""
        if lam <= 0: return 0.0
        return 1.0 - math.exp(-lam)

    def _draw_lineup(results, pitcher, ta, tac, tdc, x0, y0):
        _rounded_rect(draw,(x0,y0,x0+col_w,y0+thdr),CARD_R,fill=tdc)
        draw.rectangle((x0,y0+CARD_R,x0+col_w,y0+thdr),fill=tdc)
        draw.text((x0+6,y0+3),f"{ta} vs {pitcher['name']}",fill=WHITE,font=f_smb)
        yr=y0+thdr
        draw.rectangle((x0,yr,x0+col_w,yr+chdr),fill=BG_TABLE_HDR)
        cols=[("#",14),("PLAYER",100),("POS",26),("GRD",28),("1+H",42),("1+HR",42),("TB",32),("H+R+RBI",46)]
        td=sum(c[1] for c in cols); sc=(col_w-4)/td; cols=[(n,int(w*sc)) for n,w in cols]
        cx=x0+2
        for n,cw in cols:
            if n: _draw_centered(draw,n,cx+cw//2,yr+2,f_thdr,TEXT_MUTED)
            cx+=cw
        yr+=chdr; ttb=thrbi=0.0
        for i,b in enumerate(results):
            draw.rectangle((x0,yr,x0+col_w,yr+rh),fill=BG_TABLE_ROW if i%2==0 else BG_CARD)
            s=b.get("summary",{}); p=b.get("probs",{}); g=_grade(s)
            pa=_est_pa(b["spot"]); hp=s.get("hit_pct",0); hr_p=p.get("HR",0)
            tb=pa*(p.get("1B",0)+p.get("2B",0)*2+p.get("3B",0)*3+hr_p*4)
            eh=pa*hp; erbi=pa*(hr_p*1.5+p.get("2B",0)*0.5+p.get("3B",0)*0.8+p.get("1B",0)*0.15)
            er=pa*(hp*0.3+hr_p*1.0+(p.get("BB",0)+p.get("HBP",0))*0.15)
            hrbi=eh+er+erbi; ttb+=tb; thrbi+=hrbi

            # Poisson P(1+ hit) and P(1+ HR)
            lam_h = pa * hp
            lam_hr = pa * hr_p
            p_1h = _poisson_at_least_1(lam_h)
            p_1hr = _poisson_at_least_1(lam_hr)

            rm=yr+rh//2; cx=x0+2
            # #
            cw=cols[0][1]; _draw_centered(draw,str(b["spot"]),cx+cw//2,rm-7,f_tbl,TEXT_MUTED); cx+=cw
            # Name — big, fill the row
            cw=cols[1][1]; last=_get_last_name(b["name"])
            nf=16
            while nf>11 and _text_width(draw,last,_get_arial(nf,True))>cw-4: nf-=1
            draw.text((cx+2,rm-nf//2),last,fill=TEXT_PRIMARY,font=_get_arial(nf,True)); cx+=cw
            # Pos
            cw=cols[2][1]; _draw_centered(draw,b["pos"],cx+cw//2,rm-7,f_tbl,TEXT_MUTED); cx+=cw
            # Grade
            cw=cols[3][1]; gc=GRADE_COLORS.get(g,TEXT_SECONDARY)
            gbg=tuple(max(0,c//4) for c in gc); pw=min(24,cw-2); ph=18
            px_g=cx+(cw-pw)//2; py_g=rm-ph//2
            _rounded_rect(draw,(px_g,py_g,px_g+pw,py_g+ph),3,fill=gbg,outline=gc)
            _draw_centered(draw,g,px_g+pw//2,py_g+1,f_xsb,gc); cx+=cw
            # 1+ Hit (Poisson odds)
            cw=cols[4][1]
            h_str = f"{p_1h*100:.0f}%"
            hc=GREEN_GOOD if p_1h>=0.75 else (RED_BAD if p_1h<=0.55 else TEXT_PRIMARY)
            _draw_centered(draw,h_str,cx+cw//2,rm-7,f_tblb,hc); cx+=cw
            # 1+ HR (Poisson odds)
            cw=cols[5][1]
            hr_str = f"{p_1hr*100:.0f}%"
            hrc=GREEN_GOOD if p_1hr>=0.20 else TEXT_PRIMARY
            _draw_centered(draw,hr_str,cx+cw//2,rm-7,f_tblb,hrc); cx+=cw
            # TB
            cw=cols[6][1]; tbc=GREEN_GOOD if tb>=1.5 else TEXT_PRIMARY
            _draw_centered(draw,f"{tb:.1f}",cx+cw//2,rm-7,f_tblb,tbc); cx+=cw
            # H+R+RBI
            cw=cols[7][1]; hrc2=GREEN_GOOD if hrbi>=2.0 else TEXT_PRIMARY
            _draw_centered(draw,f"{hrbi:.1f}",cx+cw//2,rm-7,f_tblb,hrc2)
            yr+=rh
        _rounded_rect(draw,(x0,yr,x0+col_w,yr+tot),CARD_R,fill=BG_TABLE_HDR)
        draw.rectangle((x0,yr,x0+col_w,yr+CARD_R),fill=BG_TABLE_HDR)
        cx=x0+2
        for j in range(6): cx+=cols[j][1]
        _draw_centered(draw,f"{ttb:.1f}",cx+cols[6][1]//2,yr+3,f_tblb,tac); cx+=cols[6][1]
        _draw_centered(draw,f"{thrbi:.1f}",cx+cols[7][1]//2,yr+3,f_tblb,tac)
        return yr+tot-y0

    hl=_draw_lineup(away_results,gd["home_pitcher"],away_abbr,away_accent,away_dark,PAD,y)
    hr_h=_draw_lineup(home_results,gd["away_pitcher"],home_abbr,home_accent,home_dark,PAD+col_w+col_gap,y)
    y+=max(hl,hr_h)+GAP

    draw.text((PAD+6,H-14),"basenerd.com | Matchup model + 5K sims",fill=TEXT_MUTED,font=f_xs)
    _draw_right(draw,"@basenerd",W-PAD-6,H-14,f_xsb,ACCENT)
    return img


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate game preview PNG")
    parser.add_argument("--game_pk", type=int, help="MLB game PK")
    parser.add_argument("--date", type=str, help="Date (YYYY-MM-DD, 'today', 'yesterday')")
    args = parser.parse_args()

    if args.game_pk:
        generate_preview(args.game_pk)
    elif args.date:
        if args.date == "today": d = datetime.now().strftime("%Y-%m-%d")
        elif args.date == "yesterday": d = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        else: d = args.date
        games = _fetch_schedule(d)
        if not games:
            print(f"No games found for {d}"); return
        for g in games:
            gpk = g.get("gamePk")
            away = g.get("teams", {}).get("away", {}).get("team", {}).get("name", "?")
            home = g.get("teams", {}).get("home", {}).get("team", {}).get("name", "?")
            print(f"\n{'=' * 60}\nGame {gpk}: {away} @ {home}")
            try:
                generate_preview(gpk)
            except Exception as e:
                print(f"  Error: {e}")
                import traceback; traceback.print_exc()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
