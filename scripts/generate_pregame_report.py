#!/usr/bin/env python3
"""
Generate a pregame matchup report PNG (Twitter-optimized 1080x1350 portrait).

Pulls lineups from the MLB Stats API, runs matchup predictions for every
batter vs the opposing starter, and renders a styled PNG report.

Usage:
    python scripts/generate_pregame_report.py --game_pk 788096
    python scripts/generate_pregame_report.py --date 2026-03-15
    python scripts/generate_pregame_report.py --date today

Output: reports/pregame/<YYYYMMDD>/<Away_vs_Home>_<game_pk>.png
"""
from __future__ import annotations

import argparse
import io
import json
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import numpy as np
from PIL import Image, ImageDraw, ImageFont

# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

LOGO_PATH = ROOT / "static" / "basenerd-logo-official.png"
REPORTS_DIR = ROOT / "reports" / "pregame"

# ---------------------------------------------------------------------------
# Page dimensions — 1080x1350 portrait (4:5, optimal for Twitter/X mobile)
# ---------------------------------------------------------------------------
W = 1080
H = 1350
PAD = 28
CARD_R = 12
GAP = 10

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
    "A+": (34, 197, 94), "A": (34, 197, 94), "A-": (74, 197, 114),
    "B+": (120, 200, 130), "B": (148, 200, 140), "B-": (170, 200, 160),
    "C+": (200, 200, 170), "C": TEXT_SECONDARY, "C-": (190, 170, 150),
    "D+": (220, 150, 120), "D": (230, 120, 100), "D-": (239, 100, 80),
    "F": RED_BAD,
}

MLB_API = "https://statsapi.mlb.com/api/v1"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _fmt(v, decimals=1, fallback="--"):
    if v is None or (isinstance(v, float) and not math.isfinite(v)):
        return fallback
    if decimals == 0:
        return str(int(round(v)))
    return f"{v:.{decimals}f}"


def _pct(v, decimals=0, fallback="--"):
    if v is None:
        return fallback
    return f"{v*100:.{decimals}f}%"


def _fetch_headshot(pid: int, size: int = 180) -> Optional[Image.Image]:
    url = f"https://img.mlbstatic.com/mlb-photos/image/upload/w_{size},q_100/v1/people/{pid}/headshot/67/current"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200 and len(r.content) > 1000:
            return Image.open(io.BytesIO(r.content)).convert("RGBA")
    except Exception:
        pass
    return None


def _circle_crop(img: Image.Image, size: int) -> Image.Image:
    """Crop an image to a circle."""
    img = img.resize((size, size), Image.Resampling.LANCZOS)
    mask = Image.new("L", (size, size), 0)
    draw = ImageDraw.Draw(mask)
    draw.ellipse((0, 0, size - 1, size - 1), fill=255)
    result = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    result.paste(img, (0, 0), mask)
    return result


def _rounded_rect(draw, xy, r, fill=None, outline=None):
    """Draw a rounded rectangle. xy = (x0, y0, x1, y1)."""
    draw.rounded_rectangle(xy, radius=r, fill=fill, outline=outline)


def _get_font(size, bold=False):
    """Get a font, falling back to default."""
    names = ["arialbd.ttf", "Arial Bold.ttf"] if bold else ["arial.ttf", "Arial.ttf"]
    for name in names:
        for prefix in ["C:/Windows/Fonts/", "/usr/share/fonts/truetype/", ""]:
            try:
                return ImageFont.truetype(prefix + name, size)
            except (OSError, IOError):
                continue
    return ImageFont.load_default()


# ---------------------------------------------------------------------------
# MLB API
# ---------------------------------------------------------------------------
def _fetch_game_feed(game_pk: int) -> dict:
    url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()


def _fetch_schedule(date_str: str) -> list:
    """Fetch all games for a date across sport IDs (MLB=1, WBC=51, spring=1)."""
    games = []
    for sport_id in [1, 51]:
        try:
            r = requests.get(f"{MLB_API}/schedule", params={
                "sportId": sport_id, "date": date_str,
            }, timeout=15)
            r.raise_for_status()
            data = r.json()
            for d in data.get("dates", []):
                for g in d.get("games", []):
                    games.append(g)
        except Exception:
            pass
    return games


def _extract_game_data(feed: dict):
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

    game_type = (gd.get("game") or {}).get("type", "R")
    venue = gd.get("venue") or {}

    def _player_hand(pid, hand_type="batSide"):
        key = f"ID{pid}"
        p = players.get(key) or {}
        return (p.get(hand_type) or {}).get("code", "R")

    def _player_name(pid):
        key = f"ID{pid}"
        p = players.get(key) or {}
        return p.get("fullName") or str(pid)

    def _player_pos(pid, team_box):
        ps = (team_box.get("players") or {}).get(f"ID{pid}") or {}
        return ((ps.get("position") or {}).get("abbreviation") or "?")

    def _get_pitcher(side):
        pp = probables.get(side) or {}
        pid = pp.get("id")
        if not pid:
            return None
        return {
            "id": int(pid),
            "name": _player_name(pid),
            "throws": _player_hand(pid, "pitchHand"),
        }

    def _get_lineup(side):
        team_box = (boxscore.get("teams") or {}).get(side) or {}
        bo = team_box.get("battingOrder") or []
        lineup = []
        for i, pid in enumerate(bo[:9], 1):
            pid = int(pid)
            lineup.append({
                "id": pid,
                "name": _player_name(pid),
                "stand": _player_hand(pid, "batSide"),
                "pos": _player_pos(pid, team_box),
                "spot": i,
            })
        return lineup

    return {
        "game_pk": (gd.get("game") or {}).get("pk"),
        "away_name": away.get("teamName") or away.get("name", "Away"),
        "home_name": home.get("teamName") or home.get("name", "Home"),
        "away_abbrev": (away.get("abbreviation") or "AWY").upper(),
        "home_abbrev": (home.get("abbreviation") or "HME").upper(),
        "venue_name": venue.get("name", ""),
        "venue_abbrev": (home.get("abbreviation") or "").upper(),
        "game_date": (dt.get("officialDate") or "")[:10],
        "game_time": f"{dt.get('time', '')} {dt.get('ampm', '')}".strip(),
        "game_type": game_type,
        "away_lineup": _get_lineup("away"),
        "home_lineup": _get_lineup("home"),
        "away_pitcher": _get_pitcher("away"),
        "home_pitcher": _get_pitcher("home"),
    }


# ---------------------------------------------------------------------------
# Predictions
# ---------------------------------------------------------------------------
def _run_predictions(lineup, pitcher, venue, season):
    """Run matchup predictions for a lineup vs a pitcher."""
    from services.matchup_predict import predict_matchup

    results = []
    if not lineup or not pitcher:
        return results

    for batter in lineup:
        est_inning = (batter["spot"] - 1) // 3 + 1
        pred = predict_matchup(
            batter_id=batter["id"],
            pitcher_id=pitcher["id"],
            stand=batter["stand"],
            p_throws=pitcher["throws"],
            venue=venue,
            season=season,
            inning=est_inning,
        )
        if pred.get("ok"):
            results.append({**batter, **pred})
        else:
            results.append({**batter, "probs": {}, "summary": {}})
    return results


def _grade(summary):
    """Compute matchup grade."""
    if not summary:
        return "C"
    score = (
        summary.get("obp", 0.300) * 2.0
        + summary.get("xslg", 0.380) * 1.5
        + summary.get("hit_pct", 0.230) * 1.0
        - summary.get("k_pct", 0.225) * 1.5
    )
    thresholds = [
        (1.75, "A+"), (1.62, "A"), (1.50, "A-"), (1.40, "B+"),
        (1.27, "B"), (1.12, "B-"), (0.99, "C+"), (0.87, "C"),
        (0.77, "C-"), (0.66, "D+"), (0.53, "D"), (0.35, "D-"),
    ]
    for t, g in thresholds:
        if score >= t:
            return g
    return "F"


def _simulate_game(away_results, home_results, sims=5000):
    """Monte Carlo simulation for win probability and expected score."""
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

    away_wins = 0
    home_wins = 0
    away_total = 0.0
    home_total = 0.0

    for _ in range(sims):
        ar, hr_ = _sim_one_game(away_probs, home_probs, outcomes)
        away_total += ar
        home_total += hr_
        if ar > hr_:
            away_wins += 1
        elif hr_ > ar:
            home_wins += 1
        else:
            if random.random() < 0.5:
                away_wins += 1
            else:
                home_wins += 1

    return {
        "away_win_pct": away_wins / sims,
        "home_win_pct": home_wins / sims,
        "away_exp_runs": away_total / sims,
        "home_exp_runs": home_total / sims,
    }


def _sim_one_game(away_probs, home_probs, outcomes):
    import random

    def _sim_half(lineup_probs, idx):
        outs = 0
        runners = 0
        runs = 0
        while outs < 3:
            probs = lineup_probs[idx % len(lineup_probs)]
            idx += 1
            r = random.random()
            cumulative = 0
            outcome = "OUT"
            for i, p in enumerate(probs):
                cumulative += p
                if r < cumulative:
                    outcome = outcomes[i]
                    break
            if outcome in ("K", "OUT"):
                outs += 1
            elif outcome in ("BB", "HBP"):
                if runners & 1:
                    if runners & 2:
                        if runners & 4:
                            runs += 1
                        runners |= 4
                    runners |= 2
                runners |= 1
            elif outcome == "1B":
                if runners & 4:
                    runs += 1; runners &= ~4
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
                runs += bin(runners).count('1')
                runners = 4
            elif outcome == "HR":
                runs += bin(runners).count('1') + 1
                runners = 0
        return runs, idx

    away_runs = 0
    home_runs = 0
    away_idx = 0
    home_idx = 0
    for inning in range(1, 10):
        r, away_idx = _sim_half(away_probs, away_idx)
        away_runs += r
        if inning == 9 and home_runs > away_runs:
            break
        r, home_idx = _sim_half(home_probs, home_idx)
        home_runs += r
        if inning >= 9 and home_runs > away_runs:
            break
    return away_runs, home_runs


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def generate_report(game_pk: int, out_path: str = None):
    print(f"Fetching game {game_pk}...")
    feed = _fetch_game_feed(game_pk)
    gd = _extract_game_data(feed)

    if not gd["away_lineup"] or not gd["home_lineup"]:
        print("  Lineups not posted yet. Skipping.")
        return None

    if not gd["away_pitcher"] or not gd["home_pitcher"]:
        print("  Probable pitchers not set. Skipping.")
        return None

    season = int(gd["game_date"][:4]) if gd["game_date"] else 2026

    print(f"  {gd['away_name']} @ {gd['home_name']}")
    print(f"  {gd['away_pitcher']['name']} vs {gd['home_pitcher']['name']}")

    # Run predictions
    print("  Running matchup predictions...")
    away_results = _run_predictions(
        gd["away_lineup"], gd["home_pitcher"], gd["venue_abbrev"], season
    )
    home_results = _run_predictions(
        gd["home_lineup"], gd["away_pitcher"], gd["venue_abbrev"], season
    )

    # Simulate
    print("  Running Monte Carlo simulation (5000 games)...")
    sim = _simulate_game(away_results, home_results)

    # Render
    print("  Rendering report...")
    img = _render(gd, away_results, home_results, sim)

    # Save
    if not out_path:
        date_dir = REPORTS_DIR / gd["game_date"].replace("-", "")
        date_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{gd['away_abbrev']}_vs_{gd['home_abbrev']}_{game_pk}.png"
        out_path = str(date_dir / filename)

    img.save(out_path, "PNG")
    print(f"  Saved: {out_path}")
    return out_path


def _render(gd, away_results, home_results, sim):
    """Render the full pregame report as a PIL Image."""
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    # Fonts
    f_title = _get_font(28, bold=True)
    f_subtitle = _get_font(16, bold=True)
    f_label = _get_font(13, bold=False)
    f_label_b = _get_font(13, bold=True)
    f_small = _get_font(13, bold=False)
    f_small_b = _get_font(13, bold=True)
    f_big = _get_font(22, bold=True)
    f_grade = _get_font(16, bold=True)
    f_table = _get_font(16, bold=False)
    f_table_b = _get_font(16, bold=True)
    f_pct = _get_font(36, bold=True)
    f_pct_label = _get_font(14, bold=True)
    f_footer = _get_font(11, bold=False)
    f_footer_b = _get_font(12, bold=True)
    f_header = _get_font(15, bold=False)

    y = PAD

    away_p = gd["away_pitcher"]
    home_p = gd["home_pitcher"]

    # Compute pitcher expected stats from opposing lineup predictions
    def _pitcher_stats(opposing_results):
        valid = [b for b in opposing_results if b.get("probs")]
        if not valid:
            return {"ip": "0.0", "k": 0, "bb": 0, "h": 0, "hr": 0, "er": 0}
        n = len(valid)
        avg_out = sum(b["probs"].get("OUT", 0) + b["probs"].get("K", 0) for b in valid) / n
        avg_k = sum(b["probs"].get("K", 0) for b in valid) / n
        avg_bb = sum(b["probs"].get("BB", 0) + b["probs"].get("IBB", 0) + b["probs"].get("HBP", 0) for b in valid) / n
        avg_hit = sum(b.get("summary", {}).get("hit_pct", 0) for b in valid) / n
        avg_hr = sum(b["probs"].get("HR", 0) for b in valid) / n
        est_bf = 24
        est_outs = est_bf * avg_out
        full_ip = int(est_outs) // 3
        rem = int(round(est_outs)) % 3
        ip_str = f"{full_ip}.{rem}"
        er = est_bf * (avg_hr * 1.4 + (avg_hit - avg_hr) * 0.25 + avg_bb * 0.12)
        return {
            "ip": ip_str, "k": round(est_bf * avg_k, 1),
            "bb": round(est_bf * avg_bb, 1), "h": round(est_bf * avg_hit, 1),
            "hr": round(est_bf * avg_hr, 1), "er": round(er, 1),
        }

    # Away pitcher faces home lineup; home pitcher faces away lineup
    away_p_stats = _pitcher_stats(home_results)
    home_p_stats = _pitcher_stats(away_results)

    # -----------------------------------------------------------------------
    # Header — compact title bar
    # -----------------------------------------------------------------------
    header_h = 52
    _rounded_rect(draw, (PAD, y, W - PAD, y + header_h), CARD_R, fill=BG_CARD)

    title = f"{gd['away_name']}  @  {gd['home_name']}"
    bbox = draw.textbbox((0, 0), title, font=f_title)
    tw = bbox[2] - bbox[0]
    draw.text(((W - tw) // 2, y + 4), title, fill=TEXT_PRIMARY, font=f_title)

    sub = f"PREGAME MATCHUP REPORT  •  {gd['game_date']}  •  {gd['game_time']}"
    bbox = draw.textbbox((0, 0), sub, font=f_label)
    sw = bbox[2] - bbox[0]
    draw.text(((W - sw) // 2, y + 32), sub, fill=TEXT_SECONDARY, font=f_label)

    # Logo top-right
    try:
        logo = Image.open(LOGO_PATH).convert("RGBA")
        logo = logo.resize((36, 36), Image.Resampling.LANCZOS)
        img.paste(logo, (W - PAD - 44, y + 8), logo)
    except Exception:
        pass

    y += header_h + GAP

    # -----------------------------------------------------------------------
    # Pitcher matchup card with expected stats
    # -----------------------------------------------------------------------
    pitch_h = 72
    _rounded_rect(draw, (PAD, y, W - PAD, y + pitch_h), CARD_R, fill=BG_CARD)

    mid = W // 2

    # Away pitcher (left side)
    draw.text((PAD + 16, y + 6), away_p["name"], fill=TEXT_PRIMARY, font=f_subtitle)
    draw.text((PAD + 16, y + 26), f"{gd['away_abbrev']}  •  {away_p['throws']}HP", fill=ACCENT, font=f_small_b)
    # Pitcher stats line
    away_stat_line = f"{away_p_stats['ip']} IP   {away_p_stats['k']} K   {away_p_stats['bb']} BB   {away_p_stats['h']} H   {away_p_stats['hr']} HR   {away_p_stats['er']} ER"
    draw.text((PAD + 16, y + 44), away_stat_line, fill=TEXT_SECONDARY, font=f_small)

    # "vs" center
    vs_bbox = draw.textbbox((0, 0), "vs", font=f_label_b)
    draw.text((mid - (vs_bbox[2] - vs_bbox[0]) // 2, y + 14), "vs", fill=TEXT_MUTED, font=f_label_b)

    # Home pitcher (right side)
    home_txt = f"{home_p['name']}"
    bbox = draw.textbbox((0, 0), home_txt, font=f_subtitle)
    htw = bbox[2] - bbox[0]
    draw.text((W - PAD - 16 - htw, y + 6), home_txt, fill=TEXT_PRIMARY, font=f_subtitle)

    home_team_txt = f"{home_p['throws']}HP  •  {gd['home_abbrev']}"
    bbox2 = draw.textbbox((0, 0), home_team_txt, font=f_small_b)
    draw.text((W - PAD - 16 - (bbox2[2] - bbox2[0]), y + 26), home_team_txt, fill=ACCENT, font=f_small_b)

    home_stat_line = f"{home_p_stats['ip']} IP   {home_p_stats['k']} K   {home_p_stats['bb']} BB   {home_p_stats['h']} H   {home_p_stats['hr']} HR   {home_p_stats['er']} ER"
    bbox3 = draw.textbbox((0, 0), home_stat_line, font=f_small)
    draw.text((W - PAD - 16 - (bbox3[2] - bbox3[0]), y + 44), home_stat_line, fill=TEXT_SECONDARY, font=f_small)

    y += pitch_h + GAP

    # -----------------------------------------------------------------------
    # Win probability + expected score — single compact bar
    # -----------------------------------------------------------------------
    sim_h = 56
    _rounded_rect(draw, (PAD, y, W - PAD, y + sim_h), CARD_R, fill=BG_CARD)

    content_w = W - 2 * PAD
    bar_x = PAD + 16
    bar_right = W - PAD - 16

    away_pct = sim["away_win_pct"]
    home_pct = sim["home_win_pct"]

    # Left: away win % + abbrev
    away_pct_str = f"{away_pct:.0%}"
    draw.text((bar_x, y + 6), away_pct_str, fill=TEXT_PRIMARY, font=f_big)
    apw = draw.textbbox((0, 0), away_pct_str, font=f_big)[2]
    draw.text((bar_x + apw + 6, y + 12), gd["away_abbrev"], fill=TEXT_SECONDARY, font=f_small_b)

    # Right: home win % + abbrev
    home_pct_str = f"{home_pct:.0%}"
    hpw = draw.textbbox((0, 0), home_pct_str, font=f_big)[2]
    draw.text((bar_right - hpw, y + 6), home_pct_str, fill=TEXT_PRIMARY, font=f_big)
    ha_txt = gd["home_abbrev"]
    ha_w = draw.textbbox((0, 0), ha_txt, font=f_small_b)[2]
    draw.text((bar_right - hpw - ha_w - 6, y + 12), ha_txt, fill=TEXT_SECONDARY, font=f_small_b)

    # Center: expected score
    exp_score = f"{sim['away_exp_runs']:.1f} – {sim['home_exp_runs']:.1f}"
    es_bbox = draw.textbbox((0, 0), exp_score, font=f_big)
    esw = es_bbox[2] - es_bbox[0]
    draw.text(((W - esw) // 2, y + 6), exp_score, fill=ACCENT, font=f_big)

    es_label = "EXPECTED SCORE"
    esl_bbox = draw.textbbox((0, 0), es_label, font=f_small)
    draw.text(((W - (esl_bbox[2] - esl_bbox[0])) // 2, y + 30), es_label, fill=TEXT_MUTED, font=f_small)

    # Win probability bar
    bar_y = y + 44
    bar_h = 6
    bar_total_w = bar_right - bar_x
    _rounded_rect(draw, (bar_x, bar_y, bar_right, bar_y + bar_h), 3, fill=(40, 55, 80))
    away_bar_w = max(4, int(bar_total_w * away_pct))
    _rounded_rect(draw, (bar_x, bar_y, bar_x + away_bar_w, bar_y + bar_h), 3, fill=ACCENT)

    y += sim_h + GAP

    # -----------------------------------------------------------------------
    # Lineup tables (side by side)
    # -----------------------------------------------------------------------
    col_w = (W - 2 * PAD - GAP) // 2
    table_x_left = PAD
    table_x_right = PAD + col_w + GAP

    # Calculate row height to fill remaining space
    footer_h = 28
    available_h = H - y - footer_h - GAP - PAD
    # Each table: header(28) + col_hdr(24) + 9 rows + team_totals(36)
    hdr_h = 28
    col_hdr_h = 24
    totals_h = 36
    overhead = hdr_h + col_hdr_h + totals_h
    row_h = (available_h - overhead) // 9

    def _est_pa(spot):
        """Estimate plate appearances for a lineup spot in a 9-inning game."""
        # Top of order gets more PAs
        pa_by_spot = {1: 4.5, 2: 4.4, 3: 4.3, 4: 4.2, 5: 4.1, 6: 4.0, 7: 3.9, 8: 3.8, 9: 3.7}
        return pa_by_spot.get(spot, 4.0)

    def _draw_lineup_table(draw, img, results, pitcher, team_abbrev, x0, y0, col_width):
        """Draw a lineup table with game-level counting stats."""
        # Header
        _rounded_rect(draw, (x0, y0, x0 + col_width, y0 + hdr_h), CARD_R, fill=BG_TABLE_HDR)
        hdr_txt = f"{team_abbrev} vs {pitcher['name']}"
        draw.text((x0 + 10, y0 + 5), hdr_txt, fill=ACCENT, font=f_table_b)

        y_row = y0 + hdr_h

        # Column headers — two rows of stats
        _rounded_rect(draw, (x0, y_row, x0 + col_width, y_row + col_hdr_h), 0, fill=BG_CARD)

        # Columns: # | Player | Grade | H | HR | BB | K | AVG | OBP | xSLG
        cols = [
            ("", 20),        # spot
            ("BATTER", 138),  # name + headshot
            ("GRD", 34),     # grade pill
            ("H", 34),       # expected hits
            ("HR", 34),      # expected HR
            ("BB", 34),      # expected BB
            ("K", 34),       # expected K
            ("AVG", 42),     # batting avg
            ("OBP", 42),     # on-base pct
            ("xSLG", 42),    # expected slugging
        ]
        total_defined = sum(c[1] for c in cols)
        scale = (col_width - 8) / total_defined
        cols = [(name, int(w * scale)) for name, w in cols]

        cx = x0 + 4
        for name, cw in cols:
            if name:
                bbox = draw.textbbox((0, 0), name, font=f_small_b)
                tw = bbox[2] - bbox[0]
                draw.text((cx + (cw - tw) // 2, y_row + 5), name, fill=TEXT_MUTED, font=f_small_b)
            cx += cw

        y_row += col_hdr_h

        # Track team totals
        team_h = 0.0
        team_hr = 0.0
        team_bb = 0.0
        team_k = 0.0

        # Batter rows
        for i, batter in enumerate(results):
            is_alt = i % 2 == 0
            row_bg = BG_TABLE_ROW if is_alt else BG_CARD
            if i == len(results) - 1:
                draw.rectangle((x0, y_row, x0 + col_width, y_row + row_h), fill=row_bg)
            else:
                draw.rectangle((x0, y_row, x0 + col_width, y_row + row_h), fill=row_bg)

            summary = batter.get("summary", {})
            probs = batter.get("probs", {})
            grade_val = _grade(summary)

            # Compute game-level counting stats
            pa = _est_pa(batter["spot"])
            hit_pct = summary.get("hit_pct", 0)
            hr_pct = probs.get("HR", 0)
            bb_pct = probs.get("BB", 0) + probs.get("IBB", 0) + probs.get("HBP", 0)
            k_pct = probs.get("K", 0)

            exp_h = pa * hit_pct
            exp_hr = pa * hr_pct
            exp_bb = pa * bb_pct
            exp_k = pa * k_pct

            team_h += exp_h
            team_hr += exp_hr
            team_bb += exp_bb
            team_k += exp_k

            cx = x0 + 4

            # Spot number
            cw = cols[0][1]
            spot_bbox = draw.textbbox((0, 0), str(batter["spot"]), font=f_table)
            spot_y = y_row + (row_h - (spot_bbox[3] - spot_bbox[1])) // 2
            draw.text((cx + 4, spot_y), str(batter["spot"]), fill=TEXT_MUTED, font=f_table)
            cx += cw

            # Player name + pos
            cw = cols[1][1]
            name_x = cx + 4
            f_first = _get_font(15, bold=False)
            f_last = _get_font(20, bold=True)
            f_pos = _get_font(12, bold=False)
            name_parts = batter["name"].split(" ", 1)
            if len(name_parts) == 2:
                draw.text((name_x, y_row + (row_h // 2 - 32)), name_parts[0], fill=TEXT_SECONDARY, font=f_first)
                draw.text((name_x, y_row + (row_h // 2 - 14)), name_parts[1], fill=TEXT_PRIMARY, font=f_last)
            else:
                draw.text((name_x, y_row + (row_h // 2 - 12)), batter["name"], fill=TEXT_PRIMARY, font=f_last)
            pos_txt = f"{batter['pos']} • {batter['stand']}"
            draw.text((name_x, y_row + (row_h // 2 + 12)), pos_txt, fill=TEXT_MUTED, font=f_pos)
            cx += cw

            # Grade pill
            cw = cols[2][1]
            grade_color = GRADE_COLORS.get(grade_val, TEXT_SECONDARY)
            pill_bg = tuple(max(0, c // 4) for c in grade_color)
            pill_w = min(36, cw - 2)
            pill_x = cx + (cw - pill_w) // 2
            pill_y = y_row + (row_h - 24) // 2
            _rounded_rect(draw, (pill_x, pill_y, pill_x + pill_w, pill_y + 24),
                          6, fill=pill_bg, outline=grade_color)
            bbox = draw.textbbox((0, 0), grade_val, font=f_table_b)
            gw = bbox[2] - bbox[0]
            draw.text((pill_x + (pill_w - gw) // 2, pill_y + 2),
                      grade_val, fill=grade_color, font=f_table_b)
            cx += cw

            # Counting stats: H, HR, BB, K
            counting_vals = [exp_h, exp_hr, exp_bb, exp_k]
            counting_colors = [TEXT_PRIMARY, TEXT_PRIMARY, TEXT_PRIMARY, TEXT_PRIMARY]
            # Color-code
            if exp_hr >= 0.3:
                counting_colors[1] = GREEN_GOOD
            if exp_k >= 1.2:
                counting_colors[3] = RED_BAD
            elif exp_k <= 0.6:
                counting_colors[3] = GREEN_GOOD
            if exp_bb >= 0.5:
                counting_colors[2] = GREEN_GOOD

            for j, (val, color) in enumerate(zip(counting_vals, counting_colors)):
                cw = cols[3 + j][1]
                val_str = f"{val:.1f}"
                bbox = draw.textbbox((0, 0), val_str, font=f_table_b)
                vw = bbox[2] - bbox[0]
                vh = bbox[3] - bbox[1]
                stat_y = y_row + (row_h - vh) // 2
                draw.text((cx + (cw - vw) // 2, stat_y), val_str, fill=color, font=f_table_b)
                cx += cw

            # Rate stats: AVG, OBP, xSLG
            rate_vals = [
                f"{summary.get('xba', 0):.3f}" if summary.get('xba') else "--",
                f"{summary.get('obp', 0):.3f}" if summary.get('obp') else "--",
                f"{summary.get('xslg', 0):.3f}" if summary.get('xslg') else "--",
            ]
            rate_colors = [TEXT_PRIMARY, TEXT_PRIMARY, TEXT_PRIMARY]
            # Color-code rates
            if summary.get('xba', 0) >= 0.300:
                rate_colors[0] = GREEN_GOOD
            elif summary.get('xba', 0) <= 0.200:
                rate_colors[0] = RED_BAD
            if summary.get('obp', 0) >= 0.370:
                rate_colors[1] = GREEN_GOOD
            elif summary.get('obp', 0) <= 0.270:
                rate_colors[1] = RED_BAD
            if summary.get('xslg', 0) >= 0.500:
                rate_colors[2] = GREEN_GOOD
            elif summary.get('xslg', 0) <= 0.330:
                rate_colors[2] = RED_BAD

            for j, (val_str, color) in enumerate(zip(rate_vals, rate_colors)):
                cw = cols[7 + j][1]
                bbox = draw.textbbox((0, 0), val_str, font=f_table)
                vw = bbox[2] - bbox[0]
                vh = bbox[3] - bbox[1]
                stat_y = y_row + (row_h - vh) // 2
                draw.text((cx + (cw - vw) // 2, stat_y), val_str, fill=color, font=f_table)
                cx += cw

            y_row += row_h

        # Team totals row
        _rounded_rect(draw, (x0, y_row, x0 + col_width, y_row + totals_h),
                      CARD_R, fill=BG_TABLE_HDR)

        cx = x0 + 4
        # Skip spot + name columns
        cx += cols[0][1] + cols[1][1]
        # "TOTAL" in grade column
        cw = cols[2][1]
        draw.text((cx + 2, y_row + 10), "TOT", fill=TEXT_SECONDARY, font=f_small_b)
        cx += cw

        total_vals = [team_h, team_hr, team_bb, team_k]
        total_labels = ["H", "HR", "BB", "K"]
        for j, (val, label) in enumerate(zip(total_vals, total_labels)):
            cw = cols[3 + j][1]
            val_str = f"{val:.1f}"
            bbox = draw.textbbox((0, 0), val_str, font=f_table_b)
            vw = bbox[2] - bbox[0]
            draw.text((cx + (cw - vw) // 2, y_row + 10), val_str, fill=ACCENT, font=f_table_b)
            cx += cw

        return y_row + totals_h - y0

    # Draw both tables
    table_h_left = _draw_lineup_table(
        draw, img, away_results, gd["home_pitcher"],
        gd["away_abbrev"], table_x_left, y, col_w,
    )
    table_h_right = _draw_lineup_table(
        draw, img, home_results, gd["away_pitcher"],
        gd["home_abbrev"], table_x_right, y, col_w,
    )

    y += max(table_h_left, table_h_right) + GAP

    # -----------------------------------------------------------------------
    # Footer
    # -----------------------------------------------------------------------
    footer_txt = f"basenerd.com  •  Data: MLB Stats API  •  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    draw.text((PAD + 8, H - 24), footer_txt, fill=TEXT_MUTED, font=f_footer)
    brand = "@basenerd"
    bbox = draw.textbbox((0, 0), brand, font=f_footer_b)
    bw = bbox[2] - bbox[0]
    draw.text((W - PAD - 8 - bw, H - 24), brand, fill=ACCENT, font=f_footer_b)

    return img


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate pregame matchup report PNG")
    parser.add_argument("--game_pk", type=int, help="MLB game PK")
    parser.add_argument("--date", type=str, help="Date (YYYY-MM-DD, 'today', 'yesterday')")
    args = parser.parse_args()

    if args.game_pk:
        generate_report(args.game_pk)
    elif args.date:
        if args.date == "today":
            d = datetime.now().strftime("%Y-%m-%d")
        elif args.date == "yesterday":
            d = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            d = args.date

        games = _fetch_schedule(d)
        if not games:
            print(f"No games found for {d}")
            return

        for g in games:
            gpk = g.get("gamePk")
            away = g.get("teams", {}).get("away", {}).get("team", {}).get("name", "?")
            home = g.get("teams", {}).get("home", {}).get("team", {}).get("name", "?")
            print(f"\n{'='*60}")
            print(f"Game {gpk}: {away} @ {home}")
            try:
                generate_report(gpk)
            except Exception as e:
                print(f"  Error: {e}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
