#!/usr/bin/env python3
"""
Build umpire metrics from MLB live feed API (no statcast dependency).

For each completed game, fetches the live feed, extracts all called pitches,
classifies correct/incorrect, and aggregates per-umpire per-season metrics.

Output: data/umpire_metrics.parquet + data/game_outcomes.parquet (updated)
"""

import os
import sys
import time
import json
import requests
import pandas as pd
import numpy as np
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT_PATH = os.path.join(DATA_DIR, "umpire_metrics.parquet")
GAME_OUTCOMES_PATH = os.path.join(DATA_DIR, "game_outcomes.parquet")

API_BASE = "https://statsapi.mlb.com/api/v1"
LIVE_BASE = "https://statsapi.mlb.com/api/v1.1/game"

# Zone boundary: half plate width in feet (17 inches / 2 / 12)
PLATE_HALF = 17.0 / 24.0  # 0.7083 ft
SHADOW_BUFFER = 0.42  # ~5 inches


def get_schedule(season, game_type="S"):
    """Get all completed games for a season/type."""
    url = f"{API_BASE}/schedule"
    params = {
        "sportId": 1,
        "season": season,
        "gameType": game_type,
        "startDate": f"{season}-02-01",
        "endDate": f"{season}-11-30",
        "hydrate": "probablePitcher,linescore,venue,team",
    }
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()

    games = []
    for gd in resp.json().get("dates", []):
        for g in gd.get("games", []):
            status = g.get("status", {}).get("abstractGameState", "")
            if status == "Final":
                games.append({
                    "game_pk": g["gamePk"],
                    "game_date": gd["date"],
                    "season": season,
                    "home_team": g["teams"]["home"]["team"].get("abbreviation", ""),
                    "away_team": g["teams"]["away"]["team"].get("abbreviation", ""),
                    "home_score": g["teams"]["home"].get("score", 0),
                    "away_score": g["teams"]["away"].get("score", 0),
                    "total_runs": g["teams"]["home"].get("score", 0) + g["teams"]["away"].get("score", 0),
                })
    return games


def process_game_feed(game_pk):
    """Fetch live feed and extract called pitch data + umpire info."""
    url = f"{LIVE_BASE}/{game_pk}/feed/live"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            return None
        feed = resp.json()
    except Exception:
        return None

    game_data = feed.get("gameData") or {}
    live_data = feed.get("liveData") or {}

    # Umpire info
    boxscore = live_data.get("boxscore") or {}
    officials = boxscore.get("officials") or []
    hp_umpire_id = None
    hp_umpire_name = None
    for official in officials:
        if official.get("officialType") == "Home Plate":
            ump = official.get("official") or {}
            hp_umpire_id = ump.get("id")
            hp_umpire_name = ump.get("fullName")
            break

    if not hp_umpire_id:
        return None

    # Walk all plays/pitches
    all_plays = (live_data.get("plays") or {}).get("allPlays") or []
    pitches = []
    challenges = []

    for play in all_plays:
        matchup = play.get("matchup") or {}
        bat_side = ((matchup.get("batSide") or {}).get("code") or "R").upper()

        for ev in play.get("playEvents") or []:
            if not ev.get("isPitch"):
                continue

            details = ev.get("details") or {}
            call_code = ((details.get("call") or {}).get("code") or "").upper()

            # Only called strikes and balls
            is_called_strike = call_code == "C"
            is_ball = call_code in ("B", "*B")
            if not is_called_strike and not is_ball:
                # Check for ABS challenge even on non-called pitches
                review = ev.get("reviewDetails")
                if isinstance(review, dict) and review.get("reviewType") == "MJ":
                    challenges.append({
                        "overturned": bool(review.get("isOverturned")),
                    })
                continue

            pitch_data = ev.get("pitchData") or {}
            coords = pitch_data.get("coordinates") or {}
            px = coords.get("pX")
            pz = coords.get("pZ")
            sz_top = pitch_data.get("strikeZoneTop")
            sz_bot = pitch_data.get("strikeZoneBottom")

            if px is None or pz is None or sz_top is None or sz_bot is None:
                continue

            try:
                px = float(px)
                pz = float(pz)
                sz_top = float(sz_top)
                sz_bot = float(sz_bot)
            except (ValueError, TypeError):
                continue

            pitch_type = (details.get("type") or {}).get("code") or ""

            # Zone classification
            in_zone = abs(px) <= PLATE_HALF and pz >= sz_bot and pz <= sz_top

            # Shadow zones
            shadow_high = (pz > sz_top and pz <= sz_top + SHADOW_BUFFER
                          and abs(px) <= PLATE_HALF + SHADOW_BUFFER)
            shadow_low = (pz < sz_bot and pz >= sz_bot - SHADOW_BUFFER
                         and abs(px) <= PLATE_HALF + SHADOW_BUFFER)

            # Correct call?
            if is_called_strike:
                correct = in_zone
            else:
                correct = not in_zone

            pitches.append({
                "px": px,
                "pz": pz,
                "sz_top": sz_top,
                "sz_bot": sz_bot,
                "call": "strike" if is_called_strike else "ball",
                "correct": correct,
                "in_zone": in_zone,
                "shadow_high": shadow_high,
                "shadow_low": shadow_low,
                "pitch_type": pitch_type,
                "stand": bat_side,
            })

            # Check for ABS challenge
            review = ev.get("reviewDetails")
            if isinstance(review, dict) and review.get("reviewType") == "MJ":
                challenges.append({
                    "overturned": bool(review.get("isOverturned")),
                })

    return {
        "hp_umpire_id": hp_umpire_id,
        "hp_umpire_name": hp_umpire_name,
        "pitches": pitches,
        "challenges": challenges,
    }


def build():
    # Configurable: which seasons and game types to process
    season_configs = [
        (2026, "S"),  # 2026 spring training
    ]

    # Collect all game-level umpire data
    umpire_data = defaultdict(lambda: {
        "name": None,
        "games": 0,
        "total_called": 0,
        "correct": 0,
        "incorrect_strikes": 0,  # OOZ called strikes
        "incorrect_balls": 0,    # IZ called balls
        "iz_total": 0,
        "ooz_total": 0,
        "shadow_high_total": 0,
        "shadow_high_strikes": 0,
        "shadow_low_total": 0,
        "shadow_low_strikes": 0,
        "total_runs": 0,
        "abs_challenges": 0,
        "abs_overturned": 0,
        "pitch_type_stats": defaultdict(lambda: {"total": 0, "correct": 0}),
    })

    game_outcomes_rows = []

    for season, game_type in season_configs:
        print(f"\n--- Season {season} ({game_type}) ---")
        games = get_schedule(season, game_type)
        print(f"  {len(games)} completed games")

        for i, game in enumerate(games):
            game_pk = game["game_pk"]
            result = process_game_feed(game_pk)

            if result is None:
                game["hp_umpire_id"] = None
                game["hp_umpire_name"] = None
                game_outcomes_rows.append(game)
                time.sleep(0.05)
                continue

            ump_id = result["hp_umpire_id"]
            ump_name = result["hp_umpire_name"]
            pitches = result["pitches"]
            challenges = result["challenges"]

            game["hp_umpire_id"] = ump_id
            game["hp_umpire_name"] = ump_name
            game_outcomes_rows.append(game)

            key = (ump_id, season)
            d = umpire_data[key]
            d["name"] = ump_name
            d["games"] += 1
            d["total_runs"] += game["total_runs"]

            for p in pitches:
                d["total_called"] += 1
                if p["correct"]:
                    d["correct"] += 1

                if p["in_zone"]:
                    d["iz_total"] += 1
                    if p["call"] == "ball":
                        d["incorrect_balls"] += 1
                else:
                    d["ooz_total"] += 1
                    if p["call"] == "strike":
                        d["incorrect_strikes"] += 1

                if p["shadow_high"]:
                    d["shadow_high_total"] += 1
                    if p["call"] == "strike":
                        d["shadow_high_strikes"] += 1

                if p["shadow_low"]:
                    d["shadow_low_total"] += 1
                    if p["call"] == "strike":
                        d["shadow_low_strikes"] += 1

                pt = p["pitch_type"]
                if pt:
                    d["pitch_type_stats"][pt]["total"] += 1
                    if p["correct"]:
                        d["pitch_type_stats"][pt]["correct"] += 1

            for c in challenges:
                d["abs_challenges"] += 1
                if c["overturned"]:
                    d["abs_overturned"] += 1

            if (i + 1) % 25 == 0:
                print(f"    {i + 1}/{len(games)} games processed")
            time.sleep(0.05)

        print(f"    {len(games)}/{len(games)} games processed")

    # --- Compute league averages for zone_size_factor ---
    all_ooz_strikes = sum(d["incorrect_strikes"] for d in umpire_data.values())
    all_ooz_total = sum(d["ooz_total"] for d in umpire_data.values())
    lg_ooz_cs_rate = all_ooz_strikes / all_ooz_total if all_ooz_total > 0 else 0.08
    all_runs = sum(d["total_runs"] for d in umpire_data.values())
    all_games = sum(d["games"] for d in umpire_data.values())
    lg_avg_runs = all_runs / all_games if all_games > 0 else 9.0

    # --- Build output DataFrame ---
    rows = []
    for (ump_id, season), d in umpire_data.items():
        if d["total_called"] == 0:
            continue

        total = d["total_called"]
        ooz_cs_rate = d["incorrect_strikes"] / d["ooz_total"] if d["ooz_total"] > 0 else None
        iz_ball_rate = d["incorrect_balls"] / d["iz_total"] if d["iz_total"] > 0 else None

        rows.append({
            "hp_umpire_id": ump_id,
            "season": season,
            "games": d["games"],
            "total_called": total,
            "overall_cs_rate": (total - d["iz_total"] - d["incorrect_strikes"]
                                + d["iz_total"] - d["incorrect_balls"]) / total
                               if total > 0 else None,
            # Simpler: overall_cs_rate = called strikes / total
            # called strikes = IZ strikes + OOZ strikes
            "overall_cs_rate": ((d["iz_total"] - d["incorrect_balls"])
                                + d["incorrect_strikes"]) / total,
            "ooz_cs_rate": ooz_cs_rate,
            "iz_ball_rate": iz_ball_rate,
            "zone_size_factor": (ooz_cs_rate / lg_ooz_cs_rate) if ooz_cs_rate is not None and lg_ooz_cs_rate > 0 else None,
            "shadow_high_cs_rate": (d["shadow_high_strikes"] / d["shadow_high_total"]
                                    if d["shadow_high_total"] > 0 else None),
            "shadow_low_cs_rate": (d["shadow_low_strikes"] / d["shadow_low_total"]
                                   if d["shadow_low_total"] > 0 else None),
            "avg_total_runs": d["total_runs"] / d["games"] if d["games"] > 0 else None,
            "run_env_factor": (d["total_runs"] / d["games"]) / lg_avg_runs
                              if d["games"] > 0 and lg_avg_runs > 0 else None,
            "accuracy": d["correct"] / total if total > 0 else None,
            "abs_challenges": d["abs_challenges"],
            "abs_overturned": d["abs_overturned"],
        })

    result = pd.DataFrame(rows)
    result = result.sort_values(["season", "hp_umpire_id"]).reset_index(drop=True)

    os.makedirs(DATA_DIR, exist_ok=True)
    result.to_parquet(OUTPUT_PATH, index=False)

    print(f"\nUmpire metrics saved to {OUTPUT_PATH}")
    print(f"  {len(result):,} umpire-seasons ({result['hp_umpire_id'].nunique():,} umpires)")
    print(f"  Total games processed: {all_games}")

    # Top umpires by games
    top = result.sort_values("games", ascending=False).head(10)
    print(f"\nTop 10 by games:")
    for _, r in top.iterrows():
        name = None
        for k, d in umpire_data.items():
            if k[0] == r["hp_umpire_id"]:
                name = d["name"]
                break
        acc = f"{r['accuracy']:.1%}" if pd.notna(r['accuracy']) else "—"
        ooz = f"{r['ooz_cs_rate']:.3f}" if pd.notna(r['ooz_cs_rate']) else "—"
        zsf = f"{r['zone_size_factor']:.2f}" if pd.notna(r['zone_size_factor']) else "—"
        print(f"  {str(name or int(r['hp_umpire_id'])):20s}  G={int(r['games']):2d}  "
              f"Acc={acc}  OOZ={ooz}  ZoneSz={zsf}")

    # Update game_outcomes with umpire names
    if game_outcomes_rows:
        new_go = pd.DataFrame(game_outcomes_rows)
        if os.path.exists(GAME_OUTCOMES_PATH):
            existing = pd.read_parquet(GAME_OUTCOMES_PATH)
            # Add columns if missing
            for col in ["hp_umpire_name"]:
                if col not in existing.columns:
                    existing[col] = None
            # Remove old rows for seasons we're rebuilding
            rebuilt_seasons = {s for s, _ in season_configs}
            existing = existing[~existing["season"].isin(rebuilt_seasons)]
            combined = pd.concat([existing, new_go], ignore_index=True)
        else:
            combined = new_go
        combined = combined.drop_duplicates("game_pk").sort_values(
            ["game_date", "game_pk"]).reset_index(drop=True)
        # Ensure columns exist
        for col in ["home_team_id", "away_team_id", "venue_id", "venue_name",
                     "day_night", "home_probable_pitcher", "away_probable_pitcher",
                     "home_win", "run_diff", "innings", "home_lineup", "away_lineup"]:
            if col not in combined.columns:
                combined[col] = None
        combined.to_parquet(GAME_OUTCOMES_PATH, index=False)
        print(f"\ngame_outcomes.parquet updated: {len(combined)} total games")


if __name__ == "__main__":
    build()
