#!/usr/bin/env python3
"""
Build historical game outcomes dataset from MLB StatsAPI.

For each game (2021-2025), collects:
- Final score, winner, home/away teams
- Starting pitchers
- Starting lineups (batting order)
- Venue, day/night, game type
- Umpire (home plate)

This is the training target for the game prediction model.

Output: data/game_outcomes.parquet
"""

import os
import sys
import time
import json
import requests
import pandas as pd
import numpy as np
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(__file__))

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "game_outcomes.parquet")

API_BASE = "https://statsapi.mlb.com/api/v1"


def get_schedule(season):
    """Get all regular season games for a season."""
    url = f"{API_BASE}/schedule"
    params = {
        "sportId": 1,
        "season": season,
        "gameType": "R",
        "startDate": f"{season}-03-01",
        "endDate": f"{season}-11-30",
        "hydrate": "probablePitcher,linescore,venue",
    }
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def get_game_boxscore(game_pk):
    """Get boxscore for a game (includes lineups and umpires)."""
    url = f"{API_BASE}/game/{game_pk}/boxscore"
    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        return None
    return resp.json()


def extract_lineup(boxscore, side):
    """Extract batting order from boxscore."""
    try:
        team_data = boxscore["teams"][side]
        batting_order = team_data.get("battingOrder", [])
        return batting_order[:9]  # first 9 batters
    except (KeyError, TypeError):
        return []


def extract_umpire_hp(boxscore):
    """Extract home plate umpire ID and name from boxscore."""
    try:
        officials = boxscore.get("officials", [])
        for official in officials:
            if official.get("officialType") == "Home Plate":
                ump = official.get("official", {})
                return ump.get("id"), ump.get("fullName")
    except (KeyError, TypeError):
        pass
    return None, None


def build_game_outcomes():
    print("Building game outcomes dataset from MLB StatsAPI...")

    all_games = []
    seasons = range(2021, 2026)

    for season in seasons:
        print(f"\n--- Season {season} ---")
        print("  Fetching schedule...")
        schedule = get_schedule(season)

        games_in_season = []
        for game_date in schedule.get("dates", []):
            for game in game_date.get("games", []):
                status = game.get("status", {}).get("abstractGameState", "")
                if status != "Final":
                    continue

                game_pk = game["gamePk"]
                game_info = {
                    "game_pk": game_pk,
                    "game_date": game_date["date"],
                    "season": season,
                    "home_team_id": game["teams"]["home"]["team"]["id"],
                    "away_team_id": game["teams"]["away"]["team"]["id"],
                    "home_team": game["teams"]["home"]["team"].get("abbreviation",
                                  game["teams"]["home"]["team"].get("name", "")),
                    "away_team": game["teams"]["away"]["team"].get("abbreviation",
                                  game["teams"]["away"]["team"].get("name", "")),
                    "home_score": game["teams"]["home"].get("score", 0),
                    "away_score": game["teams"]["away"].get("score", 0),
                    "venue_id": game.get("venue", {}).get("id"),
                    "venue_name": game.get("venue", {}).get("name", ""),
                    "day_night": game.get("dayNight", ""),
                    "home_probable_pitcher": None,
                    "away_probable_pitcher": None,
                }

                # Probable pitchers
                home_pp = game["teams"]["home"].get("probablePitcher", {})
                away_pp = game["teams"]["away"].get("probablePitcher", {})
                game_info["home_probable_pitcher"] = home_pp.get("id")
                game_info["away_probable_pitcher"] = away_pp.get("id")

                # Winner
                game_info["home_win"] = int(game_info["home_score"] > game_info["away_score"])
                game_info["total_runs"] = game_info["home_score"] + game_info["away_score"]
                game_info["run_diff"] = game_info["home_score"] - game_info["away_score"]

                # Linescore innings
                linescore = game.get("linescore", {})
                game_info["innings"] = linescore.get("currentInning", 9)

                games_in_season.append(game_info)

        print(f"  {len(games_in_season)} final games found")

        # Fetch boxscores for lineups and umpires (in batches)
        print(f"  Fetching boxscores for lineups/umpires...")
        batch_size = 50
        for i in range(0, len(games_in_season), batch_size):
            batch = games_in_season[i:i + batch_size]
            for game_info in batch:
                try:
                    boxscore = get_game_boxscore(game_info["game_pk"])
                    if boxscore:
                        game_info["home_lineup"] = extract_lineup(boxscore, "home")
                        game_info["away_lineup"] = extract_lineup(boxscore, "away")
                        ump_id, ump_name = extract_umpire_hp(boxscore)
                        game_info["hp_umpire_id"] = ump_id
                        game_info["hp_umpire_name"] = ump_name
                    else:
                        game_info["home_lineup"] = []
                        game_info["away_lineup"] = []
                        game_info["hp_umpire_id"] = None
                        game_info["hp_umpire_name"] = None
                except Exception as e:
                    game_info["home_lineup"] = []
                    game_info["away_lineup"] = []
                    game_info["hp_umpire_id"] = None
                    game_info["hp_umpire_name"] = None

                time.sleep(0.1)  # rate limiting

            done = min(i + batch_size, len(games_in_season))
            print(f"    {done}/{len(games_in_season)} boxscores fetched")

        all_games.extend(games_in_season)

    print(f"\nTotal games: {len(all_games)}")

    # Convert to DataFrame
    df = pd.DataFrame(all_games)

    # Convert lineups to JSON strings for parquet storage
    df["home_lineup"] = df["home_lineup"].apply(json.dumps)
    df["away_lineup"] = df["away_lineup"].apply(json.dumps)

    df = df.sort_values(["game_date", "game_pk"]).reset_index(drop=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nGame outcomes saved to {OUTPUT_PATH}")
    print(f"  {len(df):,} games across {df['season'].nunique()} seasons")
    print(f"  Home win rate: {df['home_win'].mean():.3f}")
    print(f"  Avg total runs: {df['total_runs'].mean():.1f}")


if __name__ == "__main__":
    build_game_outcomes()
