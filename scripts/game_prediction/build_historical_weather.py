#!/usr/bin/env python3
"""
Build historical weather features for all games 2021-present.

Uses the Open-Meteo Archive API to fetch historical weather for each game,
then computes the same hr_factor and density_factor used in the live
weather service (services/weather.py).

Features per game:
  - weather_temp_f: temperature at game time
  - weather_wind_mph: wind speed
  - weather_wind_dir: wind direction (degrees)
  - weather_hr_factor: physics-based HR impact (0.82-1.22)
  - weather_xbh_factor: XBH impact (0.88-1.12)
  - weather_density_factor: air density only (temp + altitude)
  - weather_is_dome: 1 if dome/retractable closed

Output: data/game_weather.parquet
  Keyed on game_pk
"""

import os
import sys
import json
import math
import time
import urllib.request
import pandas as pd
import numpy as np
from datetime import datetime, date

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import query_df

# Import venue metadata and weather physics from existing services
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from services.venue_meta import VENUES, get_venue_meta
from services.weather import calculate_weather_impact, _air_density_factor

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "game_weather.parquet")

# Map team abbreviation to venue_id
# We need this because statcast has home_team as abbreviation
TEAM_TO_VENUE = {
    "BAL": 2, "BOS": 3, "NYM": 3289, "NYY": 3313, "TB": 12, "TOR": 14,
    "CWS": 4, "CLE": 5, "DET": 17, "KC": 7, "MIN": 3312,
    "HOU": 2392, "LAA": 1, "OAK": 10, "SEA": 680, "TEX": 13,
    "ATL": 4705, "MIA": 4169, "PHI": 2681, "WSH": 3309,
    "CHC": 17, "CIN": 2602, "MIL": 32, "PIT": 31, "STL": 2889,
    "ARI": 15, "COL": 19, "LAD": 22, "SD": 2680, "SF": 2395,
}

# Some venues need manual mapping since team abbreviation → venue_id isn't always clean
# Using the VENUES dict which is keyed by venue_id
def _get_venue_for_team(team_abbrev):
    """Get venue_id for a team abbreviation."""
    mapping = {
        "BAL": 2, "BOS": 3, "NYM": 3289, "NYY": 3313, "TB": 12, "TOR": 14,
        "CWS": 4, "CHW": 4, "CLE": 5, "DET": 2394, "KC": 7, "MIN": 3312,
        "HOU": 2392, "LAA": 1, "OAK": 2529, "SEA": 680, "TEX": 5325,
        "ATL": 4705, "MIA": 4169, "PHI": 2681, "WSH": 3309,
        "CHC": 17, "CIN": 2602, "MIL": 32, "PIT": 31, "STL": 2889,
        "ARI": 15, "COL": 19, "LAD": 22, "SD": 2680, "SF": 2395,
    }
    return mapping.get(team_abbrev)


def _fetch_historical_weather(lat, lon, date_str):
    """
    Fetch historical weather from Open-Meteo Archive API.
    Returns (temp_f, wind_mph, wind_dir) or None.
    """
    try:
        url = (
            f"https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={lat}&longitude={lon}"
            f"&hourly=temperature_2m,wind_speed_10m,wind_direction_10m"
            f"&temperature_unit=fahrenheit&wind_speed_unit=mph"
            f"&timezone=auto&start_date={date_str}&end_date={date_str}"
        )
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())

        hourly = data.get("hourly", {})
        temps = hourly.get("temperature_2m", [])
        winds = hourly.get("wind_speed_10m", [])
        wind_dirs = hourly.get("wind_direction_10m", [])

        if not temps:
            return None

        # Use ~7pm local time (typical game start) — index 19
        # Most games are between 18:00-20:00 local
        idx = min(19, len(temps) - 1)

        temp_f = temps[idx] if temps[idx] is not None else 72
        wind_mph = winds[idx] if idx < len(winds) and winds[idx] is not None else 0
        wind_dir = wind_dirs[idx] if idx < len(wind_dirs) and wind_dirs[idx] is not None else 0

        return (temp_f, wind_mph, wind_dir)

    except Exception:
        return None


def build_historical_weather():
    current_year = date.today().year

    print("Querying unique games for weather data...")

    # Use GROUP BY instead of DISTINCT — much faster on large tables
    sql = f"""
    SELECT
        game_pk,
        MIN(game_date) AS game_date,
        MIN(home_team) AS home_team
    FROM statcast_pitches
    WHERE game_type = 'R'
      AND game_year BETWEEN 2021 AND {current_year}
    GROUP BY game_pk
    ORDER BY MIN(game_date)
    """
    games = query_df(sql)
    print(f"  {len(games):,} unique games")

    # Check for existing data to resume
    existing = pd.DataFrame()
    if os.path.exists(OUTPUT_PATH):
        existing = pd.read_parquet(OUTPUT_PATH)
        existing_pks = set(existing["game_pk"].values)
        print(f"  {len(existing):,} games already have weather data")
        games = games[~games["game_pk"].isin(existing_pks)]
        print(f"  {len(games):,} games need weather data")

    if games.empty:
        print("All games already have weather data.")
        return

    games["game_date"] = pd.to_datetime(games["game_date"])

    # Group by date and venue for efficient API batching
    # Open-Meteo rate limit: ~600 requests/minute for archive API
    records = []
    failed = 0
    dome_count = 0

    # Process in date batches (one API call per venue per date)
    date_venue_cache = {}  # (lat, lon, date_str) → (temp, wind, wind_dir)

    for i, row in games.iterrows():
        game_pk = row["game_pk"]
        game_date = row["game_date"]
        home_team = row["home_team"]
        date_str = game_date.strftime("%Y-%m-%d")

        venue_id = _get_venue_for_team(home_team)
        if venue_id is None or venue_id not in VENUES:
            # Unknown venue — use neutral weather
            records.append({
                "game_pk": game_pk,
                "weather_temp_f": 72,
                "weather_wind_mph": 0,
                "weather_wind_dir": 0,
                "weather_hr_factor": 1.0,
                "weather_xbh_factor": 1.0,
                "weather_density_factor": 1.0,
                "weather_is_dome": 0,
            })
            continue

        venue = VENUES[venue_id]
        is_dome = venue.get("dome", False) and not venue.get("retractable", False)

        if is_dome:
            # Fixed dome — no weather impact
            dome_count += 1
            records.append({
                "game_pk": game_pk,
                "weather_temp_f": 72,
                "weather_wind_mph": 0,
                "weather_wind_dir": 0,
                "weather_hr_factor": 1.0,
                "weather_xbh_factor": 1.0,
                "weather_density_factor": float(round(_air_density_factor(72, venue.get("alt_ft", 0)), 3)),
                "weather_is_dome": 1,
            })
            continue

        # Check cache
        cache_key = (venue["lat"], venue["lon"], date_str)
        if cache_key in date_venue_cache:
            weather = date_venue_cache[cache_key]
        else:
            weather = _fetch_historical_weather(venue["lat"], venue["lon"], date_str)
            date_venue_cache[cache_key] = weather

            # Rate limiting — be nice to the free API
            if len(date_venue_cache) % 50 == 0:
                time.sleep(1)

        if weather is None:
            failed += 1
            # Neutral fallback
            records.append({
                "game_pk": game_pk,
                "weather_temp_f": 72,
                "weather_wind_mph": 0,
                "weather_wind_dir": 0,
                "weather_hr_factor": 1.0,
                "weather_xbh_factor": 1.0,
                "weather_density_factor": 1.0,
                "weather_is_dome": 0,
            })
            continue

        temp_f, wind_mph, wind_dir = weather

        # Compute impact using the same physics as live weather
        impact = calculate_weather_impact(
            temp_f, wind_mph, wind_dir,
            venue.get("alt_ft", 0),
            venue.get("bearing", 180),
            lf_dist=venue.get("lf_dist", 331),
            lcf_dist=venue.get("lcf_dist", 371),
            cf_dist=venue.get("cf_dist", 404),
            rcf_dist=venue.get("rcf_dist", 373),
            rf_dist=venue.get("rf_dist", 328),
            lf_wall=venue.get("lf_wall", 8),
            lcf_wall=venue.get("lcf_wall", 8),
            cf_wall=venue.get("cf_wall", 8),
            rcf_wall=venue.get("rcf_wall", 8),
            rf_wall=venue.get("rf_wall", 8),
        )

        records.append({
            "game_pk": game_pk,
            "weather_temp_f": round(temp_f),
            "weather_wind_mph": round(wind_mph),
            "weather_wind_dir": round(wind_dir),
            "weather_hr_factor": impact["hr_factor"],
            "weather_xbh_factor": impact["xbh_factor"],
            "weather_density_factor": impact["components"]["density_factor"],
            "weather_is_dome": 0,
        })

        if len(records) % 500 == 0:
            print(f"  Processed {len(records):,} games...")

    new_df = pd.DataFrame(records)

    # Merge with existing data
    if not existing.empty:
        result = pd.concat([existing, new_df], ignore_index=True)
    else:
        result = new_df

    result = result.drop_duplicates(subset=["game_pk"], keep="last")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    result.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nGame weather saved: {len(result):,} games total")
    print(f"  New: {len(new_df):,}, Dome: {dome_count:,}, API failures: {failed:,}")
    print(f"  API calls made: {len(date_venue_cache):,}")
    print(f"\nWeather stats:")
    print(f"  Temp range: {result['weather_temp_f'].min():.0f}°F - {result['weather_temp_f'].max():.0f}°F")
    print(f"  HR factor range: {result['weather_hr_factor'].min():.3f} - {result['weather_hr_factor'].max():.3f}")
    print(f"  Dome games: {(result['weather_is_dome'] == 1).sum():,}")


if __name__ == "__main__":
    build_historical_weather()
