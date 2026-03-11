# services/weather.py
"""Fetch game-day weather and calculate impact on batted balls."""

from __future__ import annotations
import json
import math
import logging
import urllib.request
from datetime import datetime, timezone

from services.venue_meta import get_venue_meta

log = logging.getLogger(__name__)

# Module-level cache: (venue_id, date_str) → result dict
_cache: dict = {}

# WMO weather code → human condition label
_WMO = {
    0: "Clear", 1: "Mostly Clear", 2: "Partly Cloudy", 3: "Overcast",
    45: "Fog", 48: "Fog", 51: "Drizzle", 53: "Drizzle", 55: "Drizzle",
    56: "Freezing Drizzle", 57: "Freezing Drizzle",
    61: "Light Rain", 63: "Rain", 65: "Heavy Rain",
    66: "Freezing Rain", 67: "Freezing Rain",
    71: "Light Snow", 73: "Snow", 75: "Heavy Snow",
    77: "Snow Grains",
    80: "Light Showers", 81: "Showers", 82: "Heavy Showers",
    85: "Snow Showers", 86: "Snow Showers",
    95: "Thunderstorm", 96: "Thunderstorm", 99: "Thunderstorm",
}


def fetch_game_weather(venue_id, game_datetime_str):
    """
    Fetch weather for a game.

    venue_id:           MLB venue ID (int)
    game_datetime_str:  ISO datetime string from MLB feed (e.g. "2026-03-25T23:05:00Z")

    Returns dict with: dome, condition, temp_f, wind_mph, wind_dir, impact {}
    """
    venue = get_venue_meta(venue_id)
    if venue["dome"]:
        return {
            "dome": True,
            "condition": "Dome",
            "temp_f": 72,
            "wind_mph": 0,
            "wind_dir": 0,
            "impact": {"hr_factor": 1.0, "xbh_factor": 1.0, "label": "Dome (controlled)"},
        }

    # Parse game datetime
    try:
        if game_datetime_str and "T" in str(game_datetime_str):
            dt = datetime.fromisoformat(str(game_datetime_str).replace("Z", "+00:00"))
        else:
            return _fallback(venue)
    except Exception:
        return _fallback(venue)

    date_str = dt.strftime("%Y-%m-%d")
    hour = dt.hour  # UTC hour

    cache_key = (venue_id, date_str, hour)
    if cache_key in _cache:
        return _cache[cache_key]

    # Call Open-Meteo API
    try:
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={venue['lat']}&longitude={venue['lon']}"
            f"&hourly=temperature_2m,wind_speed_10m,wind_direction_10m,weather_code"
            f"&temperature_unit=fahrenheit&wind_speed_unit=mph"
            f"&timezone=auto&start_date={date_str}&end_date={date_str}"
        )
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read())

        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        winds = hourly.get("wind_speed_10m", [])
        wind_dirs = hourly.get("wind_direction_10m", [])
        codes = hourly.get("weather_code", [])

        # Find closest hour to game time
        # Open-Meteo returns local times; convert game UTC hour to local offset
        utc_offset_sec = data.get("utc_offset_seconds", 0)
        local_hour = (hour + utc_offset_sec // 3600) % 24

        idx = min(local_hour, len(temps) - 1) if temps else 0

        temp_f = temps[idx] if idx < len(temps) else 72
        wind_mph = winds[idx] if idx < len(winds) else 0
        wind_dir = wind_dirs[idx] if idx < len(wind_dirs) else 0
        wmo_code = codes[idx] if idx < len(codes) else 0

        condition = _WMO.get(wmo_code, "Unknown")
        impact = calculate_weather_impact(temp_f, wind_mph, wind_dir, venue["alt_ft"], venue.get("bearing", 180))

        result = {
            "dome": False,
            "condition": condition,
            "temp_f": round(temp_f),
            "wind_mph": round(wind_mph),
            "wind_dir": round(wind_dir),
            "impact": impact,
        }
        _cache[cache_key] = result
        return result

    except Exception as e:
        log.warning("Weather fetch failed for venue %s: %s", venue_id, e)
        return _fallback(venue)


def calculate_weather_impact(temp_f, wind_mph, wind_dir_deg, alt_ft, bearing_to_cf=180):
    """
    Calculate multiplicative HR and XBH factors from weather.

    Based on published research:
    - Temperature: ~1.5% more HR per degree F above 72 (Alan Nathan, Baseball Prospectus)
    - Wind: blowing out to CF increases carry; blowing in decreases it
    - Altitude: thinner air = less drag = more carry (~5-8% at Coors)

    Returns: {"hr_factor": float, "xbh_factor": float, "label": str}
    """
    # Temperature factor: baseline 72°F
    temp_factor = 1.0 + 0.0015 * (temp_f - 72)
    temp_factor = max(0.90, min(1.15, temp_factor))

    # Wind factor: decompose wind into component blowing toward CF
    # wind_dir_deg = meteorological direction (where wind comes FROM)
    # Wind blowing FROM behind batter (toward CF) helps carry
    # bearing_to_cf = compass direction from home plate to CF
    wind_from = wind_dir_deg  # degrees, where wind comes from
    wind_toward = (wind_from + 180) % 360  # where wind is going
    angle_diff = math.radians(wind_toward - bearing_to_cf)
    outward_component = wind_mph * math.cos(angle_diff)  # positive = blowing out

    wind_factor = 1.0 + 0.008 * outward_component  # ~0.8% per mph out
    wind_factor = max(0.85, min(1.20, wind_factor))

    # Altitude factor: sea level baseline ~500ft
    alt_factor = 1.0 + 0.00008 * (alt_ft - 500)
    alt_factor = max(1.0, min(1.12, alt_factor))

    hr_factor = round(temp_factor * wind_factor * alt_factor, 3)
    # XBH is less affected by these factors (ground balls don't care about air)
    xbh_factor = round(1.0 + (hr_factor - 1.0) * 0.5, 3)

    # Label
    if hr_factor >= 1.08:
        label = "Very hitter-friendly"
    elif hr_factor >= 1.03:
        label = "Hitter-friendly"
    elif hr_factor <= 0.92:
        label = "Very pitcher-friendly"
    elif hr_factor <= 0.97:
        label = "Pitcher-friendly"
    else:
        label = "Neutral"

    return {
        "hr_factor": hr_factor,
        "xbh_factor": xbh_factor,
        "label": label,
    }


def _fallback(venue):
    """Return a neutral weather result when API fails."""
    impact = calculate_weather_impact(72, 0, 0, venue.get("alt_ft", 500), venue.get("bearing", 180))
    return {
        "dome": False,
        "condition": "Unknown",
        "temp_f": 72,
        "wind_mph": 0,
        "wind_dir": 0,
        "impact": impact,
    }
