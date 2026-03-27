# services/weather.py
"""Fetch game-day weather and calculate impact on batted balls."""

from __future__ import annotations
import json
import math
import logging
import os
import urllib.request
from datetime import datetime, timezone

from services.venue_meta import get_venue_meta

_ROOF_OVERRIDES_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "roof_overrides.json")


def load_roof_overrides() -> dict:
    """Load roof override file; returns {str(game_pk): bool}."""
    try:
        with open(_ROOF_OVERRIDES_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_roof_overrides(overrides: dict) -> None:
    with open(_ROOF_OVERRIDES_PATH, "w") as f:
        json.dump(overrides, f, indent=2)

log = logging.getLogger(__name__)

# Module-level cache: (venue_id, date_str, hour) → result dict
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

# Spray angle samples and weights for aggregating wind effect across a
# realistic batted-ball direction distribution.
# Spray angle convention: 0=CF, negative=pull, positive=oppo (Statcast).
# Home runs are pull-heavy; XBH are more evenly spread.
_HR_SPRAY_DIST = [(-38, 0.20), (-22, 0.28), (-8, 0.22), (8, 0.15), (22, 0.10), (38, 0.05)]
_XBH_SPRAY_DIST = [(-38, 0.15), (-22, 0.25), (-8, 0.25), (8, 0.18), (22, 0.12), (38, 0.05)]

# League-average fence distances used as the neutral baseline for dimension factors
_LF_AVG_DIST = 331
_CF_AVG_DIST = 404
_RF_AVG_DIST = 328

# Representative spray angles for each field zone
_LF_SPRAY = -30   # pulled for RHBs, oppo for LHBs
_CF_SPRAY =   0
_RF_SPRAY = +30   # pulled for LHBs, oppo for RHBs


# ---------------------------------------------------------------------------
# Physics helpers
# ---------------------------------------------------------------------------

def _air_density_factor(temp_f: float, alt_ft: float) -> float:
    """
    Relative air density vs the reference condition (72°F, sea level = 1.0).

    Uses ideal-gas temperature scaling and the ISA barometric formula for
    pressure vs altitude. Less dense air = less aerodynamic drag = more carry.
    """
    T_K_ref = 295.37                        # 72°F in Kelvin
    T_K = (temp_f - 32.0) * 5.0 / 9.0 + 273.15
    alt_m = alt_ft * 0.3048
    # ISA troposphere pressure ratio
    pressure_factor = (1.0 - 2.2558e-5 * alt_m) ** 5.2559
    # Density ∝ P / T
    return (T_K_ref / T_K) * pressure_factor


def _dimension_hr_factor(park_dist: float, wall_height: float, avg_dist: float) -> float:
    """
    HR factor from park dimensions vs league average at that field zone.

    Shorter fence → more HRs (distance effect, alpha=1.5 calibrated to ~5%/10 ft).
    Taller wall   → fewer HRs (height effect, calibrated to ~10% per 7 ft above 8 ft).

    Examples vs league-average fence (no wind, sea level, 72°F):
      Fenway LF  (310 ft, 37 ft wall): 1.10 × 0.69 = 0.76  ← Green Monster suppresses
      Fenway RF  (302 ft,  3 ft wall): 1.13 × 1.07 = 1.21  ← Pesky Pole boosts
      Oracle RF  (309 ft, 24 ft wall): 1.09 × 0.81 = 0.88  ← brick wall suppresses
      Coors LF   (347 ft,  8 ft wall): 0.93 × 1.00 = 0.93  ← deep, altitude offsets it
    """
    dist_factor   = (avg_dist / park_dist) ** 1.5
    height_factor = math.exp(-0.013 * max(0.0, wall_height - 8.0))
    return dist_factor * height_factor


def compute_hit_distance_factor(
    spray_angle_deg: float,
    temp_f: float,
    wind_mph: float,
    wind_dir_deg: float,
    alt_ft: float,
    bearing_to_cf: float = 180,
) -> float:
    """
    Multiplicative distance factor for a single batted ball vs neutral conditions
    (72°F, no wind, sea level ≈ 500 ft). Factor > 1 means the ball carries farther.

    spray_angle_deg:
        Statcast convention — 0 = straight to CF, negative = pull side,
        positive = opposite field. Added to the CF compass bearing to get the
        actual direction the ball is travelling.

    The two physics effects modelled:
      1. Air density  — combined temperature + altitude via the barometric formula.
                        Calibrated so Coors Field at 72°F ≈ +8-9% distance.
      2. Wind carry   — wind component *along the ball's travel direction*,
                        not just toward CF. This correctly accounts for cross-winds
                        that help a pulled HR while hurting an oppo-field one (or
                        vice versa).

    Coefficient reference:
      - Air density: 0.50 * (1 - rho) gives ~8.8% at Coors (5280 ft, 72°F).
      - Wind: 0.0079 per mph ≈ 3 ft/mph for a 380-ft average HR (Nathan 2015).
    """
    rho = _air_density_factor(temp_f, alt_ft)
    density_factor = 1.0 + 0.50 * (1.0 - rho)

    # Compass direction the ball is travelling
    ball_compass = (bearing_to_cf + spray_angle_deg) % 360
    # Meteorological wind direction is where it comes FROM; flip to get where it's going
    wind_toward_deg = (wind_dir_deg + 180.0) % 360
    angle_diff_rad = math.radians(wind_toward_deg - ball_compass)
    wind_outward = wind_mph * math.cos(angle_diff_rad)   # positive = helping carry

    wind_factor = 1.0 + 0.0079 * wind_outward

    return density_factor * wind_factor


# ---------------------------------------------------------------------------
# Game-level aggregate impact
# ---------------------------------------------------------------------------

def calculate_weather_impact(
    temp_f: float,
    wind_mph: float,
    wind_dir_deg: float,
    alt_ft: float,
    bearing_to_cf: float = 180,
    lf_dist: float = 331,
    cf_dist: float = 404,
    rf_dist: float = 328,
    lf_wall: float = 8,
    cf_wall: float = 8,
    rf_wall: float = 8,
) -> dict:
    """
    Aggregate weather impact on home runs and extra-base hits for a game.

    Rather than applying a single wind component toward CF for all batted balls,
    this function weights the distance factor across a realistic spray-angle
    distribution (pull-heavy for HRs, more balanced for XBH). That means a
    30-mph left-to-right cross-wind will correctly help left-field pull HRs
    while slightly hurting right-field opposite-field HRs, with the net game
    impact reflecting the typical batted-ball mix.

    Park dimensions (lf_dist/cf_dist/rf_dist in feet, *_wall in feet) are used
    to compute per-field-zone factors combining both weather and structural effects.

    Returns:
        hr_factor    — aggregate multiplicative factor for HR probability (1.0 = neutral)
        xbh_factor   — aggregate multiplicative factor for XBH probability
        label        — human-readable environment description
        lf_hr_factor — HR factor for balls to left field  (wind direction × park dims)
        cf_hr_factor — HR factor for balls to center field
        rf_hr_factor — HR factor for balls to right field
        components   — breakdown for UI display:
            density_factor   — air-density-only multiplier (temp + altitude)
            cf_wind_factor   — wind-only multiplier toward CF (display reference)
            cf_wind_mph      — signed mph component blowing out to CF (+= helping)
    """
    hr_factor = sum(
        w * compute_hit_distance_factor(s, temp_f, wind_mph, wind_dir_deg, alt_ft, bearing_to_cf)
        for s, w in _HR_SPRAY_DIST
    )
    xbh_factor = sum(
        w * compute_hit_distance_factor(s, temp_f, wind_mph, wind_dir_deg, alt_ft, bearing_to_cf)
        for s, w in _XBH_SPRAY_DIST
    )

    # XBH are less air-dependent (line drives, grounders don't benefit as much)
    # Scale deviation from neutral by 0.45 so the XBH curve is shallower than HR
    xbh_factor = 1.0 + (xbh_factor - 1.0) * 0.45

    hr_factor  = round(max(0.82, min(1.22, hr_factor)), 3)
    xbh_factor = round(max(0.88, min(1.12, xbh_factor)), 3)

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

    # --- component breakdown for display ---
    rho = _air_density_factor(temp_f, alt_ft)
    density_factor = round(1.0 + 0.50 * (1.0 - rho), 3)

    wind_toward_deg = (wind_dir_deg + 180.0) % 360
    cf_diff_rad = math.radians(wind_toward_deg - bearing_to_cf)
    cf_wind_mph = round(wind_mph * math.cos(cf_diff_rad), 1)
    cf_wind_factor = round(1.0 + 0.0079 * cf_wind_mph, 3)

    # --- directional HR factors: weather (by spray angle) × park dimensions ---
    # Each field zone uses the representative spray angle for wind decomposition,
    # combined with the park's fence distance and wall height vs league average.
    def _dir_factor(spray, park_dist, avg_dist, wall_h):
        wx = compute_hit_distance_factor(spray, temp_f, wind_mph, wind_dir_deg, alt_ft, bearing_to_cf)
        dim = _dimension_hr_factor(park_dist, wall_h, avg_dist)
        return round(max(0.65, min(1.45, wx * dim)), 3)

    lf_hr_factor = _dir_factor(_LF_SPRAY, lf_dist, _LF_AVG_DIST, lf_wall)
    cf_hr_factor = _dir_factor(_CF_SPRAY, cf_dist, _CF_AVG_DIST, cf_wall)
    rf_hr_factor = _dir_factor(_RF_SPRAY, rf_dist, _RF_AVG_DIST, rf_wall)

    return {
        "hr_factor":    hr_factor,
        "xbh_factor":   xbh_factor,
        "label":        label,
        "lf_hr_factor": lf_hr_factor,
        "cf_hr_factor": cf_hr_factor,
        "rf_hr_factor": rf_hr_factor,
        "components": {
            "density_factor":  density_factor,
            "cf_wind_factor":  cf_wind_factor,
            "cf_wind_mph":     cf_wind_mph,
        },
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_game_weather(venue_id, game_datetime_str, game_pk=None):
    """
    Fetch weather for a game.

    venue_id:           MLB venue ID (int)
    game_datetime_str:  ISO datetime string from MLB feed (e.g. "2026-03-25T23:05:00Z")
    game_pk:            MLB game pk (int or str) — used to check roof overrides

    Returns dict with: dome, retractable, roof_open, condition, temp_f, wind_mph, wind_dir, bearing, impact {}
    """
    venue = get_venue_meta(venue_id)
    is_retractable = venue.get("retractable", False)

    # Check roof override for retractable stadiums
    roof_open = None
    if is_retractable and game_pk is not None:
        overrides = load_roof_overrides()
        key = str(game_pk)
        if key in overrides:
            roof_open = overrides[key]  # True = open, False = closed

    # Treat as dome if: fixed dome, OR retractable with no override/override=closed
    treat_as_dome = venue["dome"] and not (is_retractable and roof_open is True)

    if treat_as_dome:
        # No wind/weather, but park dimensions still create directional HR differences
        dome_impact = calculate_weather_impact(
            72, 0, 0,
            venue["alt_ft"], venue.get("bearing", 180),
            lf_dist=venue.get("lf_dist", 331),
            cf_dist=venue.get("cf_dist", 404),
            rf_dist=venue.get("rf_dist", 328),
            lf_wall=venue.get("lf_wall", 8),
            cf_wall=venue.get("cf_wall", 8),
            rf_wall=venue.get("rf_wall", 8),
        )
        dome_impact["hr_factor"]  = 1.0
        dome_impact["xbh_factor"] = 1.0
        dome_impact["label"]      = "Retractable (closed)" if is_retractable else "Dome (controlled)"
        return {
            "dome":        True,
            "retractable": is_retractable,
            "roof_open":   False if is_retractable else None,
            "condition":   "Dome",
            "temp_f":      72,
            "wind_mph":    0,
            "wind_dir":    0,
            "bearing":     venue.get("bearing", 180),
            "impact":      dome_impact,
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
        times     = hourly.get("time", [])
        temps     = hourly.get("temperature_2m", [])
        winds     = hourly.get("wind_speed_10m", [])
        wind_dirs = hourly.get("wind_direction_10m", [])
        codes     = hourly.get("weather_code", [])

        # Find closest hour to game time
        utc_offset_sec = data.get("utc_offset_seconds", 0)
        local_hour = (hour + utc_offset_sec // 3600) % 24
        idx = min(local_hour, len(temps) - 1) if temps else 0

        temp_f   = temps[idx]     if idx < len(temps)     else 72
        wind_mph = winds[idx]     if idx < len(winds)     else 0
        wind_dir = wind_dirs[idx] if idx < len(wind_dirs) else 0
        wmo_code = codes[idx]     if idx < len(codes)     else 0

        condition = _WMO.get(wmo_code, "Unknown")
        impact = calculate_weather_impact(
            temp_f, wind_mph, wind_dir,
            venue["alt_ft"], venue.get("bearing", 180),
            lf_dist=venue.get("lf_dist", 331),
            cf_dist=venue.get("cf_dist", 404),
            rf_dist=venue.get("rf_dist", 328),
            lf_wall=venue.get("lf_wall", 8),
            cf_wall=venue.get("cf_wall", 8),
            rf_wall=venue.get("rf_wall", 8),
        )

        result = {
            "dome":        False,
            "retractable": is_retractable,
            "roof_open":   True if is_retractable else None,
            "condition":   condition,
            "temp_f":      round(temp_f),
            "wind_mph":    round(wind_mph),
            "wind_dir":    round(wind_dir),
            "bearing":     venue.get("bearing", 180),
            "impact":      impact,
        }
        _cache[cache_key] = result
        return result

    except Exception as e:
        log.warning("Weather fetch failed for venue %s: %s", venue_id, e)
        return _fallback(venue)


def _fallback(venue):
    """Return a neutral weather result when API fails."""
    impact = calculate_weather_impact(
        72, 0, 0,
        venue.get("alt_ft", 500), venue.get("bearing", 180),
        lf_dist=venue.get("lf_dist", 331),
        cf_dist=venue.get("cf_dist", 404),
        rf_dist=venue.get("rf_dist", 328),
        lf_wall=venue.get("lf_wall", 8),
        cf_wall=venue.get("cf_wall", 8),
        rf_wall=venue.get("rf_wall", 8),
    )
    return {
        "dome":        False,
        "retractable": venue.get("retractable", False),
        "roof_open":   None,
        "condition":   "Unknown",
        "temp_f":      72,
        "wind_mph":    0,
        "wind_dir":    0,
        "bearing":     venue.get("bearing", 180),
        "impact":      impact,
    }
