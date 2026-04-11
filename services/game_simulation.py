"""
Game Simulation Engine — pitch-by-pitch baseball game simulator.

Pre-generates a full 9-inning game using:
- XGBoost matchup model (PA outcome probabilities)
- XGBoost pitch selection model (pitch type prediction per count/context)
- Pitcher arsenal profiles (velocity, spin, movement per pitch type)
- Park factors (stadium-specific HR/hit/2B/3B adjustments)
- Batter/pitcher spray profiles (BIP coordinate generation)

Returns a complete game log that the frontend replays pitch-by-pitch.
"""

import os
import math
import json
import uuid
import logging
import random
from typing import List, Dict, Optional, Tuple, Any

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DATA_DIR = os.path.join(_ROOT, "data")

# ---------------------------------------------------------------------------
# Stadium wall distances (feet) — LF, LCF, CF, RCF, RF, wall height (ft)
# ---------------------------------------------------------------------------
STADIUM_DIMENSIONS = {
    "AZ":  {"lf": 330, "lcf": 374, "cf": 407, "rcf": 374, "rf": 334, "wall": 8},
    "ATL": {"lf": 335, "lcf": 380, "cf": 400, "rcf": 375, "rf": 325, "wall": 8},
    "BAL": {"lf": 333, "lcf": 364, "cf": 400, "rcf": 373, "rf": 318, "wall": 7},
    "BOS": {"lf": 310, "lcf": 379, "cf": 390, "rcf": 380, "rf": 302, "wall": 11},  # Green Monster LF 37ft
    "CHC": {"lf": 355, "lcf": 368, "cf": 400, "rcf": 368, "rf": 353, "wall": 11},
    "CIN": {"lf": 328, "lcf": 379, "cf": 404, "rcf": 370, "rf": 325, "wall": 8},
    "CLE": {"lf": 325, "lcf": 370, "cf": 400, "rcf": 375, "rf": 325, "wall": 9},
    "COL": {"lf": 347, "lcf": 390, "cf": 415, "rcf": 375, "rf": 350, "wall": 8},
    "CWS": {"lf": 330, "lcf": 375, "cf": 400, "rcf": 375, "rf": 335, "wall": 8},
    "DET": {"lf": 345, "lcf": 370, "cf": 420, "rcf": 365, "rf": 330, "wall": 8},
    "HOU": {"lf": 315, "lcf": 366, "cf": 409, "rcf": 373, "rf": 326, "wall": 10},
    "KC":  {"lf": 330, "lcf": 375, "cf": 410, "rcf": 375, "rf": 330, "wall": 9},
    "LAA": {"lf": 330, "lcf": 370, "cf": 396, "rcf": 370, "rf": 330, "wall": 8},
    "LAD": {"lf": 330, "lcf": 375, "cf": 395, "rcf": 375, "rf": 330, "wall": 8},
    "MIA": {"lf": 344, "lcf": 386, "cf": 407, "rcf": 392, "rf": 335, "wall": 8},
    "MIL": {"lf": 344, "lcf": 371, "cf": 400, "rcf": 374, "rf": 345, "wall": 8},
    "MIN": {"lf": 339, "lcf": 377, "cf": 404, "rcf": 367, "rf": 328, "wall": 8},
    "NYM": {"lf": 335, "lcf": 379, "cf": 408, "rcf": 383, "rf": 330, "wall": 8},
    "NYY": {"lf": 318, "lcf": 399, "cf": 408, "rcf": 385, "rf": 314, "wall": 8},
    "ATH": {"lf": 330, "lcf": 375, "cf": 400, "rcf": 375, "rf": 330, "wall": 8},
    "PHI": {"lf": 329, "lcf": 374, "cf": 401, "rcf": 369, "rf": 330, "wall": 8},
    "PIT": {"lf": 325, "lcf": 383, "cf": 399, "rcf": 375, "rf": 320, "wall": 6},
    "SD":  {"lf": 336, "lcf": 375, "cf": 396, "rcf": 382, "rf": 322, "wall": 8},
    "SF":  {"lf": 339, "lcf": 382, "cf": 399, "rcf": 365, "rf": 309, "wall": 8},
    "SEA": {"lf": 331, "lcf": 378, "cf": 401, "rcf": 381, "rf": 326, "wall": 8},
    "STL": {"lf": 336, "lcf": 375, "cf": 400, "rcf": 375, "rf": 335, "wall": 8},
    "TB":  {"lf": 315, "lcf": 370, "cf": 404, "rcf": 370, "rf": 322, "wall": 9},
    "TEX": {"lf": 329, "lcf": 372, "cf": 407, "rcf": 374, "rf": 326, "wall": 8},
    "TOR": {"lf": 328, "lcf": 375, "cf": 400, "rcf": 375, "rf": 328, "wall": 10},
    "WSH": {"lf": 336, "lcf": 377, "cf": 402, "rcf": 370, "rf": 335, "wall": 8},
}

# Stadium display names: "Park Name (Team)"
STADIUM_NAMES = {
    "AZ":  "Chase Field (Diamondbacks)",
    "ATL": "Truist Park (Braves)",
    "BAL": "Camden Yards (Orioles)",
    "BOS": "Fenway Park (Red Sox)",
    "CHC": "Wrigley Field (Cubs)",
    "CIN": "Great American Ball Park (Reds)",
    "CLE": "Progressive Field (Guardians)",
    "COL": "Coors Field (Rockies)",
    "CWS": "Guaranteed Rate Field (White Sox)",
    "DET": "Comerica Park (Tigers)",
    "HOU": "Minute Maid Park (Astros)",
    "KC":  "Kauffman Stadium (Royals)",
    "LAA": "Angel Stadium (Angels)",
    "LAD": "Dodger Stadium (Dodgers)",
    "MIA": "LoanDepot Park (Marlins)",
    "MIL": "American Family Field (Brewers)",
    "MIN": "Target Field (Twins)",
    "NYM": "Citi Field (Mets)",
    "NYY": "Yankee Stadium (Yankees)",
    "ATH": "Oakland Coliseum (Athletics)",
    "PHI": "Citizens Bank Park (Phillies)",
    "PIT": "PNC Park (Pirates)",
    "SD":  "Petco Park (Padres)",
    "SF":  "Oracle Park (Giants)",
    "SEA": "T-Mobile Park (Mariners)",
    "STL": "Busch Stadium (Cardinals)",
    "TB":  "Tropicana Field (Rays)",
    "TEX": "Globe Life Field (Rangers)",
    "TOR": "Rogers Centre (Blue Jays)",
    "WSH": "Nationals Park (Nationals)",
}

# Mapping from team abbreviation to stadium SVG filename
TEAM_TO_SVG = {
    "AZ": "diamondbacks.svg", "ATL": "braves.svg", "BAL": "orioles.svg",
    "BOS": "red_sox.svg", "CHC": "cubs.svg", "CIN": "reds.svg",
    "CLE": "guardians.svg", "COL": "rockies.svg", "CWS": "white_sox.svg",
    "DET": "tigers.svg", "HOU": "astros.svg", "KC": "royals.svg",
    "LAA": "angels.svg", "LAD": "dodgers.svg", "MIA": "marlins.svg",
    "MIL": "brewers.svg", "MIN": "twins.svg", "NYM": "mets.svg",
    "NYY": "yankees.svg", "ATH": "athletics.svg", "PHI": "phillies.svg",
    "PIT": "pirates.svg", "SD": "padres.svg", "SF": "giants.svg",
    "SEA": "mariners.svg", "STL": "cardinals.svg", "TB": "rays.svg",
    "TEX": "rangers.svg", "TOR": "blue_jays.svg", "WSH": "nationals.svg",
}

# All 30 MLB teams with IDs for roster fetching
MLB_TEAMS = {
    "AZ":  {"id": 109, "name": "Arizona Diamondbacks", "abbrev": "AZ"},
    "ATL": {"id": 144, "name": "Atlanta Braves", "abbrev": "ATL"},
    "BAL": {"id": 110, "name": "Baltimore Orioles", "abbrev": "BAL"},
    "BOS": {"id": 111, "name": "Boston Red Sox", "abbrev": "BOS"},
    "CHC": {"id": 112, "name": "Chicago Cubs", "abbrev": "CHC"},
    "CIN": {"id": 113, "name": "Cincinnati Reds", "abbrev": "CIN"},
    "CLE": {"id": 114, "name": "Cleveland Guardians", "abbrev": "CLE"},
    "COL": {"id": 115, "name": "Colorado Rockies", "abbrev": "COL"},
    "CWS": {"id": 145, "name": "Chicago White Sox", "abbrev": "CWS"},
    "DET": {"id": 116, "name": "Detroit Tigers", "abbrev": "DET"},
    "HOU": {"id": 117, "name": "Houston Astros", "abbrev": "HOU"},
    "KC":  {"id": 118, "name": "Kansas City Royals", "abbrev": "KC"},
    "LAA": {"id": 108, "name": "Los Angeles Angels", "abbrev": "LAA"},
    "LAD": {"id": 119, "name": "Los Angeles Dodgers", "abbrev": "LAD"},
    "MIA": {"id": 146, "name": "Miami Marlins", "abbrev": "MIA"},
    "MIL": {"id": 158, "name": "Milwaukee Brewers", "abbrev": "MIL"},
    "MIN": {"id": 142, "name": "Minnesota Twins", "abbrev": "MIN"},
    "NYM": {"id": 121, "name": "New York Mets", "abbrev": "NYM"},
    "NYY": {"id": 147, "name": "New York Yankees", "abbrev": "NYY"},
    "ATH": {"id": 133, "name": "Athletics", "abbrev": "ATH"},
    "PHI": {"id": 143, "name": "Philadelphia Phillies", "abbrev": "PHI"},
    "PIT": {"id": 134, "name": "Pittsburgh Pirates", "abbrev": "PIT"},
    "SD":  {"id": 135, "name": "San Diego Padres", "abbrev": "SD"},
    "SF":  {"id": 137, "name": "San Francisco Giants", "abbrev": "SF"},
    "SEA": {"id": 136, "name": "Seattle Mariners", "abbrev": "SEA"},
    "STL": {"id": 138, "name": "St. Louis Cardinals", "abbrev": "STL"},
    "TB":  {"id": 139, "name": "Tampa Bay Rays", "abbrev": "TB"},
    "TEX": {"id": 140, "name": "Texas Rangers", "abbrev": "TEX"},
    "TOR": {"id": 141, "name": "Toronto Blue Jays", "abbrev": "TOR"},
    "WSH": {"id": 120, "name": "Washington Nationals", "abbrev": "WSH"},
}

# ---------------------------------------------------------------------------
# Pitch location profiles by pitch type (mean px, mean pz, std_px, std_pz)
# Derived from league-average Statcast data. Coordinates in feet.
# Convention: px > 0 = arm side for RHP (inside to RHH)
# ---------------------------------------------------------------------------
PITCH_LOCATION_PROFILES = {
    "FF": {"px_mean": 0.0,  "pz_mean": 2.8, "px_std": 0.55, "pz_std": 0.55},
    "SI": {"px_mean": 0.15, "pz_mean": 2.2, "px_std": 0.55, "pz_std": 0.50},
    "FC": {"px_mean":-0.1,  "pz_mean": 2.6, "px_std": 0.50, "pz_std": 0.55},
    "SL": {"px_mean":-0.25, "pz_mean": 2.1, "px_std": 0.60, "pz_std": 0.65},
    "CU": {"px_mean":-0.1,  "pz_mean": 1.8, "px_std": 0.55, "pz_std": 0.70},
    "CH": {"px_mean": 0.15, "pz_mean": 2.0, "px_std": 0.55, "pz_std": 0.60},
    "FS": {"px_mean": 0.1,  "pz_mean": 1.9, "px_std": 0.50, "pz_std": 0.60},
    "KC": {"px_mean":-0.1,  "pz_mean": 1.7, "px_std": 0.55, "pz_std": 0.70},
    "ST": {"px_mean":-0.30, "pz_mean": 2.0, "px_std": 0.65, "pz_std": 0.65},
    "SV": {"px_mean":-0.20, "pz_mean": 1.9, "px_std": 0.60, "pz_std": 0.70},
    "KN": {"px_mean": 0.0,  "pz_mean": 2.3, "px_std": 0.70, "pz_std": 0.70},
}

# Strike zone boundaries (feet)
SZ_LEFT = -0.708  # half of 17-inch plate
SZ_RIGHT = 0.708
SZ_BOT_DEFAULT = 1.6
SZ_TOP_DEFAULT = 3.5

# Typical pitch outcomes by zone — P(swing) and P(whiff|swing)
# These are baseline rates that get adjusted by batter/pitcher profiles
_BASE_ZONE_SWING_RATE = 0.68
_BASE_CHASE_RATE = 0.295
_BASE_ZONE_WHIFF_RATE = 0.15
_BASE_CHASE_WHIFF_RATE = 0.38
_BASE_FOUL_GIVEN_CONTACT = 0.42  # foul / (foul + in_play) when contact is made

# Outcome distributions for BIP exit velo / launch angle by result
_BIP_PROFILES = {
    "HR":  {"ev_mean": 103.0, "ev_std": 4.0, "la_mean": 28.0, "la_std": 5.0, "dist_mean": 395, "dist_std": 20},
    "3B":  {"ev_mean": 100.0, "ev_std": 5.0, "la_mean": 15.0, "la_std": 8.0, "dist_mean": 340, "dist_std": 30},
    "2B":  {"ev_mean": 97.0,  "ev_std": 6.0, "la_mean": 18.0, "la_std": 10.0, "dist_mean": 300, "dist_std": 40},
    "1B":  {"ev_mean": 91.0,  "ev_std": 8.0, "la_mean": 5.0,  "la_std": 15.0, "dist_mean": 220, "dist_std": 60},
    "OUT": {"ev_mean": 87.0,  "ev_std": 10.0, "la_mean": 25.0, "la_std": 20.0, "dist_mean": 260, "dist_std": 80},
}

# Lazy-loaded data
_park_factors_df = None
_pitcher_arsenal_df = None
_batter_spray_df = None


def _load_data():
    """Lazy-load parquet data files."""
    global _park_factors_df, _pitcher_arsenal_df, _batter_spray_df
    if _park_factors_df is not None:
        return
    try:
        _park_factors_df = pd.read_parquet(os.path.join(_DATA_DIR, "park_factors.parquet"))
    except Exception:
        _park_factors_df = pd.DataFrame()
    try:
        _pitcher_arsenal_df = pd.read_parquet(os.path.join(_DATA_DIR, "pitcher_arsenal.parquet"))
    except Exception:
        _pitcher_arsenal_df = pd.DataFrame()
    try:
        _batter_spray_df = pd.read_parquet(os.path.join(_DATA_DIR, "batter_spray_profiles.parquet"))
    except Exception:
        _batter_spray_df = pd.DataFrame()


def _get_park_factors(venue: str, season: int = 2025) -> dict:
    """Get park factors for a venue. Returns multipliers (1.0 = neutral)."""
    _load_data()
    if _park_factors_df is None or _park_factors_df.empty:
        return {"hr_factor": 1.0, "hit_factor": 1.0, "2b_factor": 1.0, "3b_factor": 1.0, "run_factor": 1.0}

    pf = _park_factors_df
    mask = (pf["venue"] == venue) & (pf["stand"] == "ALL")
    row = pf[mask & (pf["season"] == season)]
    if row.empty:
        row = pf[mask].sort_values("season", ascending=False).head(1)
    if row.empty:
        return {"hr_factor": 1.0, "hit_factor": 1.0, "2b_factor": 1.0, "3b_factor": 1.0, "run_factor": 1.0}

    r = row.iloc[0]
    return {
        "hr_factor": float(r.get("hr_factor", 1.0)) if pd.notna(r.get("hr_factor")) else 1.0,
        "hit_factor": float(r.get("hit_factor", 1.0)) if pd.notna(r.get("hit_factor")) else 1.0,
        "2b_factor": float(r.get("2b_factor", 1.0)) if pd.notna(r.get("2b_factor")) else 1.0,
        "3b_factor": float(r.get("3b_factor", 1.0)) if pd.notna(r.get("3b_factor")) else 1.0,
        "run_factor": float(r.get("run_factor", 1.0)) if pd.notna(r.get("run_factor")) else 1.0,
    }


def _get_pitcher_arsenal_profile(pitcher_id: int, season: int = 2025) -> list:
    """
    Get pitcher's pitch arsenal from pitcher_arsenal.parquet.
    Returns list of dicts with pitch_type, usage, avg_velo, avg_spin, etc.
    """
    _load_data()
    if _pitcher_arsenal_df is None or _pitcher_arsenal_df.empty:
        return []

    pa = _pitcher_arsenal_df
    mask = (pa["pitcher"] == pitcher_id) & (pa["stand"] == "ALL")
    df = pa[mask & (pa["season"] == season)]
    if df.empty:
        df = pa[mask].sort_values("season", ascending=False)
        if not df.empty:
            latest = df["season"].iloc[0]
            df = df[df["season"] == latest]
    if df.empty:
        return []

    arsenal = []
    for _, row in df.iterrows():
        arsenal.append({
            "pitch_type": row["pitch_type"],
            "usage": float(row.get("usage", 0)),
            "avg_velo": float(row.get("avg_velo", 93)) if pd.notna(row.get("avg_velo")) else 93.0,
            "avg_spin": float(row.get("avg_spin", 2200)) if pd.notna(row.get("avg_spin")) else 2200.0,
            "avg_hb": float(row.get("avg_hb", 0)) if pd.notna(row.get("avg_hb")) else 0.0,
            "avg_ivb": float(row.get("avg_ivb", 0)) if pd.notna(row.get("avg_ivb")) else 0.0,
            "whiff_rate": float(row.get("whiff_rate", 0.24)) if pd.notna(row.get("whiff_rate")) else 0.24,
            "zone_rate": float(row.get("zone_rate", 0.45)) if pd.notna(row.get("zone_rate")) else 0.45,
            "chase_rate": float(row.get("chase_rate", 0.29)) if pd.notna(row.get("chase_rate")) else 0.29,
        })
    return sorted(arsenal, key=lambda x: -x["usage"])


def _get_batter_spray_profile(batter_id: int, season: int = 2025) -> dict:
    """Get batter's spray angle tendencies."""
    _load_data()
    default = {"pull_pct": 0.40, "center_pct": 0.33, "oppo_pct": 0.27}
    if _batter_spray_df is None or _batter_spray_df.empty:
        return default

    bs = _batter_spray_df
    mask = bs["batter"] == batter_id
    row = bs[mask & (bs["season"] == season)]
    if row.empty:
        row = bs[mask].sort_values("season", ascending=False).head(1)
    if row.empty:
        return default

    r = row.iloc[0]
    return {
        "pull_pct": float(r.get("bat_pull_pct", 0.40)),
        "center_pct": float(r.get("bat_center_pct", 0.33)),
        "oppo_pct": float(r.get("bat_oppo_pct", 0.27)),
    }


# ---------------------------------------------------------------------------
# Pitch selection model integration
# ---------------------------------------------------------------------------

def _predict_pitch_type(
    pitcher_arsenal: list,
    p_throws: str,
    stand: str,
    balls: int,
    strikes: int,
    outs: int,
    inning: int,
    runner_1b: int,
    runner_2b: int,
    runner_3b: int,
    score_diff: int,
    n_thru_order: int,
    pitch_num_in_ab: int,
    prev_pitch: str,
    prev_pitch_velo: float,
    rng: random.Random,
) -> str:
    """
    Predict the next pitch type using the pitch selection XGBoost model.
    Falls back to weighted random from arsenal if model unavailable.
    """
    # Try using the ML model
    try:
        from services.matchup_predict import _pitch_sel, _pitch_sel_meta, _load as _load_matchup
        _load_matchup()

        if _pitch_sel is not None and _pitch_sel_meta is not None:
            model = _pitch_sel["model"]
            le_stand = _pitch_sel["stand_encoder"]
            le_p_throws = _pitch_sel["p_throws_encoder"]
            le_prev = _pitch_sel["prev_pitch_encoder"]

            # Build arsenal usage dict
            arsenal_usage = {}
            for a in pitcher_arsenal:
                arsenal_usage[f"arsenal_{a['pitch_type']}"] = a["usage"]

            # Compute avg velo across arsenal
            if pitcher_arsenal:
                total_usage = sum(a["usage"] for a in pitcher_arsenal)
                p_avg_velo = sum(a["avg_velo"] * a["usage"] for a in pitcher_arsenal) / max(total_usage, 0.01)
            else:
                p_avg_velo = 93.5

            runners_on = runner_1b + runner_2b + runner_3b

            # Encode categoricals
            try:
                stand_enc = le_stand.transform([stand])[0]
            except ValueError:
                stand_enc = 1  # default R
            try:
                p_throws_enc = le_p_throws.transform([p_throws])[0]
            except ValueError:
                p_throws_enc = 1

            # Previous pitch encoding
            prev_valid = prev_pitch if prev_pitch else "NONE"
            try:
                prev_enc = le_prev.transform([prev_valid])[0]
            except ValueError:
                try:
                    prev_enc = le_prev.transform(["OTHER"])[0]
                except ValueError:
                    prev_enc = le_prev.transform(["NONE"])[0]

            # Build feature vector in the order the model expects
            feature_names = _pitch_sel_meta["features"]
            feature_vals = {
                "balls": balls,
                "strikes": strikes,
                "ahead_in_count": 1 if strikes > balls else 0,
                "behind_in_count": 1 if balls > strikes else 0,
                "two_strikes": 1 if strikes == 2 else 0,
                "three_balls": 1 if balls == 3 else 0,
                "first_pitch": 1 if (balls == 0 and strikes == 0) else 0,
                "outs_when_up": outs,
                "inning": inning,
                "early_innings": 1 if inning <= 3 else 0,
                "mid_innings": 1 if 4 <= inning <= 6 else 0,
                "late_innings": 1 if inning >= 7 else 0,
                "runner_on_1b": runner_1b,
                "runner_on_2b": runner_2b,
                "runner_on_3b": runner_3b,
                "runners_on": runners_on,
                "risp": 1 if (runner_2b or runner_3b) else 0,
                "score_diff": score_diff,
                "n_thruorder_pitcher": n_thru_order,
                "pitch_num_in_ab": pitch_num_in_ab,
                "stand_enc": stand_enc,
                "p_throws_enc": p_throws_enc,
                "prev_pitch_enc": prev_enc,
                "p_avg_velo": p_avg_velo,
                "p_num_pitch_types": len(pitcher_arsenal),
                "prev_pitch_velo": prev_pitch_velo if prev_pitch_velo else p_avg_velo,
            }
            # Add arsenal columns
            all_arsenal_cols = [f for f in feature_names if f.startswith("arsenal_")]
            for col in all_arsenal_cols:
                feature_vals.setdefault(col, 0.0)
            feature_vals.update(arsenal_usage)

            X = np.array([[feature_vals.get(f, 0.0) for f in feature_names]], dtype=np.float32)
            proba = model.predict_proba(X)[0]
            classes = _pitch_sel_meta["classes"]

            # Filter to only pitches in this pitcher's arsenal
            arsenal_types = {a["pitch_type"] for a in pitcher_arsenal}
            filtered_probs = {}
            for i, cls in enumerate(classes):
                if cls in arsenal_types:
                    filtered_probs[cls] = float(proba[i])

            if filtered_probs:
                total = sum(filtered_probs.values())
                if total > 0:
                    # Sample from the distribution
                    types = list(filtered_probs.keys())
                    weights = [filtered_probs[t] / total for t in types]
                    return rng.choices(types, weights=weights, k=1)[0]
    except Exception as e:
        log.debug("Pitch selection model failed, using arsenal fallback: %s", e)

    # Fallback: weighted random from arsenal
    if pitcher_arsenal:
        types = [a["pitch_type"] for a in pitcher_arsenal]
        weights = [a["usage"] for a in pitcher_arsenal]
        return rng.choices(types, weights=weights, k=1)[0]

    return "FF"  # ultimate fallback


# ---------------------------------------------------------------------------
# Pitch location generation
# ---------------------------------------------------------------------------

def _generate_pitch_location(
    pitch_type: str,
    p_throws: str,
    stand: str,
    intended_zone: bool,
    rng: random.Random,
) -> Tuple[float, float]:
    """
    Generate realistic pitch location (px, pz) in feet.
    intended_zone: True if pitcher is trying to throw a strike.
    """
    profile = PITCH_LOCATION_PROFILES.get(pitch_type, PITCH_LOCATION_PROFILES["FF"])

    px_mean = profile["px_mean"]
    pz_mean = profile["pz_mean"]
    px_std = profile["px_std"]
    pz_std = profile["pz_std"]

    # Mirror px for LHP
    if p_throws == "L":
        px_mean = -px_mean

    # Adjust for batter side — pitcher tends to work away from same-side batters
    if p_throws == stand:
        px_mean += 0.15 * (1 if p_throws == "R" else -1)
    else:
        px_mean -= 0.1 * (1 if p_throws == "R" else -1)

    if intended_zone:
        # Tighter distribution, centered in zone
        px = rng.gauss(px_mean * 0.5, px_std * 0.65)
        pz = rng.gauss((SZ_BOT_DEFAULT + SZ_TOP_DEFAULT) / 2 + (pz_mean - 2.5) * 0.3, pz_std * 0.6)
    else:
        # Wider distribution, chase pitches
        px = rng.gauss(px_mean * 1.3, px_std * 1.1)
        pz = rng.gauss(pz_mean * 0.85, pz_std * 1.1)

    # Clamp to realistic range
    px = max(-2.5, min(2.5, px))
    pz = max(0.0, min(5.0, pz))

    return round(px, 3), round(pz, 3)


def _is_in_zone(px: float, pz: float, sz_top: float = SZ_TOP_DEFAULT, sz_bot: float = SZ_BOT_DEFAULT) -> bool:
    """Check if pitch is in the strike zone."""
    return SZ_LEFT <= px <= SZ_RIGHT and sz_bot <= pz <= sz_top


# ---------------------------------------------------------------------------
# Pitch outcome determination
# ---------------------------------------------------------------------------

def _determine_pitch_outcome(
    px: float,
    pz: float,
    pitch_type: str,
    pitch_arsenal_entry: dict,
    batter_chase_rate: float,
    batter_zone_swing_rate: float,
    batter_whiff_rate: float,
    balls: int,
    strikes: int,
    rng: random.Random,
    sz_top: float = SZ_TOP_DEFAULT,
    sz_bot: float = SZ_BOT_DEFAULT,
) -> str:
    """
    Determine the outcome of a single pitch.
    Returns one of: 'ball', 'called_strike', 'swinging_strike', 'foul', 'in_play'
    """
    in_zone = _is_in_zone(px, pz, sz_top, sz_bot)

    # Determine if batter swings
    if in_zone:
        swing_rate = batter_zone_swing_rate
        # Higher swing rate on 2 strikes
        if strikes == 2:
            swing_rate = min(swing_rate + 0.08, 0.95)
    else:
        swing_rate = batter_chase_rate
        # Reduce chase on 3-ball counts
        if balls == 3:
            swing_rate *= 0.65
        # Increase chase on 2 strikes
        if strikes == 2:
            swing_rate = min(swing_rate + 0.06, 0.60)

        # Distance from zone affects chase rate
        dist_from_zone = 0
        if px < SZ_LEFT:
            dist_from_zone += abs(px - SZ_LEFT)
        elif px > SZ_RIGHT:
            dist_from_zone += abs(px - SZ_RIGHT)
        if pz < sz_bot:
            dist_from_zone += abs(pz - sz_bot)
        elif pz > sz_top:
            dist_from_zone += abs(pz - sz_top)

        # Further from zone = less likely to swing
        swing_rate *= max(0.1, 1.0 - dist_from_zone * 0.4)

    swings = rng.random() < swing_rate

    if not swings:
        # Called pitch
        if in_zone:
            return "called_strike"
        else:
            return "ball"

    # Batter swings — determine contact
    # Per-pitch whiff rate from arsenal data
    pitch_whiff = pitch_arsenal_entry.get("whiff_rate", 0.24) if pitch_arsenal_entry else 0.24

    # Blend pitcher's pitch whiff rate with batter's overall whiff rate
    whiff_rate = (pitch_whiff * 0.6 + batter_whiff_rate * 0.4)

    # Out of zone swings whiff more
    if not in_zone:
        whiff_rate = min(whiff_rate * 1.4, 0.75)

    # Two-strike whiff slightly higher (more defensive swings)
    if strikes == 2:
        whiff_rate *= 1.05

    if rng.random() < whiff_rate:
        return "swinging_strike"

    # Contact made — foul or in play
    # Two-strike counts produce more fouls (protective swings)
    foul_rate = _BASE_FOUL_GIVEN_CONTACT
    if strikes == 2:
        foul_rate = 0.55  # more foul balls with 2 strikes

    # Barely in/out of zone = more fouls
    if not in_zone:
        foul_rate = min(foul_rate + 0.10, 0.65)

    if rng.random() < foul_rate:
        return "foul"

    return "in_play"


# ---------------------------------------------------------------------------
# BIP outcome + coordinates
# ---------------------------------------------------------------------------

def _determine_bip_outcome(
    pa_probs: dict,
    park_factors: dict,
    rng: random.Random,
) -> str:
    """
    Given PA outcome probabilities, determine the BIP-specific outcome.
    Only called when the pitch result is 'in_play'.
    Applies park factors to adjust probabilities.
    """
    # Extract hit/out probabilities from matchup model
    p_hr = pa_probs.get("HR", 0.03)
    p_3b = pa_probs.get("3B", 0.005)
    p_2b = pa_probs.get("2B", 0.045)
    p_1b = pa_probs.get("1B", 0.15)
    p_out = pa_probs.get("OUT", 0.70)

    # Apply park factors
    p_hr *= park_factors.get("hr_factor", 1.0)
    p_2b *= park_factors.get("2b_factor", 1.0)
    p_3b *= park_factors.get("3b_factor", 1.0)
    p_1b *= park_factors.get("hit_factor", 1.0)

    # We only care about BIP outcomes (no K/BB here), so re-normalize
    # to just hit types + out
    total = p_hr + p_3b + p_2b + p_1b + p_out
    if total <= 0:
        return "OUT"

    probs = {
        "HR": p_hr / total,
        "3B": p_3b / total,
        "2B": p_2b / total,
        "1B": p_1b / total,
        "OUT": p_out / total,
    }

    outcomes = list(probs.keys())
    weights = [probs[o] for o in outcomes]
    return rng.choices(outcomes, weights=weights, k=1)[0]


def _generate_bip_data(
    outcome: str,
    batter_stand: str,
    spray_profile: dict,
    venue: str,
    rng: random.Random,
) -> dict:
    """
    Generate ball-in-play data: coordinates, exit velo, launch angle, distance.
    Coordinates are in the 0-250 normalized space used by stadium SVGs.
    """
    profile = _BIP_PROFILES.get(outcome, _BIP_PROFILES["OUT"])

    ev = max(50, rng.gauss(profile["ev_mean"], profile["ev_std"]))
    la = rng.gauss(profile["la_mean"], profile["la_std"])
    dist = max(30, rng.gauss(profile["dist_mean"], profile["dist_std"]))

    # Spray angle based on batter tendency
    pull_pct = spray_profile.get("pull_pct", 0.40)
    center_pct = spray_profile.get("center_pct", 0.33)
    oppo_pct = spray_profile.get("oppo_pct", 0.27)

    spray_roll = rng.random()
    if spray_roll < pull_pct:
        # Pull side
        if batter_stand == "R":
            angle = rng.gauss(-25, 12)  # Left field
        else:
            angle = rng.gauss(25, 12)   # Right field
    elif spray_roll < pull_pct + center_pct:
        # Center
        angle = rng.gauss(0, 10)
    else:
        # Opposite field
        if batter_stand == "R":
            angle = rng.gauss(25, 12)   # Right field
        else:
            angle = rng.gauss(-25, 12)  # Left field

    angle = max(-45, min(45, angle))

    # Convert spray angle + distance to (x, y) in 0-250 coordinate space
    # Home plate at (125, 208), y decreases going to outfield
    angle_rad = math.radians(angle)
    # Scale: ~400ft = ~180 pixels
    scale = 180 / 400
    dx = dist * math.sin(angle_rad) * scale
    dy = -dist * math.cos(angle_rad) * scale  # negative because y goes up

    x = 125 + dx
    y = 208 + dy

    # Clamp to field
    x = max(5, min(245, x))
    y = max(5, min(230, y))

    # Determine if it's a ground ball or fly ball
    is_ground = la < 10
    kind = "ground" if is_ground else "air"

    # Check stadium wall for HR validation
    if outcome == "HR":
        dims = STADIUM_DIMENSIONS.get(venue)
        if dims:
            # Get wall distance at this spray angle
            wall_dist = _wall_distance_at_angle(angle, dims)
            wall_height = dims.get("wall", 8)
            # Simple physics: does the ball clear the wall?
            # Approximate: need enough distance and launch angle
            if dist < wall_dist + 5:
                # Borderline — might not clear
                clear_height = _estimate_ball_height(ev, la, wall_dist)
                if clear_height < wall_height:
                    # Doesn't clear — downgrade to a long out or double
                    outcome = "OUT" if rng.random() < 0.6 else "2B"

    is_out = outcome == "OUT"

    # Generate event description
    if outcome == "HR":
        event = "Home Run"
    elif outcome == "3B":
        event = "Triple"
    elif outcome == "2B":
        event = "Double"
    elif outcome == "1B":
        if is_ground:
            event = rng.choice(["Groundball Single", "Infield Single", "Single"])
        else:
            event = rng.choice(["Line Drive Single", "Bloop Single", "Single"])
    else:
        if is_ground:
            event = rng.choice(["Groundout", "Groundout", "Fielders Choice", "Forceout"])
        else:
            if la > 35:
                event = rng.choice(["Flyout", "Flyout", "Pop Out"])
            else:
                event = rng.choice(["Lineout", "Flyout", "Flyout"])

    return {
        "outcome": outcome,
        "x": round(x, 1),
        "y": round(y, 1),
        "event": event,
        "ev": round(ev, 1),
        "la": round(la, 1),
        "dist": round(dist),
        "is_out": is_out,
        "kind": kind,
    }


def _wall_distance_at_angle(spray_angle: float, dims: dict) -> float:
    """
    Interpolate wall distance at a given spray angle.
    Spray angle: -45 (right field) to +45 (left field), 0 = center.
    """
    # Map angle to field positions
    # -45 = RF line, -22.5 = RCF, 0 = CF, 22.5 = LCF, 45 = LF line
    angle = max(-45, min(45, spray_angle))

    lf = dims["lf"]
    lcf = dims["lcf"]
    cf = dims["cf"]
    rcf = dims["rcf"]
    rf = dims["rf"]

    # Piecewise linear interpolation
    if angle >= 22.5:
        t = (angle - 22.5) / 22.5
        return lcf + t * (lf - lcf)
    elif angle >= 0:
        t = angle / 22.5
        return cf + t * (lcf - cf)
    elif angle >= -22.5:
        t = -angle / 22.5
        return cf + t * (rcf - cf)
    else:
        t = (-angle - 22.5) / 22.5
        return rcf + t * (rf - rcf)


def _estimate_ball_height(exit_velo: float, launch_angle: float, distance: float) -> float:
    """
    Rough estimate of ball height at a given distance from home plate.
    Uses simplified projectile physics (no air resistance).
    """
    if exit_velo <= 0 or launch_angle <= 0:
        return 0

    v = exit_velo * 1.467  # mph to ft/s
    theta = math.radians(launch_angle)
    vx = v * math.cos(theta)
    vy = v * math.sin(theta)

    if vx <= 0:
        return 0

    t = distance / vx  # time to reach wall
    # Height = vy*t - 0.5*g*t^2 + initial height (~3ft for bat contact)
    g = 32.174  # ft/s^2
    height = 3.0 + vy * t - 0.5 * g * t * t

    return max(0, height)


# ---------------------------------------------------------------------------
# Base running
# ---------------------------------------------------------------------------

def _advance_runners(
    bases: list,  # [1B_runner, 2B_runner, 3B_runner] — player_id or None
    event: str,
    rng: random.Random,
) -> Tuple[list, int]:
    """
    Advance runners based on BIP event. Returns (new_bases, runs_scored).
    bases: [1B, 2B, 3B] where each is player_id or None.
    """
    runs = 0
    new_bases = [None, None, None]

    if event == "HR":
        # Everyone scores
        runs = sum(1 for b in bases if b is not None) + 1  # +1 for batter
        return [None, None, None], runs

    if event in ("BB", "HBP", "IBB"):
        # Force runners forward
        # Batter to 1B, force chain
        if bases[0] is not None:
            if bases[1] is not None:
                if bases[2] is not None:
                    runs += 1  # runner on 3rd scores
                new_bases[2] = bases[1]
            else:
                new_bases[1] = bases[0]
                new_bases[2] = bases[2]
            new_bases[0] = "batter"
        else:
            new_bases[0] = "batter"
            new_bases[1] = bases[1]
            new_bases[2] = bases[2]
        return new_bases, runs

    if event == "1B":
        # Runners advance 1-2 bases
        if bases[2] is not None:
            runs += 1
        if bases[1] is not None:
            if rng.random() < 0.6:
                runs += 1  # scores from 2B on single
            else:
                new_bases[2] = bases[1]
        if bases[0] is not None:
            if new_bases[2] is None:
                new_bases[2] = bases[0] if rng.random() < 0.3 else None
                if new_bases[2] is None:
                    new_bases[1] = bases[0]
            else:
                new_bases[1] = bases[0]
        new_bases[0] = "batter"
        return new_bases, runs

    if event == "2B":
        if bases[2] is not None:
            runs += 1
        if bases[1] is not None:
            runs += 1
        if bases[0] is not None:
            if rng.random() < 0.55:
                runs += 1
            else:
                new_bases[2] = bases[0]
        new_bases[1] = "batter"
        return new_bases, runs

    if event == "3B":
        runs += sum(1 for b in bases if b is not None)
        new_bases[2] = "batter"
        return new_bases, runs

    # OUT — runners may advance on sac flies, groundouts etc.
    # Simplified: runner on 3B scores on fly out with <2 outs (sac fly)
    # handled by caller based on out type

    return bases.copy(), runs


def _advance_runners_on_out(
    bases: list,
    outs_before: int,
    bip_data: dict,
    rng: random.Random,
) -> Tuple[list, int]:
    """Handle runner advancement on outs."""
    runs = 0
    new_bases = bases.copy()
    kind = bip_data.get("kind", "air")
    la = bip_data.get("la", 20)

    if outs_before < 2:
        # Sac fly: runner on 3B scores on fly ball out
        if kind == "air" and la > 15 and bases[2] is not None:
            runs += 1
            new_bases[2] = None
            # Runner on 2B might advance to 3B
            if bases[1] is not None and rng.random() < 0.3:
                new_bases[2] = bases[1]
                new_bases[1] = None

        # Ground ball: runner on 3B might score, DP possible
        elif kind == "ground":
            if bases[2] is not None and rng.random() < 0.5:
                runs += 1
                new_bases[2] = None

            # Runner on 2B advances to 3B on ground ball
            if bases[1] is not None and rng.random() < 0.4:
                if new_bases[2] is None:
                    new_bases[2] = bases[1]
                    new_bases[1] = None

    return new_bases, runs


# ---------------------------------------------------------------------------
# Main simulation engine
# ---------------------------------------------------------------------------

def simulate_game(
    away_lineup: List[dict],
    home_lineup: List[dict],
    away_pitcher: dict,
    home_pitcher: dict,
    away_bullpen: List[dict],
    home_bullpen: List[dict],
    venue: str = "NYY",
    season: int = 2026,
    seed: Optional[int] = None,
) -> dict:
    """
    Simulate a full baseball game pitch by pitch.

    Parameters:
    -----------
    away_lineup: List of 9 dicts with keys: id, name, stand, pos
    home_lineup: Same format
    away_pitcher: Dict with keys: id, name, p_throws
    home_pitcher: Same format
    away_bullpen: List of dicts with keys: id, name, p_throws
    home_bullpen: Same format
    venue: Team abbreviation (e.g., "NYY", "LAD")
    season: Current season year
    seed: Random seed for reproducibility

    Returns:
    --------
    Complete game log dict with pitches, events, lineups, score, etc.
    """
    rng = random.Random(seed)

    # Load park factors for the venue
    park_factors = _get_park_factors(venue, season)
    dims = STADIUM_DIMENSIONS.get(venue, STADIUM_DIMENSIONS.get("NYY"))

    # Load pitcher arsenal profiles
    away_pitcher_arsenal = _get_pitcher_arsenal_profile(away_pitcher["id"], season)
    home_pitcher_arsenal = _get_pitcher_arsenal_profile(home_pitcher["id"], season)

    # If arsenal is empty, create a generic one
    if not away_pitcher_arsenal:
        away_pitcher_arsenal = _default_arsenal(away_pitcher.get("p_throws", "R"))
    if not home_pitcher_arsenal:
        home_pitcher_arsenal = _default_arsenal(home_pitcher.get("p_throws", "R"))

    # Load batter spray profiles
    spray_profiles = {}
    for batter in away_lineup + home_lineup:
        spray_profiles[batter["id"]] = _get_batter_spray_profile(batter["id"], season)

    # --------------- Game state ---------------
    pitches = []          # Every pitch in the game
    events = []           # PA/game events
    game_id = str(uuid.uuid4())

    score = {"away": 0, "home": 0}
    linescore = {"away": [], "home": []}

    # Current pitchers
    current_pitchers = {
        "away": {**away_pitcher, "arsenal": away_pitcher_arsenal, "pitch_count": 0, "is_starter": True},
        "home": {**home_pitcher, "arsenal": home_pitcher_arsenal, "pitch_count": 0, "is_starter": True},
    }
    bullpen_remaining = {
        "away": list(away_bullpen),
        "home": list(home_bullpen),
    }
    pitching_changes = []  # Track all pitching changes

    # Batting order cursors
    batting_index = {"away": 0, "home": 0}

    # Player stats tracking
    player_stats = {}
    for b in away_lineup + home_lineup:
        player_stats[b["id"]] = {
            "name": b["name"], "team": "away" if b in away_lineup else "home",
            "pa": 0, "ab": 0, "h": 0, "bb": 0, "k": 0, "hr": 0,
            "rbi": 0, "r": 0, "1b": 0, "2b": 0, "3b": 0, "hbp": 0,
        }

    # Track pitcher stats
    pitcher_stats = {}
    for p in [away_pitcher, home_pitcher] + away_bullpen + home_bullpen:
        pitcher_stats[p["id"]] = {
            "name": p["name"], "team": "away" if p in [away_pitcher] + away_bullpen else "home",
            "ip_outs": 0, "h": 0, "r": 0, "er": 0, "bb": 0, "k": 0,
            "hr": 0, "pitches": 0, "bf": 0,
        }

    # Times through order tracking
    n_thru_order = {"away": 1, "home": 1}

    # --------------- Inning loop ---------------
    max_innings = 9
    inning = 1
    game_over = False

    while not game_over:
        for half in ["Top", "Bottom"]:
            if game_over:
                break

            batting_team = "away" if half == "Top" else "home"
            pitching_team = "home" if half == "Top" else "away"

            lineup = away_lineup if batting_team == "away" else home_lineup
            pitcher_info = current_pitchers[pitching_team]

            outs = 0
            bases = [None, None, None]  # [1B, 2B, 3B]
            inning_runs = 0

            # Check if bottom of 9th+ needed
            if half == "Bottom" and inning >= max_innings:
                if score["home"] > score["away"]:
                    # Home team already winning, skip
                    linescore["home"].append(0)
                    break

            while outs < 3:
                # --- Check for pitching change ---
                should_change = _should_change_pitcher(pitcher_info, inning, outs, bullpen_remaining[pitching_team])
                if should_change and bullpen_remaining[pitching_team]:
                    new_pitcher = bullpen_remaining[pitching_team].pop(0)
                    new_arsenal = _get_pitcher_arsenal_profile(new_pitcher["id"], season)
                    if not new_arsenal:
                        new_arsenal = _default_arsenal(new_pitcher.get("p_throws", "R"))

                    pitching_changes.append({
                        "inning": inning,
                        "half": half,
                        "outs": outs,
                        "old_pitcher": pitcher_info["id"],
                        "new_pitcher": new_pitcher["id"],
                        "new_pitcher_name": new_pitcher["name"],
                        "pitch_index": len(pitches),
                        "options": [p["id"] for p in bullpen_remaining[pitching_team]],
                    })

                    events.append({
                        "type": "pitching_change",
                        "inning": inning,
                        "half": half,
                        "outs": outs,
                        "old_pitcher_id": pitcher_info["id"],
                        "old_pitcher_name": pitcher_info["name"],
                        "new_pitcher_id": new_pitcher["id"],
                        "new_pitcher_name": new_pitcher["name"],
                        "pitch_index": len(pitches),
                    })

                    current_pitchers[pitching_team] = {
                        **new_pitcher,
                        "arsenal": new_arsenal,
                        "pitch_count": 0,
                        "is_starter": False,
                    }
                    pitcher_info = current_pitchers[pitching_team]

                # --- Get current batter ---
                batter_idx = batting_index[batting_team] % 9
                batter = lineup[batter_idx]
                batting_index[batting_team] += 1

                # Track times through order
                if batter_idx == 0 and batting_index[batting_team] > 9:
                    n_thru_order[pitching_team] = min(n_thru_order[pitching_team] + 1, 5)

                # --- Get matchup prediction ---
                pa_probs = _get_pa_probs(
                    batter["id"], pitcher_info["id"],
                    batter.get("stand", "R"), pitcher_info.get("p_throws", "R"),
                    venue, season, inning, outs,
                    1 if bases[0] else 0,
                    1 if bases[1] else 0,
                    1 if bases[2] else 0,
                    n_thru_order[pitching_team],
                )

                # Get batter's swing profile for pitch outcomes
                batter_chase = pa_probs.get("_batter_chase", _BASE_CHASE_RATE)
                batter_zone_swing = pa_probs.get("_batter_zone_swing", _BASE_ZONE_SWING_RATE)
                batter_whiff = pa_probs.get("_batter_whiff", 0.24)

                spray = spray_profiles.get(batter["id"], {"pull_pct": 0.40, "center_pct": 0.33, "oppo_pct": 0.27})

                # --- Batter strike zone ---
                sz_top = rng.gauss(SZ_TOP_DEFAULT, 0.1)
                sz_bot = rng.gauss(SZ_BOT_DEFAULT, 0.05)

                # --- Pitch-by-pitch PA simulation ---
                balls = 0
                strikes = 0
                pitch_num_in_ab = 0
                prev_pitch = None
                prev_pitch_velo = None
                pa_pitches = []
                pa_complete = False
                pa_event = None
                pa_bip = None

                pitcher_stats[pitcher_info["id"]]["bf"] += 1
                player_stats[batter["id"]]["pa"] += 1

                while not pa_complete:
                    pitch_num_in_ab += 1
                    pitcher_info["pitch_count"] += 1
                    pitcher_stats[pitcher_info["id"]]["pitches"] += 1

                    # Decide pitch type
                    score_diff = score[pitching_team] - score[batting_team]
                    pitch_type = _predict_pitch_type(
                        pitcher_info["arsenal"],
                        pitcher_info.get("p_throws", "R"),
                        batter.get("stand", "R"),
                        balls, strikes, outs, inning,
                        1 if bases[0] else 0,
                        1 if bases[1] else 0,
                        1 if bases[2] else 0,
                        score_diff,
                        n_thru_order[pitching_team],
                        pitch_num_in_ab,
                        prev_pitch,
                        prev_pitch_velo,
                        rng,
                    )

                    # Get arsenal entry for this pitch type
                    arsenal_entry = None
                    for a in pitcher_info["arsenal"]:
                        if a["pitch_type"] == pitch_type:
                            arsenal_entry = a
                            break

                    # Decide if pitcher intends a strike
                    zone_intent = _zone_intent(balls, strikes, pitcher_info, rng)

                    # Generate location
                    px, pz = _generate_pitch_location(
                        pitch_type, pitcher_info.get("p_throws", "R"),
                        batter.get("stand", "R"), zone_intent, rng,
                    )

                    # Generate velocity with some noise
                    base_velo = arsenal_entry["avg_velo"] if arsenal_entry else 93.0
                    mph = round(base_velo + rng.gauss(0, 1.2), 1)

                    # Spin rate
                    base_spin = arsenal_entry["avg_spin"] if arsenal_entry else 2200
                    spin = round(base_spin + rng.gauss(0, 50))

                    # Break
                    hb = arsenal_entry["avg_hb"] if arsenal_entry else 0
                    ivb = arsenal_entry["avg_ivb"] if arsenal_entry else 0

                    # Determine outcome
                    result = _determine_pitch_outcome(
                        px, pz, pitch_type, arsenal_entry,
                        batter_chase, batter_zone_swing, batter_whiff,
                        balls, strikes, rng, sz_top, sz_bot,
                    )

                    # Build pitch record
                    pitch_record = {
                        "idx": len(pitches),
                        "inning": inning,
                        "half": half,
                        "batter_id": batter["id"],
                        "batter_name": batter["name"],
                        "batter_stand": batter.get("stand", "R"),
                        "pitcher_id": pitcher_info["id"],
                        "pitcher_name": pitcher_info["name"],
                        "pitcher_throws": pitcher_info.get("p_throws", "R"),
                        "balls": balls,
                        "strikes": strikes,
                        "outs": outs,
                        "pitch_type": pitch_type,
                        "pitch_name": _pitch_name(pitch_type),
                        "px": px,
                        "pz": pz,
                        "sz_top": round(sz_top, 2),
                        "sz_bot": round(sz_bot, 2),
                        "mph": mph,
                        "spin": spin,
                        "hb": round(hb, 1),
                        "ivb": round(ivb, 1),
                        "result": result,
                        "is_ball": result == "ball",
                        "is_strike": result in ("called_strike", "swinging_strike"),
                        "is_foul": result == "foul",
                        "is_in_play": result == "in_play",
                        "runners": [b is not None for b in bases],
                        "score_away": score["away"],
                        "score_home": score["home"],
                        "event": None,
                        "bip": None,
                    }

                    # Update count
                    if result == "ball":
                        balls += 1
                        if balls >= 4:
                            pa_complete = True
                            pa_event = "BB"
                    elif result in ("called_strike", "swinging_strike"):
                        strikes += 1
                        if strikes >= 3:
                            pa_complete = True
                            pa_event = "K"
                    elif result == "foul":
                        if strikes < 2:
                            strikes += 1
                        # foul with 2 strikes — count stays
                    elif result == "in_play":
                        pa_complete = True
                        bip_outcome = _determine_bip_outcome(pa_probs, park_factors, rng)
                        bip_data = _generate_bip_data(
                            bip_outcome, batter.get("stand", "R"),
                            spray, venue, rng,
                        )
                        pa_event = bip_data["outcome"]
                        pa_bip = bip_data
                        pitch_record["bip"] = bip_data

                    # Record event on PA-ending pitch
                    if pa_complete:
                        pitch_record["event"] = pa_event
                        pitch_record["balls"] = balls if pa_event != "BB" else balls - 1
                        pitch_record["strikes"] = strikes if pa_event != "K" else strikes - 1

                    pitches.append(pitch_record)
                    pa_pitches.append(pitch_record)

                    prev_pitch = pitch_type
                    prev_pitch_velo = mph

                # --- PA complete: update game state ---
                # Update player stats
                ps = player_stats[batter["id"]]
                pst = pitcher_stats[pitcher_info["id"]]

                if pa_event == "K":
                    ps["k"] += 1
                    ps["ab"] += 1
                    pst["k"] += 1
                    outs += 1
                    pst["ip_outs"] += 1

                    events.append({
                        "type": "strikeout",
                        "inning": inning, "half": half,
                        "batter_id": batter["id"],
                        "batter_name": batter["name"],
                        "pitcher_id": pitcher_info["id"],
                        "pitcher_name": pitcher_info["name"],
                        "description": f"{batter['name']} strikes out.",
                        "pitch_index": pitches[-1]["idx"],
                    })

                elif pa_event in ("BB", "HBP", "IBB"):
                    if pa_event == "BB":
                        ps["bb"] += 1
                        pst["bb"] += 1
                    elif pa_event == "HBP":
                        ps["hbp"] += 1

                    new_bases, runs = _advance_runners(bases, pa_event, rng)
                    new_bases[0] = batter["id"]
                    bases = new_bases
                    inning_runs += runs
                    score[batting_team] += runs

                    events.append({
                        "type": "walk",
                        "inning": inning, "half": half,
                        "batter_id": batter["id"],
                        "batter_name": batter["name"],
                        "pitcher_id": pitcher_info["id"],
                        "description": f"{batter['name']} walks.",
                        "runs_scored": runs,
                        "pitch_index": pitches[-1]["idx"],
                    })

                elif pa_event in ("1B", "2B", "3B", "HR"):
                    ps["h"] += 1
                    ps["ab"] += 1
                    ps[pa_event.lower()] = ps.get(pa_event.lower(), 0) + 1
                    pst["h"] += 1
                    if pa_event == "HR":
                        ps["hr"] += 1
                        pst["hr"] += 1

                    new_bases, runs = _advance_runners(bases, pa_event, rng)

                    # Place batter on appropriate base
                    if pa_event == "1B":
                        new_bases[0] = batter["id"]
                    elif pa_event == "2B":
                        new_bases[1] = batter["id"]
                    elif pa_event == "3B":
                        new_bases[2] = batter["id"]
                    # HR: batter scores (already counted in _advance_runners)

                    rbi = runs
                    ps["rbi"] += rbi
                    inning_runs += runs
                    score[batting_team] += runs
                    pst["r"] += runs
                    pst["er"] += runs

                    # Track runs scored by runners
                    for b in bases:
                        if b and b in player_stats:
                            player_stats[b]["r"] += 0  # simplified

                    bases = new_bases

                    desc = f"{batter['name']} {pa_bip['event'].lower()}." if pa_bip else f"{batter['name']} hits a {pa_event}."
                    if rbi > 0:
                        desc += f" {rbi} RBI."

                    events.append({
                        "type": "hit",
                        "inning": inning, "half": half,
                        "batter_id": batter["id"],
                        "batter_name": batter["name"],
                        "pitcher_id": pitcher_info["id"],
                        "event": pa_bip["event"] if pa_bip else pa_event,
                        "description": desc,
                        "runs_scored": runs,
                        "rbi": rbi,
                        "pitch_index": pitches[-1]["idx"],
                        "bip": pa_bip,
                    })

                elif pa_event == "OUT":
                    ps["ab"] += 1
                    outs += 1
                    pst["ip_outs"] += 1

                    # Handle runner advancement on outs
                    new_bases, runs = _advance_runners_on_out(bases, outs - 1, pa_bip or {}, rng)
                    if runs > 0:
                        inning_runs += runs
                        score[batting_team] += runs
                        pst["r"] += runs
                        pst["er"] += runs

                        ps["rbi"] += runs  # sac fly RBI

                    bases = new_bases

                    desc = f"{batter['name']} {pa_bip['event'].lower()}." if pa_bip else f"{batter['name']} makes an out."
                    if runs > 0:
                        desc += f" {runs} RBI."

                    events.append({
                        "type": "out",
                        "inning": inning, "half": half,
                        "batter_id": batter["id"],
                        "batter_name": batter["name"],
                        "pitcher_id": pitcher_info["id"],
                        "event": pa_bip["event"] if pa_bip else "Out",
                        "description": desc,
                        "runs_scored": runs,
                        "pitch_index": pitches[-1]["idx"],
                        "bip": pa_bip,
                    })

                # --- Walk-off check ---
                if half == "Bottom" and inning >= max_innings and score["home"] > score["away"]:
                    game_over = True
                    break

            # End of half-inning
            linescore[batting_team].append(inning_runs)

        # End of full inning
        if not game_over:
            if inning >= max_innings and score["away"] != score["home"]:
                game_over = True
            elif inning >= max_innings:
                max_innings += 1  # Extra innings

        inning += 1
        if inning > 20:  # Safety valve
            game_over = True

    # Pad linescore if needed
    while len(linescore["away"]) < len(linescore["home"]):
        linescore["away"].append(0)
    while len(linescore["home"]) < len(linescore["away"]):
        linescore["home"].append(0)

    # Build final game data
    return {
        "game_id": game_id,
        "venue": venue,
        "stadium_svg": TEAM_TO_SVG.get(venue, "generic.svg"),
        "stadium_dims": dims,
        "park_factors": park_factors,
        "season": season,
        "total_pitches": len(pitches),
        "pitches": pitches,
        "events": events,
        "pitching_changes": pitching_changes,
        "score": score,
        "linescore": linescore,
        "player_stats": player_stats,
        "pitcher_stats": pitcher_stats,
        "away_lineup": away_lineup,
        "home_lineup": home_lineup,
        "away_pitcher": away_pitcher,
        "home_pitcher": home_pitcher,
        "away_bullpen": away_bullpen,
        "home_bullpen": home_bullpen,
        "innings_played": inning - 1,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_pa_probs(
    batter_id, pitcher_id, stand, p_throws,
    venue, season, inning, outs,
    runner_1b, runner_2b, runner_3b, n_thru_order,
) -> dict:
    """Get PA outcome probabilities from the matchup model."""
    try:
        from services.matchup_predict import predict_matchup, _load as _load_matchup
        from services.matchup_predict import _get_batter_features
        _load_matchup()

        result = predict_matchup(
            batter_id=batter_id,
            pitcher_id=pitcher_id,
            stand=stand,
            p_throws=p_throws,
            venue=venue,
            season=season,
            inning=inning,
            outs=outs,
            runner_1b=runner_1b,
            runner_2b=runner_2b,
            runner_3b=runner_3b,
            n_thru_order=n_thru_order,
        )

        if result.get("ok"):
            probs = result["probs"]
            # Also pull batter swing tendencies
            try:
                batter_feats = _get_batter_features(batter_id, season, p_throws)
                probs["_batter_chase"] = batter_feats.get("bat_chase_rate", _BASE_CHASE_RATE)
                probs["_batter_zone_swing"] = batter_feats.get("bat_zone_swing_rate", _BASE_ZONE_SWING_RATE)
                probs["_batter_whiff"] = batter_feats.get("bat_whiff_rate", 0.24)
            except Exception:
                pass
            return probs
    except Exception as e:
        log.warning("Matchup prediction failed for %s vs %s: %s", batter_id, pitcher_id, e)

    # Fallback: league average probabilities
    return {
        "1B": 0.155, "2B": 0.045, "3B": 0.005, "HR": 0.035,
        "BB": 0.082, "HBP": 0.01, "IBB": 0.003, "K": 0.225, "OUT": 0.44,
        "_batter_chase": _BASE_CHASE_RATE,
        "_batter_zone_swing": _BASE_ZONE_SWING_RATE,
        "_batter_whiff": 0.24,
    }


def _zone_intent(balls: int, strikes: int, pitcher_info: dict, rng: random.Random) -> bool:
    """Decide whether the pitcher intends to throw in the zone."""
    # Base zone rate from pitcher's profile
    base_rate = 0.45
    for a in pitcher_info.get("arsenal", []):
        zone_r = a.get("zone_rate")
        if zone_r:
            base_rate = max(base_rate, zone_r * a.get("usage", 0.2) + base_rate * (1 - a.get("usage", 0.2)))
            break

    # Count-based adjustments
    if balls == 3 and strikes < 2:
        base_rate += 0.25  # Must throw strikes
    elif balls == 3 and strikes == 2:
        base_rate += 0.10  # Full count — careful
    elif strikes == 2 and balls < 2:
        base_rate -= 0.15  # Can waste pitches
    elif balls == 0 and strikes == 0:
        base_rate += 0.10  # First pitch strike tendency
    elif balls >= 2 and strikes == 0:
        base_rate += 0.20  # Behind in count

    base_rate = max(0.2, min(0.85, base_rate))
    return rng.random() < base_rate


def _should_change_pitcher(pitcher_info: dict, inning: int, outs: int, bullpen: list) -> bool:
    """Determine if an automatic pitching change should occur."""
    if not bullpen:
        return False

    pc = pitcher_info.get("pitch_count", 0)
    is_starter = pitcher_info.get("is_starter", False)

    if is_starter:
        # Starters pulled around 90-110 pitches
        if pc >= 100:
            return True
        if pc >= 85 and inning >= 7:
            return True
        if pc >= 75 and inning >= 8:
            return True
    else:
        # Relievers usually go 1-2 innings (~25-40 pitches)
        if pc >= 35:
            return True
        if pc >= 25 and outs == 0:  # clean inning break
            return True

    return False


def _default_arsenal(p_throws: str) -> list:
    """Create a generic arsenal for pitchers with no data."""
    return [
        {"pitch_type": "FF", "usage": 0.45, "avg_velo": 94.0, "avg_spin": 2300,
         "avg_hb": 8, "avg_ivb": 15, "whiff_rate": 0.20, "zone_rate": 0.50, "chase_rate": 0.25},
        {"pitch_type": "SL", "usage": 0.25, "avg_velo": 85.0, "avg_spin": 2500,
         "avg_hb": -3, "avg_ivb": 1, "whiff_rate": 0.32, "zone_rate": 0.40, "chase_rate": 0.35},
        {"pitch_type": "CH", "usage": 0.15, "avg_velo": 85.0, "avg_spin": 1700,
         "avg_hb": 14, "avg_ivb": 5, "whiff_rate": 0.30, "zone_rate": 0.42, "chase_rate": 0.33},
        {"pitch_type": "CU", "usage": 0.15, "avg_velo": 79.0, "avg_spin": 2700,
         "avg_hb": -5, "avg_ivb": -5, "whiff_rate": 0.28, "zone_rate": 0.38, "chase_rate": 0.30},
    ]


_PITCH_NAMES = {
    "FF": "4-Seam Fastball", "SI": "Sinker", "FC": "Cutter",
    "SL": "Slider", "CU": "Curveball", "CH": "Changeup",
    "FS": "Splitter", "KC": "Knuckle Curve", "ST": "Sweeper",
    "SV": "Slurve", "KN": "Knuckleball",
}

def _pitch_name(pitch_type: str) -> str:
    return _PITCH_NAMES.get(pitch_type, pitch_type)


# ---------------------------------------------------------------------------
# State builder — converts raw game log into gamecast-compatible JSON at pitch N
# ---------------------------------------------------------------------------

def build_state_at_pitch(game_data: dict, through: int) -> dict:
    """
    Build a gamecast-compatible state dict showing the game through pitch N.
    This mirrors the normalize_gamecast() output so the frontend can reuse
    Game Caster rendering functions.
    """
    pitches_all = game_data["pitches"]
    through = max(0, min(through, len(pitches_all) - 1))

    # Replay game state up to this pitch
    score = {"away": 0, "home": 0}
    current_inning = 1
    current_half = "Top"

    # Find current PA pitches
    current_pitch = pitches_all[through]
    current_batter_id = current_pitch["batter_id"]
    current_pitcher_id = current_pitch["pitcher_id"]

    # Collect pitches in current PA
    pa_pitches = []
    for i in range(through, -1, -1):
        p = pitches_all[i]
        if p["batter_id"] == current_batter_id and p["inning"] == current_pitch["inning"] and p["half"] == current_pitch["half"]:
            pa_pitches.insert(0, p)
        else:
            break

    # Format pitches for gamecast
    formatted_pitches = []
    for i, p in enumerate(pa_pitches):
        formatted_pitches.append({
            "n": i + 1,
            "px": p["px"],
            "pz": p["pz"],
            "sz_top": p["sz_top"],
            "sz_bot": p["sz_bot"],
            "pitchType": p["pitch_name"],
            "mph": p["mph"],
            "spinRate": p["spin"],
            "vertMove": p["ivb"],
            "horizMove": p["hb"],
            "isBall": p["is_ball"],
            "isStrike": p["is_strike"],
            "isFoul": p["is_foul"],
            "isInPlay": p["is_in_play"],
            "call": _result_to_call(p["result"]),
            "desc": _result_to_desc(p["result"], p.get("event")),
        })

    # Compute score through this pitch
    score = {"away": current_pitch["score_away"], "home": current_pitch["score_home"]}

    # Build runners from current pitch state
    runners_raw = current_pitch.get("runners", [False, False, False])

    # Count current balls/strikes/outs
    balls = current_pitch["balls"]
    strikes = current_pitch["strikes"]
    outs_val = current_pitch["outs"]

    # If the pitch ended the PA, update count
    if current_pitch.get("event"):
        if current_pitch["event"] == "K":
            strikes = min(strikes + 1, 3)
        elif current_pitch["event"] == "BB":
            balls = min(balls + 1, 4)
        elif current_pitch["event"] in ("OUT",):
            outs_val = min(outs_val + 1, 3)

    # Build feed of events through this pitch
    feed = []
    for ev in game_data["events"]:
        if ev.get("pitch_index", 999999) <= through:
            feed.append({
                "type": ev["type"],
                "event": ev.get("event", ev["type"]),
                "description": ev.get("description", ""),
                "inning": f"{'Top' if ev['half'] == 'Top' else 'Bot'} {ev['inning']}",
                "isScoring": ev.get("runs_scored", 0) > 0,
            })

    # BIP data for current pitch
    bip = None
    if current_pitch.get("bip"):
        b = current_pitch["bip"]
        bip = {
            "id": f"sim-{through}",
            "has": True,
            "x": b["x"],
            "y": b["y"],
            "event": b["event"],
            "description": b["event"],
            "ev": b["ev"],
            "la": b["la"],
            "dist": b["dist"],
            "is_out": b["is_out"],
            "kind": b["kind"],
        }

    # Get last completed event
    last_play = ""
    for ev in reversed(feed):
        if ev.get("description"):
            last_play = ev["description"]
            break

    # Build lineups with stats through this point
    away_lineup_state = _build_lineup_state(game_data["away_lineup"], game_data["player_stats"], "away", pitches_all, through)
    home_lineup_state = _build_lineup_state(game_data["home_lineup"], game_data["player_stats"], "home", pitches_all, through)

    # Linescore through current inning
    ls_away = []
    ls_home = []
    for i, runs in enumerate(game_data["linescore"]["away"]):
        inn = i + 1
        if inn <= current_pitch["inning"]:
            ls_away.append(runs)
        elif inn == current_pitch["inning"] and current_pitch["half"] == "Top":
            # Current inning in progress
            ls_away.append(score["away"] - sum(ls_away))
    for i, runs in enumerate(game_data["linescore"]["home"]):
        inn = i + 1
        if inn <= current_pitch["inning"]:
            ls_home.append(runs)

    return {
        "ok": True,
        "state": "final" if through >= len(pitches_all) - 1 else "live",
        "detailedState": "Final" if through >= len(pitches_all) - 1 else "Simulating",
        "inning": current_pitch["inning"],
        "half": current_pitch["half"],
        "displayHalf": current_pitch["half"][:3],
        "balls": balls,
        "strikes": strikes,
        "outs": outs_val,
        "score": score,
        "lastPlay": last_play,
        "batter": {
            "id": current_pitch["batter_id"],
            "name": current_pitch["batter_name"],
            "shortName": _short_name(current_pitch["batter_name"]),
            "stand": current_pitch.get("batter_stand", "R"),
        },
        "pitcher": {
            "id": current_pitch["pitcher_id"],
            "name": current_pitch["pitcher_name"],
            "shortName": _short_name(current_pitch["pitcher_name"]),
            "throws": current_pitch.get("pitcher_throws", "R"),
        },
        "runners": {
            "first": {"id": 0, "name": "Runner"} if runners_raw[0] else None,
            "second": {"id": 0, "name": "Runner"} if runners_raw[1] else None,
            "third": {"id": 0, "name": "Runner"} if runners_raw[2] else None,
        },
        "pitches": formatted_pitches,
        "lineups": {
            "away": away_lineup_state,
            "home": home_lineup_state,
        },
        "feed": feed,
        "bip": bip,
        "linescore": {"away": ls_away, "home": ls_home},
        "venue": game_data["venue"],
        "stadiumSvg": game_data["stadium_svg"],
        # Simulation metadata
        "totalPitches": game_data["total_pitches"],
        "currentPitchIndex": through,
        "finalScore": game_data["score"],
        "pitchingChanges": game_data["pitching_changes"],
    }


def _build_lineup_state(lineup, player_stats, team, pitches_all, through):
    """Build lineup display with stats accumulated through pitch N."""
    result = []
    # Compute stats through the current pitch
    stats_through = {}
    for p in pitches_all[:through + 1]:
        bid = p["batter_id"]
        if bid not in stats_through:
            stats_through[bid] = {"pa": 0, "ab": 0, "h": 0, "bb": 0, "k": 0, "hr": 0, "rbi": 0}
        if p.get("event"):
            stats_through[bid]["pa"] += 1
            ev = p["event"]
            if ev in ("1B", "2B", "3B", "HR"):
                stats_through[bid]["h"] += 1
                stats_through[bid]["ab"] += 1
                if ev == "HR":
                    stats_through[bid]["hr"] += 1
            elif ev == "K":
                stats_through[bid]["k"] += 1
                stats_through[bid]["ab"] += 1
            elif ev == "BB":
                stats_through[bid]["bb"] += 1
            elif ev == "OUT":
                stats_through[bid]["ab"] += 1

    for i, b in enumerate(lineup):
        s = stats_through.get(b["id"], {"pa": 0, "ab": 0, "h": 0, "bb": 0, "k": 0})
        ab = s["ab"]
        h = s["h"]
        bat_line = f"{h}-{ab}" if ab > 0 else "—"

        result.append({
            "id": b["id"],
            "name": b["name"],
            "shortName": _short_name(b["name"]),
            "pos": b.get("pos", "DH"),
            "spot": i + 1,
            "batLine": bat_line,
        })
    return result


def _short_name(full_name: str) -> str:
    """Convert 'First Last' to 'F. Last'."""
    parts = (full_name or "").split()
    if len(parts) >= 2:
        return f"{parts[0][0]}. {parts[-1]}"
    return full_name or "?"


def _result_to_call(result: str) -> str:
    mapping = {
        "ball": "Ball",
        "called_strike": "Called Strike",
        "swinging_strike": "Swinging Strike",
        "foul": "Foul",
        "in_play": "In Play",
    }
    return mapping.get(result, result)


def _result_to_desc(result: str, event: str = None) -> str:
    if event:
        return f"In play — {event}"
    mapping = {
        "ball": "Ball",
        "called_strike": "Called Strike",
        "swinging_strike": "Swinging Strike",
        "foul": "Foul Ball",
    }
    return mapping.get(result, result)
