#!/usr/bin/env python3
"""
Generate 2026 player stat projections — team-first methodology.

Philosophy:
  Every projection lives within a team context. Playing time is allocated
  based on roster role, then rate stats are projected using Marcel-style
  weighted averages. Counting stats derive from allocated PA/IP × rates.
  Team totals are validated against season win projections.

Team-level constraints (per 162 games):
  - ~6,100 total PA per team
  - ~1,458 total IP per team (162 × 9)
  - Pitcher wins sum to team projected wins

Player rate stats use Marcel methodology:
  - 3-year weighted average (5/4/3, most recent heaviest)
  - Regression toward league mean based on sample size
  - Age adjustments from MLB API birth dates

Usage:
  python scripts/generate_player_projections.py [--season 2026]
"""

import os
import sys
import json
import time
import argparse
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
API_BASE = "https://statsapi.mlb.com/api/v1"

MARCEL_WEIGHTS = {0: 5, 1: 4, 2: 3}

# League average rates for regression
LG_AVG_BATTER = {
    "k_pct": 0.224, "bb_pct": 0.082, "whiff_rate": 0.248,
    "chase_rate": 0.295, "avg_ev": 88.5, "barrel_rate": 0.070,
    "hard_hit_rate": 0.370, "iso": 0.155, "babip": 0.295, "xwoba": 0.315,
    "hr_per_fb": 0.120, "fb_rate": 0.355, "gb_rate": 0.430,
}

LG_AVG_PITCHER = {
    "whiff_rate": 0.248, "zone_rate": 0.450, "chase_rate": 0.295,
    "csw_rate": 0.290, "xwoba": 0.315,
}

BATTER_REGRESSION_PA = 400
PITCHER_REGRESSION_PITCHES = 3000

# Team-level constants
TEAM_PA_PER_SEASON = 6100       # ~37.65 PA/game × 162
TEAM_IP_PER_SEASON = 1458.0     # 162 × 9
TEAM_GAMES = 162

# PA allocation by lineup role
PA_ALLOCATION = {
    "everyday": 620,      # everyday starter (slots 1-8)
    "primary_dh": 580,    # primary DH
    "platoon": 400,       # platoon / semi-regular
    "bench_bat": 250,     # backup infielder/outfielder
    "backup_c": 200,      # backup catcher
    "extra": 150,         # extra man / September callup
}

# IP allocation by pitching role
IP_ALLOCATION = {
    "sp1": 190, "sp2": 180, "sp3": 170, "sp4": 155, "sp5": 140,
    "closer": 65, "setup1": 65, "setup2": 60,
    "mid1": 60, "mid2": 55, "mid3": 50, "mid4": 45,
    "long": 55, "mop": 40, "extra_rp": 30,
}

TEAM_IDS = [
    108, 109, 110, 111, 112, 113, 114, 115, 116, 117,
    118, 119, 120, 121, 133, 134, 135, 136, 137, 138,
    139, 140, 141, 142, 143, 144, 145, 146, 147, 158,
]

TEAM_ABBREVS = {
    108: "LAA", 109: "ARI", 110: "BAL", 111: "BOS", 112: "CHC",
    113: "CIN", 114: "CLE", 115: "COL", 116: "DET", 117: "HOU",
    118: "KC",  119: "LAD", 120: "WSH", 121: "NYM", 133: "OAK",
    134: "PIT", 135: "SD",  136: "SEA", 137: "SF",  138: "STL",
    139: "TB",  140: "TEX", 141: "TOR", 142: "MIN", 143: "PHI",
    144: "ATL", 145: "CWS", 146: "MIA", 147: "NYY", 158: "MIL",
}


# ────────────────────────────────────────────────────────
# Data loading
# ────────────────────────────────────────────────────────

def fetch_full_roster(team_id, season):
    """Fetch full depth chart roster from MLB API. Returns list of dicts."""
    url = f"{API_BASE}/teams/{team_id}/roster"
    for roster_type in ["depthChart", "40Man"]:
        for yr in [season, season - 1]:
            try:
                resp = requests.get(url, params={"rosterType": roster_type, "season": yr}, timeout=30)
                resp.raise_for_status()
                roster = resp.json().get("roster", [])
                if roster:
                    seen = set()
                    result = []
                    for entry in roster:
                        pid = entry["person"]["id"]
                        if pid not in seen:
                            seen.add(pid)
                            result.append({
                                "id": pid,
                                "name": entry["person"]["fullName"],
                                "pos_type": entry["position"]["type"],
                                "pos": entry["position"]["abbreviation"],
                            })
                    return result
            except Exception:
                continue
    return []


def fetch_player_info(player_ids):
    """Fetch birth dates and positions from MLB API."""
    info = {}
    batch_size = 50
    ids_list = list(player_ids)

    for i in range(0, len(ids_list), batch_size):
        batch = ids_list[i:i + batch_size]
        ids_str = ",".join(str(pid) for pid in batch)
        try:
            url = f"{API_BASE}/people?personIds={ids_str}"
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            for person in resp.json().get("people", []):
                pid = person["id"]
                bd = person.get("birthDate", "")
                age = None
                if bd:
                    age = int(str(datetime.now().year + (1 if datetime.now().month < 4 else 0))) - int(bd[:4])
                info[pid] = {
                    "age": age,
                    "primary_pos": person.get("primaryPosition", {}).get("abbreviation", ""),
                    "bat_side": person.get("batSide", {}).get("code", ""),
                }
        except Exception:
            pass
    return info


def load_player_data():
    """Load parquet datasets."""
    bp_path = os.path.join(DATA_DIR, "batter_profiles.parquet")
    pa_path = os.path.join(DATA_DIR, "pitcher_arsenal.parquet")
    pw_path = os.path.join(DATA_DIR, "pitcher_workload.parquet")

    bp = pd.read_parquet(bp_path) if os.path.exists(bp_path) else pd.DataFrame()
    pa = pd.read_parquet(pa_path) if os.path.exists(pa_path) else pd.DataFrame()
    pw = pd.read_parquet(pw_path) if os.path.exists(pw_path) else pd.DataFrame()

    return bp, pa, pw


def load_team_wins():
    """Load projected team wins from season projection."""
    path = os.path.join(DATA_DIR, "season_projection_2026.json")
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        raw = json.load(f)
    wins = {}
    for div, teams in raw.items():
        if div.startswith("_"):
            continue
        for t in teams:
            wins[t["team_id"]] = t["avg_wins"]
    return wins


# ────────────────────────────────────────────────────────
# Marcel rate projections
# ────────────────────────────────────────────────────────

def age_adjustment(age, is_pitcher=False):
    """Age-based performance multiplier. Peak ~27 (P) or ~28 (H)."""
    if age is None:
        return 1.0
    peak = 27 if is_pitcher else 28
    if age <= peak:
        return 1.0
    decline = 0.005 if is_pitcher else 0.004
    return max(0.85, 1.0 - decline * (age - peak))


def marcel_batter_rates(player_id, bp_df, age=None):
    """Project rate stats for a batter using Marcel methodology.
    Returns dict of projected rate stats, or None if no data."""
    player_data = bp_df[(bp_df["batter"] == player_id) & (bp_df["vs_hand"] == "ALL")]
    if player_data.empty:
        return None

    seasons = sorted(player_data["season"].unique(), reverse=True)
    rate_cols = ["k_pct", "bb_pct", "whiff_rate", "chase_rate", "avg_ev",
                 "barrel_rate", "hard_hit_rate", "iso", "babip", "xwoba",
                 "hr_per_fb", "fb_rate", "gb_rate"]

    weighted_stats = {col: 0.0 for col in rate_cols}
    total_weight = 0.0
    career_pa = 0

    for idx, season in enumerate(seasons[:3]):
        weight = MARCEL_WEIGHTS.get(idx, 2)
        row = player_data[player_data["season"] == season].iloc[0]
        pa = int(row.get("pa", 0) or 0)
        career_pa += pa

        for col in rate_cols:
            val = float(row.get(col, 0) or 0)
            if val > 0 or col in ("iso", "babip", "xwoba"):
                weighted_stats[col] += val * weight * pa
            else:
                weighted_stats[col] += LG_AVG_BATTER.get(col, 0) * weight * pa
        total_weight += weight * pa

    if total_weight == 0:
        return None

    projected = {}
    for col in rate_cols:
        raw = weighted_stats[col] / total_weight
        lg_avg = LG_AVG_BATTER.get(col, raw)
        reliability = min(1.0, total_weight / (BATTER_REGRESSION_PA * sum(MARCEL_WEIGHTS.values())))
        projected[col] = raw * reliability + lg_avg * (1 - reliability)

    # Age adjustment
    if age:
        adj = age_adjustment(age)
        projected["iso"] *= adj
        projected["xwoba"] = projected["xwoba"] * adj + LG_AVG_BATTER["xwoba"] * (1 - adj)
        projected["barrel_rate"] *= adj

    projected["_career_pa"] = career_pa
    return projected


def marcel_pitcher_rates(player_id, pa_df, age=None):
    """Project rate stats for a pitcher. Returns dict or None."""
    player_data = pa_df[(pa_df["pitcher"] == player_id) & (pa_df["stand"] == "ALL")]
    if player_data.empty:
        return None

    seasons = sorted(player_data["season"].unique(), reverse=True)
    rate_cols = ["whiff_rate", "zone_rate", "chase_rate", "csw_rate", "xwoba"]
    weighted_stats = {col: 0.0 for col in rate_cols}
    weighted_velo = 0.0
    weighted_stuff = 0.0
    weighted_ctrl = 0.0
    total_weight = 0.0
    career_pitches = 0

    for idx, season in enumerate(seasons[:3]):
        weight = MARCEL_WEIGHTS.get(idx, 2)
        season_data = player_data[player_data["season"] == season]
        n_pitches = int(season_data["n"].sum())
        career_pitches += n_pitches

        valid = season_data.dropna(subset=["xwoba"])
        if valid.empty or valid["n"].sum() == 0:
            continue
        season_n = valid["n"].sum()

        for col in rate_cols:
            col_vals = valid[col].fillna(LG_AVG_PITCHER.get(col, 0))
            season_avg = (col_vals * valid["n"]).sum() / season_n
            weighted_stats[col] += season_avg * weight * season_n

        velo_valid = valid.dropna(subset=["avg_velo"])
        if not velo_valid.empty:
            v = (velo_valid["avg_velo"] * velo_valid["n"]).sum() / velo_valid["n"].sum()
            weighted_velo += v * weight * season_n

        sp = valid["avg_stuff_plus"].dropna()
        if not sp.empty:
            s = (sp.astype(float) * valid.loc[sp.index, "n"]).sum() / valid.loc[sp.index, "n"].sum()
            weighted_stuff += s * weight * season_n
        else:
            weighted_stuff += 100.0 * weight * season_n

        cp = valid["avg_control_plus"].dropna()
        if not cp.empty:
            c = (cp.astype(float) * valid.loc[cp.index, "n"]).sum() / valid.loc[cp.index, "n"].sum()
            weighted_ctrl += c * weight * season_n
        else:
            weighted_ctrl += 100.0 * weight * season_n

        total_weight += weight * season_n

    if total_weight == 0:
        return None

    projected = {}
    for col in rate_cols:
        raw = weighted_stats[col] / total_weight
        lg_avg = LG_AVG_PITCHER.get(col, raw)
        reliability = min(1.0, total_weight / (PITCHER_REGRESSION_PITCHES * sum(MARCEL_WEIGHTS.values())))
        projected[col] = raw * reliability + lg_avg * (1 - reliability)

    projected["velo"] = weighted_velo / total_weight if total_weight > 0 else 93.0
    projected["stuff_plus"] = weighted_stuff / total_weight if total_weight > 0 else 100.0
    projected["control_plus"] = weighted_ctrl / total_weight if total_weight > 0 else 100.0

    if age:
        adj = age_adjustment(age, is_pitcher=True)
        if adj < 1.0 and adj > 0:
            projected["xwoba"] = projected["xwoba"] / adj + LG_AVG_PITCHER["xwoba"] * (1 - 1/adj)
            projected["velo"] *= (1 - (1 - adj) * 0.5)

    projected["_career_pitches"] = career_pitches
    return projected


# ────────────────────────────────────────────────────────
# Counting stats from rates + allocated playing time
# ────────────────────────────────────────────────────────

def batter_counting_stats(rates, pa):
    """Derive counting stats from rate stats and allocated PA."""
    ab = int(pa * (1 - rates["bb_pct"] - 0.012))

    # AVG from components
    hr_ab_rate = rates["fb_rate"] * rates["hr_per_fb"] * (1 - rates["k_pct"])
    avg = rates["babip"] * (1 - rates["k_pct"]) * (1 - hr_ab_rate) + hr_ab_rate
    avg = max(0.170, min(0.340, avg))

    hits = int(ab * avg)
    hr = max(0, int(ab * hr_ab_rate))
    doubles = max(0, int((hits - hr) * 0.24))
    triples = max(0, int((hits - hr) * 0.025))
    singles = max(0, hits - hr - doubles - triples)

    # RBI/R using realistic per-event values
    # HR: batter scores + ~0.5 avg runners on base = ~1.5 RBI per HR
    # non-HR hits: ~0.12 RBI each
    # BB: ~0.04 RBI each (bases loaded walks, etc.)
    bb = int(pa * rates["bb_pct"])
    rbi = max(0, int(hr * 1.50 + (hits - hr) * 0.12 + bb * 0.04 + pa * 0.005))
    runs = max(0, int(hr * 0.65 + (hits - hr) * 0.27 + bb * 0.20 + pa * 0.01))

    slg = avg + rates["iso"]
    obp = max(avg, min(0.500, avg + rates["bb_pct"] * 0.85 + 0.010))

    return {
        "pa": pa,
        "ab": ab,
        "h": hits,
        "hr": hr,
        "2b": doubles,
        "3b": triples,
        "rbi": rbi,
        "r": runs,
        "avg": round(avg, 3),
        "obp": round(obp, 3),
        "slg": round(slg, 3),
        "ops": round(obp + slg, 3),
        "k_pct": round(rates["k_pct"], 3),
        "bb_pct": round(rates["bb_pct"], 3),
        "iso": round(rates["iso"], 3),
        "babip": round(rates["babip"], 3),
        "xwoba": round(rates["xwoba"], 3),
        "barrel_rate": round(rates.get("barrel_rate", 0.07), 3),
        "hard_hit_rate": round(rates.get("hard_hit_rate", 0.37), 3),
        "avg_ev": round(rates.get("avg_ev", 88.5), 1),
        "whiff_rate": round(rates.get("whiff_rate", 0.248), 3),
        "chase_rate": round(rates.get("chase_rate", 0.295), 3),
    }


def pitcher_counting_stats(rates, ip, team_wins, team_losses, team_ip_total):
    """Derive counting stats from rate stats and allocated IP."""
    era = max(2.00, min(7.00, -6.0 + 33.0 * rates["xwoba"]))

    k_pct = min(0.40, rates["whiff_rate"] * 0.90)
    k_per_9 = max(4.0, min(14.0, k_pct * 27.0))
    bb_pct = max(0.04, 0.22 - 0.33 * rates["zone_rate"])
    bb_per_9 = max(1.5, min(6.0, bb_pct * 27.0))
    whip = max(0.80, min(2.00, 0.72 + era * 0.145))

    k_total = max(0, int(ip * k_per_9 / 9))

    # W/L: distribute team wins proportionally by IP, adjusted by ERA
    # Better ERA = higher share of wins, lower share of losses
    ip_share = ip / team_ip_total if team_ip_total > 0 else 0
    era_factor = max(0.4, min(1.6, (4.50 / max(2.0, era))))  # >1 for good ERA, <1 for bad
    raw_wins = team_wins * ip_share * era_factor
    raw_losses = team_losses * ip_share * (1 / era_factor)

    # Pitchers only get decisions in ~55% of their games
    decision_rate = 0.55 if ip > 80 else 0.30  # starters vs relievers
    wins = max(0, int(raw_wins * decision_rate))
    losses = max(0, int(raw_losses * decision_rate))

    return {
        "ip": round(ip, 1),
        "era": round(era, 2),
        "w": wins,
        "l": losses,
        "k": k_total,
        "k_per_9": round(k_per_9, 1),
        "bb_per_9": round(bb_per_9, 1),
        "whip": round(whip, 2),
        "xwoba": round(rates["xwoba"], 3),
        "whiff_rate": round(rates["whiff_rate"], 3),
        "chase_rate": round(rates["chase_rate"], 3),
        "zone_rate": round(rates["zone_rate"], 3),
        "csw_rate": round(rates.get("csw_rate", 0.290), 3),
        "velo": round(rates.get("velo", 93.0), 1),
        "stuff_plus": round(rates.get("stuff_plus", 100.0), 1),
        "control_plus": round(rates.get("control_plus", 100.0), 1),
    }


# ────────────────────────────────────────────────────────
# Playing time allocation
# ────────────────────────────────────────────────────────

def is_starter_role(pw_df, pitcher_id):
    """Check if pitcher is primarily a starter from workload data."""
    if pw_df.empty:
        return False
    pw = pw_df[pw_df["pitcher"] == pitcher_id]
    if pw.empty:
        return False
    latest = pw["season"].max()
    return pw[pw["season"] == latest]["is_starter"].mean() > 0.5


def get_pitcher_season_ip(pw_df, pitcher_id):
    """Get average season IP from workload history (last 2 years)."""
    if pw_df.empty:
        return None
    pw = pw_df[pw_df["pitcher"] == pitcher_id]
    if pw.empty:
        return None
    # Aggregate IP by season, take mean of last 2
    by_season = pw.groupby("season")["ip"].sum()
    recent = by_season.sort_index(ascending=False).head(2)
    if recent.empty:
        return None
    return float(recent.mean())


def allocate_hitter_pa(hitters, bp_df, player_info):
    """Allocate PA to hitters based on roster position and quality.
    Returns list of (player_dict, allocated_pa) tuples."""

    # Score each hitter for lineup ordering
    scored = []
    for h in hitters:
        pid = h["id"]
        info = player_info.get(pid, {})
        pos = info.get("primary_pos", h.get("pos", ""))

        # Get their projected quality (xwoba from rates or default)
        player_data = bp_df[(bp_df["batter"] == pid) & (bp_df["vs_hand"] == "ALL")]
        if not player_data.empty:
            latest = player_data.sort_values("season", ascending=False).iloc[0]
            xwoba = float(latest.get("xwoba", 0) or 0)
            recent_pa = int(latest.get("pa", 0) or 0)
        else:
            xwoba = 0.280
            recent_pa = 0

        scored.append({
            **h,
            "xwoba": xwoba if xwoba > 0.100 else 0.280,
            "recent_pa": recent_pa,
            "primary_pos": pos,
        })

    # Sort by quality (xwoba) descending
    scored.sort(key=lambda x: x["xwoba"], reverse=True)

    allocations = []
    total_pa = 0
    positions_filled = set()

    for i, h in enumerate(scored):
        pos = h["primary_pos"]

        if i < 8:
            # Top 8 hitters are everyday starters
            pa = PA_ALLOCATION["everyday"]
        elif i == 8:
            # 9th hitter (DH or weaker starter)
            pa = PA_ALLOCATION["primary_dh"]
        elif i < 11:
            # Platoon / semi-regular
            pa = PA_ALLOCATION["platoon"]
        elif pos == "C" and i >= 8:
            # Backup catcher
            pa = PA_ALLOCATION["backup_c"]
        elif i < 14:
            # Bench bats
            pa = PA_ALLOCATION["bench_bat"]
        else:
            # Extra men
            pa = PA_ALLOCATION["extra"]

        allocations.append((h, pa))
        total_pa += pa

    # Scale to hit exactly TEAM_PA_PER_SEASON
    if total_pa > 0:
        scale = TEAM_PA_PER_SEASON / total_pa
        allocations = [(h, max(50, int(pa * scale))) for h, pa in allocations]

        # Fine-tune: adjust top hitters to absorb rounding errors
        current_total = sum(pa for _, pa in allocations)
        diff = TEAM_PA_PER_SEASON - current_total
        if diff != 0 and allocations:
            h, pa = allocations[0]
            allocations[0] = (h, pa + diff)

    return allocations


def allocate_pitcher_ip(pitchers, pw_df, player_info):
    """Allocate IP to pitchers based on role.
    Returns list of (player_dict, allocated_ip, role) tuples."""

    # Classify each pitcher
    starters = []
    relievers = []

    for p in pitchers:
        pid = p["id"]
        is_sp = is_starter_role(pw_df, pid)
        historical_ip = get_pitcher_season_ip(pw_df, pid)

        p_scored = {
            **p,
            "is_sp": is_sp,
            "historical_ip": historical_ip,
        }

        # Get quality from arsenal data for sorting
        # (loaded in caller, passed through p dict)

        if is_sp:
            starters.append(p_scored)
        else:
            relievers.append(p_scored)

    # If not enough starters detected, promote best relievers
    while len(starters) < 5 and relievers:
        # Pick reliever with most historical IP
        relievers.sort(key=lambda x: x.get("historical_ip") or 0, reverse=True)
        promoted = relievers.pop(0)
        promoted["is_sp"] = True
        starters.append(promoted)

    # Sort starters: use historical IP as proxy for role (SP1 has most IP)
    starters.sort(key=lambda x: x.get("historical_ip") or 100, reverse=True)

    # Sort relievers by quality (we'll get xwoba from rates later, for now use historical)
    relievers.sort(key=lambda x: x.get("historical_ip") or 30, reverse=True)

    allocations = []
    sp_roles = ["sp1", "sp2", "sp3", "sp4", "sp5"]
    rp_roles = ["closer", "setup1", "setup2", "mid1", "mid2", "mid3", "mid4", "long", "mop", "extra_rp"]

    # Assign starter IP
    for i, sp in enumerate(starters[:5]):
        role = sp_roles[i]
        base_ip = IP_ALLOCATION[role]

        # Adjust based on historical IP if available
        hist = sp.get("historical_ip")
        if hist and hist > 0:
            # Blend: 60% template, 40% historical
            ip = base_ip * 0.6 + min(220, hist) * 0.4
        else:
            ip = base_ip * 0.85  # discount unproven starters

        allocations.append((sp, ip, "SP"))

    # Assign reliever IP
    total_sp_ip = sum(ip for _, ip, _ in allocations)
    remaining_ip = TEAM_IP_PER_SEASON - total_sp_ip

    for i, rp in enumerate(relievers):
        if i < len(rp_roles):
            role = rp_roles[i]
            base_ip = IP_ALLOCATION[role]
        else:
            base_ip = 25  # extra arms

        # Scale to fill remaining IP
        allocations.append((rp, base_ip, "RP"))

    # Scale reliever IP to fill exactly remaining innings
    sp_count = min(5, len(starters))
    rp_allocations = allocations[sp_count:]
    rp_total = sum(ip for _, ip, _ in rp_allocations)

    if rp_total > 0:
        scale = remaining_ip / rp_total
        scaled_rp = [(p, max(15, ip * scale), role) for p, ip, role in rp_allocations]
        allocations = allocations[:sp_count] + scaled_rp

    # Final scaling to hit exactly TEAM_IP_PER_SEASON
    current_total = sum(ip for _, ip, _ in allocations)
    if current_total > 0:
        final_scale = TEAM_IP_PER_SEASON / current_total
        allocations = [(p, ip * final_scale, role) for p, ip, role in allocations]

    return allocations


# ────────────────────────────────────────────────────────
# Main projection engine
# ────────────────────────────────────────────────────────

def generate_projections(season=2026):
    """Generate team-context player projections."""
    print(f"Generating {season} player projections (team-first methodology)")
    print("=" * 60)

    # Load data
    print("\nLoading data...")
    bp_df, pa_df, pw_df = load_player_data()
    team_wins_proj = load_team_wins()
    print(f"  Batter profiles: {bp_df['batter'].nunique()} players")
    print(f"  Pitcher arsenal: {pa_df['pitcher'].nunique()} players")
    print(f"  Pitcher workload: {pw_df['pitcher'].nunique()} players")
    print(f"  Team win projections: {len(team_wins_proj)} teams")

    # Fetch all rosters
    print("\nFetching rosters from MLB API...")
    all_rosters = {}
    all_player_ids = set()
    for tid in TEAM_IDS:
        roster = fetch_full_roster(tid, season)
        all_rosters[tid] = roster
        for p in roster:
            all_player_ids.add(p["id"])
        time.sleep(0.05)
    print(f"  Total players across all teams: {len(all_player_ids)}")

    # Fetch player info (ages, positions)
    print("Fetching player info...")
    player_info = fetch_player_info(all_player_ids)
    print(f"  Got info for {len(player_info)} players")

    # Process each team
    all_batters = []
    all_pitchers = []

    for tid in TEAM_IDS:
        abbrev = TEAM_ABBREVS.get(tid, str(tid))
        roster = all_rosters[tid]
        proj_wins = team_wins_proj.get(tid, 81)
        proj_losses = TEAM_GAMES - proj_wins

        if not roster:
            print(f"  {abbrev}: No roster data, skipping")
            continue

        # Split roster into hitters and pitchers
        hitters = [p for p in roster if p["pos_type"] != "Pitcher"]
        pitchers = [p for p in roster if p["pos_type"] == "Pitcher"]

        # ── Hitter projections ──
        hitter_allocs = allocate_hitter_pa(hitters, bp_df, player_info)

        team_rbi_total = 0
        team_runs_total = 0
        team_hr_total = 0

        for h, pa_alloc in hitter_allocs:
            pid = h["id"]
            info = player_info.get(pid, {})
            rates = marcel_batter_rates(pid, bp_df, age=info.get("age"))

            if rates is None:
                # Use league average defaults for unproven players
                rates = dict(LG_AVG_BATTER)
                rates["_career_pa"] = 0

            stats = batter_counting_stats(rates, pa_alloc)
            stats["player_id"] = pid
            stats["name"] = h["name"]
            stats["team"] = abbrev
            stats["team_id"] = tid
            all_batters.append(stats)

            team_rbi_total += stats["rbi"]
            team_runs_total += stats["r"]
            team_hr_total += stats["hr"]

        # ── Pitcher projections ──
        pitcher_allocs = allocate_pitcher_ip(pitchers, pw_df, player_info)
        team_ip_total = sum(ip for _, ip, _ in pitcher_allocs)

        for p, ip_alloc, role in pitcher_allocs:
            pid = p["id"]
            info = player_info.get(pid, {})
            rates = marcel_pitcher_rates(pid, pa_df, age=info.get("age"))

            if rates is None:
                rates = dict(LG_AVG_PITCHER)
                rates["velo"] = 94.0 if role == "RP" else 93.0
                rates["stuff_plus"] = 100.0
                rates["control_plus"] = 100.0
                rates["_career_pitches"] = 0

            stats = pitcher_counting_stats(rates, ip_alloc, proj_wins, proj_losses, team_ip_total)
            stats["player_id"] = pid
            stats["name"] = p["name"]
            stats["team"] = abbrev
            stats["team_id"] = tid
            stats["role"] = role
            all_pitchers.append(stats)

        # Team summary
        team_pitcher_wins = sum(p["w"] for p in all_pitchers if p["team"] == abbrev)
        team_pitcher_losses = sum(p["l"] for p in all_pitchers if p["team"] == abbrev)
        n_hitters = len([b for b in all_batters if b["team"] == abbrev])
        n_pitchers = len([p for p in all_pitchers if p["team"] == abbrev])

        print(f"  {abbrev}: {n_hitters}H + {n_pitchers}P | "
              f"PA={sum(pa for _, pa in hitter_allocs):.0f} IP={team_ip_total:.0f} | "
              f"Proj W-L: {proj_wins:.0f}-{proj_losses:.0f} | "
              f"Pitcher W-L: {team_pitcher_wins}-{team_pitcher_losses} | "
              f"Team R: {team_runs_total} HR: {team_hr_total}")

    # Sort results
    all_batters.sort(key=lambda x: x["xwoba"], reverse=True)
    all_pitchers.sort(key=lambda x: x["xwoba"])

    # Build output
    output = {
        "batters": all_batters,
        "pitchers": all_pitchers,
        "_meta": {
            "season": season,
            "method": "Team-context Marcel projection",
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "n_batters": len(all_batters),
            "n_pitchers": len(all_pitchers),
            "n_teams": len(TEAM_IDS),
            "constraints": {
                "pa_per_team": TEAM_PA_PER_SEASON,
                "ip_per_team": TEAM_IP_PER_SEASON,
            },
        }
    }

    out_path = os.path.join(DATA_DIR, f"player_projections_{season}.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, default=float)

    print(f"\n{'=' * 60}")
    print(f"Projections saved to {out_path}")
    print(f"  {len(all_batters)} batters, {len(all_pitchers)} pitchers")

    # Summaries
    print(f"\nTop 15 Batters by xwOBA:")
    print(f"  {'Name':>25}  {'Team':>4}  {'PA':>4}  {'AVG':>5}  {'HR':>3}  {'RBI':>4}  {'OPS':>5}  {'xwOBA':>5}")
    for p in all_batters[:15]:
        print(f"  {p['name']:>25}  {p['team']:>4}  {p['pa']:>4}  {p['avg']:.3f}  {p['hr']:>3}  {p['rbi']:>4}  {p['ops']:.3f}  {p['xwoba']:.3f}")

    print(f"\nTop 15 Pitchers by xwOBA:")
    print(f"  {'Name':>25}  {'Team':>4}  {'Role':>2}  {'IP':>5}  {'ERA':>5}  {'K':>4}  {'W':>2}  {'L':>2}  {'xwOBA':>5}")
    for p in all_pitchers[:15]:
        print(f"  {p['name']:>25}  {p['team']:>4}  {p['role']:>2}  {p['ip']:>5.0f}  {p['era']:>5.2f}  {p['k']:>4}  {p['w']:>2}  {p['l']:>2}  {p['xwoba']:.3f}")

    # Team validation
    print(f"\nTeam Validation (PA / IP / W-L):")
    for tid in sorted(TEAM_IDS, key=lambda t: team_wins_proj.get(t, 81), reverse=True):
        ab = TEAM_ABBREVS.get(tid, str(tid))
        team_b = [b for b in all_batters if b.get("team_id") == tid]
        team_p = [p for p in all_pitchers if p.get("team_id") == tid]
        t_pa = sum(b["pa"] for b in team_b)
        t_ip = sum(p["ip"] for p in team_p)
        t_w = sum(p["w"] for p in team_p)
        t_l = sum(p["l"] for p in team_p)
        t_hr = sum(b["hr"] for b in team_b)
        t_r = sum(b["r"] for b in team_b)
        proj_w = team_wins_proj.get(tid, 81)
        print(f"  {ab:>4}: {len(team_b):>2}H {len(team_p):>2}P | "
              f"PA={t_pa:>5} IP={t_ip:>6.0f} | "
              f"HR={t_hr:>3} R={t_r:>4} | "
              f"PitcherW-L={t_w:>2}-{t_l:<2} (proj {proj_w:.0f})")

    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate player projections")
    parser.add_argument("--season", type=int, default=2026)
    args = parser.parse_args()
    generate_projections(args.season)
