#!/usr/bin/env python3
"""
Generate 2026 player stat projections using Marcel-style methodology.

Uses batter_profiles.parquet and pitcher_arsenal.parquet with:
- 3-year weighted averages (5/4/3 weighting, most recent heaviest)
- Regression toward league mean based on sample size
- Age adjustments (optional, when birth year available)
- Playing time projection from recent PA/IP trends

Outputs: data/player_projections_2026.json
"""

import os
import sys
import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# Marcel weights: most recent season gets highest weight
MARCEL_WEIGHTS = {0: 5, 1: 4, 2: 3}  # 0 = most recent, 1 = year before, etc.
PROJECTION_SEASON = 2026

# League average rates for regression targets
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

# Regression PA/IP thresholds (below this, regress more toward league avg)
BATTER_REGRESSION_PA = 400
PITCHER_REGRESSION_IP_APPROX = 500  # in pitches ~ 500 pitches ≈ 30 IP


def load_rosters():
    """Load team rosters for name/team mapping."""
    path = os.path.join(DATA_DIR, "team_rosters.json")
    with open(path) as f:
        rosters = json.load(f)

    names = {}
    teams = {}
    positions = {}

    for tid, t in rosters.items():
        if tid.startswith("_"):
            continue
        abbrev = t.get("abbrev", "")
        for h in t.get("lineup", []):
            names[h["id"]] = h["name"]
            teams[h["id"]] = abbrev
        for s in t.get("rotation", []):
            names[s["id"]] = s["name"]
            teams[s["id"]] = abbrev
            positions[s["id"]] = "SP"
        for r in t.get("bullpen", []):
            names[r["id"]] = r["name"]
            teams[r["id"]] = abbrev
            positions[r["id"]] = "RP"

    return names, teams, positions


def fetch_player_ages(player_ids):
    """Fetch birth dates from MLB API for age adjustments."""
    ages = {}
    batch_size = 50
    ids_list = list(player_ids)

    for i in range(0, len(ids_list), batch_size):
        batch = ids_list[i:i + batch_size]
        ids_str = ",".join(str(pid) for pid in batch)
        try:
            url = f"https://statsapi.mlb.com/api/v1/people?personIds={ids_str}"
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            for person in resp.json().get("people", []):
                pid = person["id"]
                bd = person.get("birthDate", "")
                if bd:
                    birth_year = int(bd[:4])
                    ages[pid] = PROJECTION_SEASON - birth_year
        except Exception:
            pass

    return ages


def age_adjustment(age, is_pitcher=False):
    """Return a multiplier for age-based performance adjustment.
    Peak age ~27 for pitchers, ~28 for hitters."""
    peak = 27 if is_pitcher else 28
    if age <= peak:
        return 1.0
    decline_per_year = 0.005 if is_pitcher else 0.004
    return max(0.85, 1.0 - decline_per_year * (age - peak))


def marcel_project_batter(player_id, bp_df, roster_batters, age=None):
    """Generate Marcel-style projection for a batter."""
    player_data = bp_df[(bp_df["batter"] == player_id) & (bp_df["vs_hand"] == "ALL")]
    if player_data.empty:
        return None

    seasons = sorted(player_data["season"].unique(), reverse=True)
    if not seasons:
        return None

    most_recent = seasons[0]
    rate_cols = ["k_pct", "bb_pct", "whiff_rate", "chase_rate", "avg_ev",
                 "barrel_rate", "hard_hit_rate", "iso", "babip", "xwoba",
                 "hr_per_fb", "fb_rate", "gb_rate"]

    weighted_stats = {col: 0.0 for col in rate_cols}
    total_weight = 0.0
    total_pa = 0
    pa_samples = []

    for idx, season in enumerate(seasons[:3]):
        weight = MARCEL_WEIGHTS.get(idx, 2)
        row = player_data[player_data["season"] == season].iloc[0]
        pa = int(row.get("pa", 0) or 0)
        pa_samples.append(pa)
        total_pa += pa * weight

        for col in rate_cols:
            val = float(row.get(col, 0) or 0)
            if val > 0 or col in ("iso", "babip", "xwoba"):
                weighted_stats[col] += val * weight * pa
            else:
                weighted_stats[col] += LG_AVG_BATTER.get(col, 0) * weight * pa

        total_weight += weight * pa

    if total_weight == 0:
        return None

    # Compute weighted averages
    projected = {}
    for col in rate_cols:
        raw = weighted_stats[col] / total_weight
        # Regression toward league mean
        lg_avg = LG_AVG_BATTER.get(col, raw)
        reliability = min(1.0, total_weight / (BATTER_REGRESSION_PA * sum(MARCEL_WEIGHTS.values())))
        projected[col] = raw * reliability + lg_avg * (1 - reliability)

    # Age adjustment
    if age:
        adj = age_adjustment(age, is_pitcher=False)
        # Adjust power/contact metrics
        projected["iso"] *= adj
        projected["xwoba"] = projected["xwoba"] * adj + LG_AVG_BATTER["xwoba"] * (1 - adj)
        projected["barrel_rate"] *= adj

    # PA projection: weighted recent PA, regressed
    if pa_samples:
        pa_proj = int(pa_samples[0] * 0.6 + (pa_samples[1] if len(pa_samples) > 1 else pa_samples[0]) * 0.3 +
                      (pa_samples[2] if len(pa_samples) > 2 else pa_samples[0]) * 0.1)
        pa_proj = max(100, min(700, pa_proj))
    else:
        pa_proj = 400

    # Derive counting stats from rate stats
    ab = int(pa_proj * (1 - projected["bb_pct"] - 0.012))  # subtract BB% and ~HBP%

    # AVG from BABIP, K%, and HR rate
    # AVG = BABIP × (1 - K%) × (1 - HR/AB_rate) + HR/AB_rate (approximately)
    hr_ab_rate = projected["fb_rate"] * projected["hr_per_fb"] * (1 - projected["k_pct"])
    avg_est = projected["babip"] * (1 - projected["k_pct"]) * (1 - hr_ab_rate) + hr_ab_rate
    avg_est = max(0.170, min(0.340, avg_est))

    hits = int(ab * avg_est)
    hr = max(1, int(ab * hr_ab_rate))
    doubles = max(0, int((hits - hr) * 0.24))  # ~24% of non-HR hits are doubles
    triples = max(0, int((hits - hr) * 0.025))
    singles = max(0, hits - hr - doubles - triples)
    rbi = max(0, int(hr * 3.2 + (hits - hr) * 0.30 + projected["bb_pct"] * pa_proj * 0.10))
    runs = max(0, int(hr * 1.0 + (hits - hr) * 0.35 + projected["bb_pct"] * pa_proj * 0.30))
    slg = avg_est + projected["iso"]
    obp = max(avg_est, min(0.500, avg_est + projected["bb_pct"] * 0.85 + 0.010))  # rough OBP

    return {
        "player_id": player_id,
        "pa": pa_proj,
        "ab": ab,
        "h": hits,
        "hr": hr,
        "2b": doubles,
        "3b": triples,
        "rbi": rbi,
        "r": runs,
        "avg": round(avg_est, 3),
        "obp": round(obp, 3),
        "slg": round(slg, 3),
        "ops": round(obp + slg, 3),
        "k_pct": round(projected["k_pct"], 3),
        "bb_pct": round(projected["bb_pct"], 3),
        "iso": round(projected["iso"], 3),
        "babip": round(projected["babip"], 3),
        "xwoba": round(projected["xwoba"], 3),
        "barrel_rate": round(projected["barrel_rate"], 3),
        "hard_hit_rate": round(projected["hard_hit_rate"], 3),
        "avg_ev": round(projected["avg_ev"], 1),
        "whiff_rate": round(projected["whiff_rate"], 3),
        "chase_rate": round(projected["chase_rate"], 3),
    }


def marcel_project_pitcher(player_id, pa_df, age=None):
    """Generate Marcel-style projection for a pitcher."""
    # Get aggregate stats (stand=ALL) for the pitcher
    player_data = pa_df[(pa_df["pitcher"] == player_id) & (pa_df["stand"] == "ALL")]
    if player_data.empty:
        return None

    # Group by season to get per-season aggregates
    seasons = sorted(player_data["season"].unique(), reverse=True)
    if not seasons:
        return None

    rate_cols = ["whiff_rate", "zone_rate", "chase_rate", "csw_rate", "xwoba"]
    weighted_stats = {col: 0.0 for col in rate_cols}
    weighted_velo = 0.0
    weighted_stuff = 0.0
    weighted_ctrl = 0.0
    total_weight = 0.0
    total_n = 0
    n_samples = []

    for idx, season in enumerate(seasons[:3]):
        weight = MARCEL_WEIGHTS.get(idx, 2)
        season_data = player_data[player_data["season"] == season]

        # Sum pitches across pitch types for this season
        n_pitches = int(season_data["n"].sum())
        n_samples.append(n_pitches)

        # Weighted average across pitch types within the season
        valid = season_data.dropna(subset=["xwoba"])
        if valid.empty or valid["n"].sum() == 0:
            continue

        season_n = valid["n"].sum()

        for col in rate_cols:
            col_vals = valid[col].fillna(LG_AVG_PITCHER.get(col, 0))
            season_avg = (col_vals * valid["n"]).sum() / season_n
            weighted_stats[col] += season_avg * weight * season_n

        # Velo (weighted by pitch count)
        velo_valid = valid.dropna(subset=["avg_velo"])
        if not velo_valid.empty:
            v = (velo_valid["avg_velo"] * velo_valid["n"]).sum() / velo_valid["n"].sum()
            weighted_velo += v * weight * season_n

        # Stuff+ and Control+
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
        total_n += n_pitches * weight

    if total_weight == 0:
        return None

    projected = {}
    for col in rate_cols:
        raw = weighted_stats[col] / total_weight
        lg_avg = LG_AVG_PITCHER.get(col, raw)
        reliability = min(1.0, total_weight / (PITCHER_REGRESSION_IP_APPROX * sum(MARCEL_WEIGHTS.values())))
        projected[col] = raw * reliability + lg_avg * (1 - reliability)

    velo = weighted_velo / total_weight if total_weight > 0 else 93.0
    stuff_plus = weighted_stuff / total_weight if total_weight > 0 else 100.0
    control_plus = weighted_ctrl / total_weight if total_weight > 0 else 100.0

    # Age adjustment
    if age:
        adj = age_adjustment(age, is_pitcher=True)
        projected["xwoba"] = projected["xwoba"] / adj + LG_AVG_PITCHER["xwoba"] * (1 - 1/adj) if adj > 0 else projected["xwoba"]
        velo *= (1 - (1 - adj) * 0.5)  # velo declines slower than overall performance

    # Derive traditional stats from xwOBA
    # ERA ≈ -6 + 33 * xwOBA (calibrated: .315 xwOBA → ~4.40 ERA)
    era_est = max(2.00, min(7.00, -6.0 + 33.0 * projected["xwoba"]))

    # K/9 from whiff rate: K% ≈ whiff_rate * 0.90 (rough correlation)
    k_pct_est = min(0.40, projected["whiff_rate"] * 0.90)
    k_per_9 = max(4.0, min(14.0, k_pct_est * 27.0))  # K/9 ≈ K% * ~27 BF/9IP

    # BB/9 from zone_rate: lower zone = more walks
    bb_pct_est = max(0.04, 0.22 - 0.33 * projected["zone_rate"])
    bb_per_9 = max(1.5, min(6.0, bb_pct_est * 27.0))

    # WHIP from ERA approximation: WHIP ≈ 0.667 + ERA * 0.167
    whip = max(0.80, min(2.00, 0.72 + era_est * 0.145))

    # IP projection from pitch count history
    if n_samples:
        pitches_proj = int(n_samples[0] * 0.6 +
                          (n_samples[1] if len(n_samples) > 1 else n_samples[0]) * 0.3 +
                          (n_samples[2] if len(n_samples) > 2 else n_samples[0]) * 0.1)
    else:
        pitches_proj = 2000

    # ~15.5 pitches per IP for starters, ~14 for relievers
    pitches_per_ip = 15.5 if pitches_proj > 1500 else 14.0
    ip_est = max(10, min(220, pitches_proj / pitches_per_ip))

    # Strikeouts
    k_total = max(0, int(ip_est * k_per_9 / 9))

    # Wins/losses rough estimate from ERA and IP
    games = ip_est / 5.5 if ip_est > 80 else ip_est / 1.5  # starts vs appearances
    win_pct = max(0.30, min(0.70, 0.5 + (4.50 - era_est) * 0.05))
    wins = max(0, int(games * win_pct * 0.55))  # pitchers get decision ~55% of games
    losses = max(0, int(games * (1 - win_pct) * 0.45))

    return {
        "player_id": player_id,
        "ip": round(ip_est, 1),
        "era": round(era_est, 2),
        "w": wins,
        "l": losses,
        "k": k_total,
        "k_per_9": round(k_per_9, 1),
        "bb_per_9": round(bb_per_9, 1),
        "whip": round(whip, 2),
        "xwoba": round(projected["xwoba"], 3),
        "whiff_rate": round(projected["whiff_rate"], 3),
        "chase_rate": round(projected["chase_rate"], 3),
        "zone_rate": round(projected["zone_rate"], 3),
        "csw_rate": round(projected.get("csw_rate", 0.290), 3),
        "velo": round(velo, 1),
        "stuff_plus": round(stuff_plus, 1),
        "control_plus": round(control_plus, 1),
    }


def generate_projections():
    """Generate all player projections."""
    print("Loading data...")
    bp = pd.read_parquet(os.path.join(DATA_DIR, "batter_profiles.parquet"))
    pa = pd.read_parquet(os.path.join(DATA_DIR, "pitcher_arsenal.parquet"))
    names, teams, positions = load_rosters()

    # Get all player IDs from rosters
    all_player_ids = set(names.keys())
    print(f"  {len(all_player_ids)} players on MLB rosters")

    # Fetch ages
    print("Fetching player ages from MLB API...")
    ages = fetch_player_ages(all_player_ids)
    print(f"  Got ages for {len(ages)} players")

    # Separate batters and pitchers based on roster position
    pitcher_ids = set(positions.keys())
    batter_ids = all_player_ids - pitcher_ids

    # Generate batter projections
    print(f"\nProjecting {len(batter_ids)} batters...")
    batter_projections = []
    for pid in batter_ids:
        proj = marcel_project_batter(pid, bp, batter_ids, age=ages.get(pid))
        if proj:
            proj["name"] = names.get(pid, "Unknown")
            proj["team"] = teams.get(pid, "")
            batter_projections.append(proj)

    batter_projections.sort(key=lambda x: x["xwoba"], reverse=True)
    print(f"  Generated {len(batter_projections)} batter projections")

    # Generate pitcher projections
    print(f"\nProjecting {len(pitcher_ids)} pitchers...")
    pitcher_projections = []
    for pid in pitcher_ids:
        proj = marcel_project_pitcher(pid, pa, age=ages.get(pid))
        if proj:
            proj["name"] = names.get(pid, "Unknown")
            proj["team"] = teams.get(pid, "")
            proj["role"] = positions.get(pid, "P")
            pitcher_projections.append(proj)

    pitcher_projections.sort(key=lambda x: x["xwoba"])
    print(f"  Generated {len(pitcher_projections)} pitcher projections")

    # Build output
    output = {
        "batters": batter_projections,
        "pitchers": pitcher_projections,
        "_meta": {
            "season": PROJECTION_SEASON,
            "method": "Marcel-style weighted projection",
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "n_batters": len(batter_projections),
            "n_pitchers": len(pitcher_projections),
        }
    }

    out_path = os.path.join(DATA_DIR, f"player_projections_{PROJECTION_SEASON}.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, default=float)

    print(f"\nProjections saved to {out_path}")

    # Print top 10 batters
    print(f"\nTop 10 Batters by xwOBA:")
    print(f"  {'Name':>25}  {'Team':>4}  {'PA':>4}  {'AVG':>5}  {'HR':>3}  {'OPS':>5}  {'xwOBA':>5}")
    for p in batter_projections[:10]:
        print(f"  {p['name']:>25}  {p['team']:>4}  {p['pa']:>4}  {p['avg']:.3f}  {p['hr']:>3}  {p['ops']:.3f}  {p['xwoba']:.3f}")

    # Print top 10 pitchers
    print(f"\nTop 10 Pitchers by xwOBA:")
    print(f"  {'Name':>25}  {'Team':>4}  {'IP':>5}  {'ERA':>5}  {'K':>4}  {'WHIP':>5}  {'xwOBA':>5}")
    for p in pitcher_projections[:10]:
        print(f"  {p['name']:>25}  {p['team']:>4}  {p['ip']:>5.0f}  {p['era']:>5.2f}  {p['k']:>4}  {p['whip']:>5.2f}  {p['xwoba']:.3f}")

    return output


if __name__ == "__main__":
    generate_projections()
