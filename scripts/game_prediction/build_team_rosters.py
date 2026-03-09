#!/usr/bin/env python3
"""
Build team rosters with player quality metrics for season simulation.

Pulls 40-man rosters from MLB API, matches players to our datasets
(batter profiles, pitcher arsenal, catcher framing, pitcher workload),
and outputs team-level data ready for the game simulator.

Output: data/team_rosters.json
"""

import os
import sys
import json
import time
import requests
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
API_BASE = "https://statsapi.mlb.com/api/v1"

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


def fetch_roster(team_id: int, season: int) -> tuple:
    """Fetch depth chart roster from MLB API (projected starters, not full 40-man).
    Falls back to 40-man if depth chart unavailable.
    Returns (deduped_roster, raw_roster) — raw_roster preserves position-order
    for lineup construction, deduped_roster is for iterating unique players."""
    url = f"{API_BASE}/teams/{team_id}/roster"
    for roster_type in ["depthChart", "40Man"]:
        for yr in [season, season - 1]:
            try:
                resp = requests.get(url, params={"rosterType": roster_type, "season": yr}, timeout=30)
                resp.raise_for_status()
                roster = resp.json().get("roster", [])
                if roster:
                    # Deduplicate for player iteration
                    seen = set()
                    deduped = []
                    for entry in roster:
                        pid = entry["person"]["id"]
                        if pid not in seen:
                            seen.add(pid)
                            deduped.append(entry)
                    return deduped, roster
            except Exception:
                continue
    print(f"  Warning: Could not fetch roster for team {team_id}")
    return [], []


def load_player_data():
    """Load all player datasets."""
    data = {}

    # Batter profiles
    bp_path = os.path.join(OUTPUT_DIR, "batter_profiles.parquet")
    if os.path.exists(bp_path):
        bp = pd.read_parquet(bp_path)
        # Use most recent season available per player
        data["batters"] = bp
    else:
        data["batters"] = pd.DataFrame()

    # Pitcher arsenal
    pa_path = os.path.join(OUTPUT_DIR, "pitcher_arsenal.parquet")
    if os.path.exists(pa_path):
        pa = pd.read_parquet(pa_path)
        data["pitchers"] = pa
    else:
        data["pitchers"] = pd.DataFrame()

    # Catcher framing
    cf_path = os.path.join(OUTPUT_DIR, "catcher_framing.parquet")
    if os.path.exists(cf_path):
        cf = pd.read_parquet(cf_path)
        data["catchers"] = cf
    else:
        data["catchers"] = pd.DataFrame()

    # Pitcher workload (for role detection: starter vs reliever)
    pw_path = os.path.join(OUTPUT_DIR, "pitcher_workload.parquet")
    if os.path.exists(pw_path):
        pw = pd.read_parquet(pw_path)
        data["workload"] = pw
    else:
        data["workload"] = pd.DataFrame()

    return data


def get_best_season_stats(df, player_col, player_id, stand_filter=None):
    """Get the most recent season's stats for a player, falling back to earlier seasons."""
    mask = df[player_col] == player_id
    if stand_filter:
        mask &= df["vs_hand" if "vs_hand" in df.columns else "stand"] == stand_filter
    subset = df[mask]
    if subset.empty:
        return None
    # Most recent season with decent sample
    for season in sorted(subset["season"].unique(), reverse=True):
        row = subset[subset["season"] == season]
        if not row.empty:
            return row.iloc[0]
    return None


def compute_batter_quality(stats) -> float:
    """Compute a batter quality score (0-1 scale, 0.5 = average).
    Based on xwoba, barrel rate, chase rate, k rate."""
    if stats is None:
        return 0.34  # slightly below-avg default for unproven players

    xwoba = float(stats.get("xwoba", 0) or 0)
    if xwoba <= 0:
        return 0.35

    # xwoba typically ranges 0.250-0.420, avg ~0.315
    # Map to 0-1 scale with wider spread
    score = max(0.05, min(0.95, (xwoba - 0.220) / (0.440 - 0.220)))
    return score


def compute_pitcher_quality(stats_rows) -> dict:
    """Compute pitcher quality metrics from arsenal data.
    Returns dict with stuff_plus, control_plus, velo, xwoba."""
    if stats_rows is None or (hasattr(stats_rows, 'empty') and stats_rows.empty):
        return {"stuff_plus": 95.0, "control_plus": 95.0,
                "velo": 93.0, "xwoba": 0.330, "whiff_rate": 0.22}

    if isinstance(stats_rows, pd.Series):
        stats_rows = stats_rows.to_frame().T

    # Weighted average across pitch types (drop NaN xwoba rows)
    valid = stats_rows.dropna(subset=["xwoba"])
    total_n = valid["n"].sum()
    if total_n == 0:
        return {"stuff_plus": 100.0, "control_plus": 100.0,
                "velo": 93.0, "xwoba": 0.315, "whiff_rate": 0.24}

    w_velo = (valid["avg_velo"] * valid["n"]).sum() / total_n
    w_whiff = (valid["whiff_rate"] * valid["n"]).sum() / total_n
    w_xwoba = (valid["xwoba"] * valid["n"]).sum() / total_n

    # Regress small samples toward league average (.315)
    if total_n < 500:
        weight = total_n / 500.0
        w_xwoba = w_xwoba * weight + 0.315 * (1 - weight)

    # stuff_plus/control_plus may be NaN from our data
    sp = valid["avg_stuff_plus"].dropna()
    cp = valid["avg_control_plus"].dropna()
    stuff = float((sp * valid.loc[sp.index, "n"]).sum() / valid.loc[sp.index, "n"].sum()) if not sp.empty and sp.sum() > 0 else 100.0
    ctrl = float((cp * valid.loc[cp.index, "n"]).sum() / valid.loc[cp.index, "n"].sum()) if not cp.empty and cp.sum() > 0 else 100.0

    return {
        "stuff_plus": stuff if stuff > 0 else 100.0,
        "control_plus": ctrl if ctrl > 0 else 100.0,
        "velo": float(w_velo) if not np.isnan(w_velo) else 93.0,
        "xwoba": float(w_xwoba) if not np.isnan(w_xwoba) else 0.315,
        "whiff_rate": float(w_whiff) if not np.isnan(w_whiff) else 0.24,
    }


def is_starter_role(workload_df, pitcher_id) -> bool:
    """Determine if pitcher is primarily a starter based on workload data."""
    if workload_df.empty:
        return False
    pw = workload_df[workload_df["pitcher"] == pitcher_id]
    if pw.empty:
        return False
    # Use most recent season
    latest = pw["season"].max()
    pw_latest = pw[pw["season"] == latest]
    return pw_latest["is_starter"].mean() > 0.5


def build_team_rosters(season: int = 2026):
    """Build team roster data with player quality metrics."""
    print(f"Building team rosters for {season}...")

    player_data = load_player_data()
    batters_df = player_data["batters"]
    pitchers_df = player_data["pitchers"]
    catchers_df = player_data["catchers"]
    workload_df = player_data["workload"]

    teams = {}

    for team_id in TEAM_IDS:
        abbrev = TEAM_ABBREVS.get(team_id, str(team_id))
        print(f"  {abbrev} ({team_id})...")

        roster, raw_roster = fetch_roster(team_id, season)
        time.sleep(0.1)  # rate limit

        if not roster:
            print(f"    No roster found, using defaults")
            teams[team_id] = _default_team(team_id)
            continue

        # Separate position players and pitchers
        hitters = []
        pitchers_list = []
        catchers = []

        for entry in roster:
            pid = entry["person"]["id"]
            pos_type = entry["position"]["type"]
            pos_abbr = entry["position"]["abbreviation"]

            if pos_type == "Pitcher":
                # Get pitcher quality from arsenal data.
                # Prefer most recent season with meaningful sample (200+ pitches).
                all_p_rows = pitchers_df[
                    (pitchers_df["pitcher"] == pid) & (pitchers_df["stand"] == "ALL")
                ]
                p_rows = pd.DataFrame()
                if not all_p_rows.empty:
                    for season_yr in sorted(all_p_rows["season"].unique(), reverse=True):
                        candidate = all_p_rows[all_p_rows["season"] == season_yr]
                        if candidate["n"].sum() >= 200:
                            p_rows = candidate
                            break
                    # If no season has 200+ pitches, use most recent anyway
                    if p_rows.empty:
                        latest = all_p_rows["season"].max()
                        p_rows = all_p_rows[all_p_rows["season"] == latest]

                quality = compute_pitcher_quality(p_rows)
                is_sp = is_starter_role(workload_df, pid)

                pitchers_list.append({
                    "id": pid,
                    "name": entry["person"]["fullName"],
                    "is_starter": is_sp,
                    **quality,
                })

            else:
                # Position player
                b_stats = get_best_season_stats(batters_df, "batter", pid, "ALL")
                quality = compute_batter_quality(b_stats)

                player_info = {
                    "id": pid,
                    "name": entry["person"]["fullName"],
                    "position": pos_abbr,
                    "quality": quality,
                }

                if b_stats is not None:
                    xw = float(b_stats.get("xwoba", 0) or 0)
                    pa = int(b_stats.get("pa", 0) or 0)
                    if xw <= 0.100:
                        xw = 0.295  # treat near-zero as missing
                    elif pa < 200:
                        # Regress small samples toward league avg (.315)
                        # At 0 PA = full default, at 200+ PA = full actual
                        weight = pa / 200.0
                        xw = xw * weight + 0.315 * (1 - weight)
                    player_info["xwoba"] = xw
                    player_info["k_pct"] = float(b_stats.get("k_pct", 0) or 0)
                    player_info["bb_pct"] = float(b_stats.get("bb_pct", 0) or 0)
                    player_info["barrel_rate"] = float(b_stats.get("barrel_rate", 0) or 0)
                    player_info["iso"] = float(b_stats.get("iso", 0) or 0)
                else:
                    # Unproven/prospect default: slightly below-avg MLB hitter
                    player_info["xwoba"] = 0.295
                    player_info["k_pct"] = 0.24
                    player_info["bb_pct"] = 0.07
                    player_info["barrel_rate"] = 0.06
                    player_info["iso"] = 0.130

                hitters.append(player_info)

                if pos_abbr == "C":
                    catchers.append(player_info)

        # Build lineup from depth chart: pick one starter per position slot.
        # The depth chart lists players in order at each position, so the first
        # unused player at each position is the projected starter there.
        # We use the raw roster (before dedup) to find starters by position.
        lineup_positions = ["C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "DH"]
        hitter_by_id = {h["id"]: h for h in hitters}
        lineup = []
        used_ids = set()
        for pos in lineup_positions:
            # Walk the raw roster entries to find first player at this position
            for entry in raw_roster:
                pid = entry["person"]["id"]
                if entry["position"]["abbreviation"] == pos and pid not in used_ids and pid in hitter_by_id:
                    lineup.append(hitter_by_id[pid])
                    used_ids.add(pid)
                    break
        # Fill any missing slots with best remaining hitters
        for h in sorted(hitters, key=lambda x: x["quality"], reverse=True):
            if len(lineup) >= 9:
                break
            if h["id"] not in used_ids:
                lineup.append(h)
                used_ids.add(h["id"])
        # Pad if still short
        while len(lineup) < 9:
            lineup.append(_default_hitter())

        # Sort pitchers into rotation and bullpen
        starters = [p for p in pitchers_list if p["is_starter"]]
        relievers = [p for p in pitchers_list if not p["is_starter"]]

        # If not enough starters detected, take top pitchers by xwoba
        if len(starters) < 5:
            non_starters = [p for p in pitchers_list if p not in starters]
            non_starters.sort(key=lambda x: x["xwoba"])  # lower xwoba = better
            while len(starters) < 5 and non_starters:
                starters.append(non_starters.pop(0))

        # Sort starters by xwoba (best first)
        starters.sort(key=lambda x: x["xwoba"])
        # Sort relievers by xwoba (best first)
        relievers.sort(key=lambda x: x["xwoba"])

        # Catcher framing
        catcher_framing = 0.0
        if catchers and not catchers_df.empty:
            for c in catchers:
                cf_row = catchers_df[catchers_df["catcher"] == c["id"]]
                if not cf_row.empty:
                    latest = cf_row["season"].max()
                    catcher_framing = float(cf_row[cf_row["season"] == latest]["framing_runs_per_game"].iloc[0])
                    break

        # Compute raw xwOBA values (will normalize across league after collecting all teams)
        lineup_xwobas = [h.get("xwoba", 0.260) for h in lineup]
        avg_lineup_xwoba = np.mean(lineup_xwobas)

        rotation_xwoba = np.mean([s["xwoba"] for s in starters[:5]]) if starters else 0.330
        bullpen_xwoba = np.mean([r["xwoba"] for r in relievers[:8]]) if relievers else 0.315
        pitching_xwoba = 0.65 * rotation_xwoba + 0.35 * bullpen_xwoba

        # Store raw data for second pass (normalization)
        teams[team_id] = {
            "team_id": team_id,
            "abbrev": abbrev,
            "lineup": [{"id": h["id"], "name": h["name"], "quality": h["quality"],
                         "xwoba": h.get("xwoba", 0.260)} for h in lineup],
            "rotation": [{"id": s["id"], "name": s["name"],
                          "stuff_plus": s["stuff_plus"],
                          "control_plus": s["control_plus"],
                          "velo": s["velo"], "xwoba": s["xwoba"],
                          "whiff_rate": s["whiff_rate"]} for s in starters[:5]],
            "bullpen": [{"id": r["id"], "name": r["name"],
                         "stuff_plus": r["stuff_plus"],
                         "control_plus": r["control_plus"],
                         "velo": r["velo"], "xwoba": r["xwoba"],
                         "whiff_rate": r["whiff_rate"]} for r in relievers],
            "catcher_framing": catcher_framing,
            "lineup_quality": float(avg_lineup_xwoba),
            "rotation_xwoba": float(rotation_xwoba),
            "bullpen_xwoba": float(bullpen_xwoba),
            "pitching_xwoba": float(pitching_xwoba),
            "n_hitters": len(hitters),
            "n_pitchers": len(pitchers_list),
        }

    # === Second pass: normalize xwOBA pools and compute RS/RA ===
    # Batting and pitching xwOBA come from different player pools and have different
    # means. In a real league, league-avg offense = league-avg pitching. Normalize
    # both pools to center on a common league average (.315, typical MLB).
    league_xwoba = 0.315
    all_bat_xwoba = [t["lineup_quality"] for t in teams.values()]
    all_pit_xwoba = [t["pitching_xwoba"] for t in teams.values()]
    mean_bat = np.mean(all_bat_xwoba)
    mean_pit = np.mean(all_pit_xwoba)
    print(f"\n  xwOBA pool means — batting: {mean_bat:.3f}, pitching: {mean_pit:.3f}")
    print(f"  Normalizing both to league avg {league_xwoba:.3f}")

    for team_id, t in teams.items():
        # Shift each team's xwOBA relative to their pool's mean, centered on league avg
        norm_bat_xwoba = league_xwoba + (t["lineup_quality"] - mean_bat)
        norm_pit_xwoba = league_xwoba + (t["pitching_xwoba"] - mean_pit)

        # Convert to runs using the same formula for both
        runs_scored_per_game = -2.0 + 20.5 * norm_bat_xwoba
        runs_allowed_per_game = -2.0 + 20.5 * norm_pit_xwoba

        # Catcher framing adjustment
        runs_allowed_per_game -= t["catcher_framing"] * 0.5

        # Regress 15% toward league average (standard in projection systems)
        # This accounts for roster uncertainty, in-season adjustments, and
        # the tendency of extreme projections to overstate true talent gaps
        league_avg_rpg = -2.0 + 20.5 * league_xwoba  # ~4.4575
        regression = 0.15
        runs_scored_per_game = runs_scored_per_game * (1 - regression) + league_avg_rpg * regression
        runs_allowed_per_game = runs_allowed_per_game * (1 - regression) + league_avg_rpg * regression

        # Floor/ceiling
        runs_scored_per_game = max(3.0, min(6.5, runs_scored_per_game))
        runs_allowed_per_game = max(3.0, min(6.5, runs_allowed_per_game))

        # Pythagorean expectation
        exp = 1.83
        rs_exp = runs_scored_per_game ** exp
        ra_exp = runs_allowed_per_game ** exp
        pyth_wpct = rs_exp / (rs_exp + ra_exp)

        t["runs_scored_pg"] = float(runs_scored_per_game)
        t["runs_allowed_pg"] = float(runs_allowed_per_game)
        t["expected_wpct"] = float(pyth_wpct)

    # Save
    output_path = os.path.join(OUTPUT_DIR, "team_rosters.json")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(teams, f, indent=2, default=float)

    print(f"\nTeam rosters saved to {output_path}")

    # Summary
    print(f"\n{'Team':>5}  {'LinXW':>6}  {'PitXW':>6}  {'RS/G':>5}  {'RA/G':>5}  {'ExpW%':>6}  {'Wins':>5}")
    for tid in sorted(teams.keys(), key=lambda t: teams[t]["expected_wpct"], reverse=True):
        t = teams[tid]
        print(f"  {t['abbrev']:>4}  {t['lineup_quality']:>6.3f}  {t['pitching_xwoba']:>6.3f}  "
              f"{t['runs_scored_pg']:>5.2f}  {t['runs_allowed_pg']:>5.2f}  "
              f"{t['expected_wpct']:>5.3f}  {t['expected_wpct']*162:>5.0f}")

    return teams


def _default_team(team_id):
    return {
        "team_id": team_id,
        "abbrev": TEAM_ABBREVS.get(team_id, str(team_id)),
        "lineup": [_default_hitter() for _ in range(9)],
        "rotation": [_default_starter() for _ in range(5)],
        "bullpen": [_default_reliever() for _ in range(7)],
        "catcher_framing": 0.0,
        "lineup_quality": 0.5,
        "rotation_xwoba": 0.315,
        "expected_wpct": 0.500,
        "n_hitters": 0,
        "n_pitchers": 0,
    }


def _default_hitter():
    return {"id": 0, "name": "Unknown", "quality": 0.34, "xwoba": 0.295}


def _default_starter():
    return {"id": 0, "name": "Unknown", "stuff_plus": 100.0, "control_plus": 100.0,
            "velo": 93.0, "xwoba": 0.315, "whiff_rate": 0.24}


def _default_reliever():
    return {"id": 0, "name": "Unknown", "stuff_plus": 100.0, "control_plus": 100.0,
            "velo": 94.0, "xwoba": 0.300, "whiff_rate": 0.27}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, default=2026)
    args = parser.parse_args()
    build_team_rosters(args.season)
