#!/usr/bin/env python3
"""
Nightly Monte Carlo season projection.

Fetches current standings + remaining schedule from MLB API,
blends actual record with pre-season win projections,
simulates remaining games, and writes playoff odds to
data/season_projection_{season}.json.

Run nightly after games complete (e.g., 1:30 AM ET = 6:30 AM UTC).

Usage:
  python scripts/update_season_projection.py [--season 2026] [--sims 10000]
"""

import os
import sys
import json
import random
import argparse
import requests
from datetime import datetime, timezone
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
API_BASE = "https://statsapi.mlb.com/api/v1"

N_SIMS = 5_000
# After this many games played, fully trust actual record over pre-season projection
BLEND_GAMES = 30

# Division name mapping: MLB API full name → abbreviated key used in the JSON/template
DIV_ABBREV = {
    "American League East":    "AL East",
    "American League Central": "AL Central",
    "American League West":    "AL West",
    "National League East":    "NL East",
    "National League Central": "NL Central",
    "National League West":    "NL West",
}


# ---------------------------------------------------------------------------
# MLB API helpers
# ---------------------------------------------------------------------------

def fetch_standings(season: int) -> dict:
    url = f"{API_BASE}/standings"
    params = {
        "leagueId": "103,104",
        "season": str(season),
        "standingsTypes": "regularSeason",
        "hydrate": "team(division,league)",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_schedule(season: int) -> dict:
    url = f"{API_BASE}/schedule"
    params = {
        "sportId": 1,
        "season": str(season),
        "gameType": "R",
        "startDate": f"{season}-03-01",
        "endDate": f"{season}-10-05",
        "hydrate": "linescore,team",
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_standings(payload: dict) -> dict:
    """Return {team_id: {w, l, league, division, abbrev, name, ...}}."""
    teams = {}
    for div_block in payload.get("records", []):
        raw_div  = (div_block.get("division") or {}).get("name", "")
        raw_lg   = (div_block.get("league")   or {}).get("name", "")
        div_key  = DIV_ABBREV.get(raw_div, raw_div)
        for tr in div_block.get("teamRecords", []):
            team = tr.get("team") or {}
            tid  = team.get("id")
            if not tid:
                continue
            teams[tid] = {
                "team_id":  tid,
                "abbrev":   team.get("abbreviation", ""),
                "name":     team.get("name", ""),
                "league":   raw_lg,
                "division": raw_div,
                "div_key":  div_key,
                "w":        int(tr.get("wins", 0) or 0),
                "l":        int(tr.get("losses", 0) or 0),
            }
    return teams


def parse_schedule(payload: dict) -> list:
    """Return list of {home_id, away_id, status}."""
    games = []
    for date_block in payload.get("dates", []):
        for g in date_block.get("games", []):
            status = g.get("status", {}).get("abstractGameState", "")
            teams  = g.get("teams", {})
            home_t = (teams.get("home") or {}).get("team") or {}
            away_t = (teams.get("away") or {}).get("team") or {}
            home_id = home_t.get("id")
            away_id = away_t.get("id")
            if home_id and away_id:
                games.append({
                    "home_id": home_id,
                    "away_id": away_id,
                    "status":  status,
                })
    return games


# ---------------------------------------------------------------------------
# Pre-season projection loader
# ---------------------------------------------------------------------------

def load_preseason_wins(season: int) -> dict:
    """Load previous season_projection JSON for talent baseline (keyed by team_id)."""
    proj_path = os.path.join(DATA_DIR, f"season_projection_{season}.json")
    if not os.path.exists(proj_path):
        return {}
    with open(proj_path) as f:
        data = json.load(f)
    wins = {}
    for key, val in data.items():
        if key == "_meta" or not isinstance(val, list):
            continue
        for t in val:
            tid = t.get("team_id")
            if tid:
                wins[tid] = float(t.get("avg_wins", 81))
    return wins


# ---------------------------------------------------------------------------
# Talent estimation
# ---------------------------------------------------------------------------

def compute_win_probs(teams: dict, preseason_wins: dict) -> dict:
    """
    Estimate each team's per-game win probability.

    Early in season: blend pre-season projection with actual record.
    Late in season: trust actual record.
    """
    win_probs = {}
    for tid, t in teams.items():
        w = t["w"]
        l = t["l"]
        gp = w + l

        pre_wp = preseason_wins.get(tid, 81) / 162.0

        if gp == 0:
            wp = pre_wp
        else:
            actual_wp = w / gp
            blend = max(0.0, (BLEND_GAMES - gp) / BLEND_GAMES)
            wp = blend * pre_wp + (1.0 - blend) * actual_wp

        win_probs[tid] = max(0.25, min(0.75, wp))
    return win_probs


# ---------------------------------------------------------------------------
# Simulation helpers
# ---------------------------------------------------------------------------

def log5(wp_home: float, wp_away: float, home_adv: float = 0.04) -> float:
    """P(home wins) via log5 + small home-field bump."""
    wp_h = min(0.75, max(0.25, wp_home + home_adv / 2))
    wp_a = min(0.75, max(0.25, wp_away - home_adv / 2))
    denom = wp_h * (1 - wp_a) + wp_a * (1 - wp_h)
    return (wp_h * (1 - wp_a)) / denom if denom > 0 else 0.5


def series_winner(t1: int, t2: int, win_probs: dict, best_of: int) -> int:
    """Simulate a best-of-N series, return winning team_id."""
    need = (best_of + 1) // 2
    w1 = w2 = 0
    p = log5(win_probs.get(t1, 0.5), win_probs.get(t2, 0.5))
    while w1 < need and w2 < need:
        if random.random() < p:
            w1 += 1
        else:
            w2 += 1
    return t1 if w1 >= need else t2


def simulate_playoff(league_teams: list, final_wins: dict, win_probs: dict,
                     counters: dict):
    """
    Simulate one league's 6-team playoff bracket.

    Seeds 1-6 by final wins. Wild-card round is best-of-3, DS best-of-5, CS best-of-7.
    Returns World Series representative for this league.
    """
    sorted_teams = sorted(league_teams, key=lambda t: final_wins.get(t["team_id"], 0), reverse=True)
    if len(sorted_teams) < 6:
        return None

    seeded = [t["team_id"] for t in sorted_teams[:6]]

    # Wild card round: (3 vs 6), (4 vs 5)
    wc_w1 = series_winner(seeded[2], seeded[5], win_probs, 3)
    wc_w2 = series_winner(seeded[3], seeded[4], win_probs, 3)

    ds_field = [seeded[0], seeded[1], wc_w1, wc_w2]
    for tid in ds_field:
        counters["ds"][tid] += 1

    # Division series: 1 vs lower WC winner, 2 vs higher WC winner
    ds_w1 = series_winner(seeded[0], wc_w2, win_probs, 5)
    ds_w2 = series_winner(seeded[1], wc_w1, win_probs, 5)

    for tid in [ds_w1, ds_w2]:
        counters["cs"][tid] += 1

    # Championship series
    lg_champ = series_winner(ds_w1, ds_w2, win_probs, 7)
    counters["ws"][lg_champ] += 1

    return lg_champ


# ---------------------------------------------------------------------------
# Main simulation loop
# ---------------------------------------------------------------------------

def run_simulation(teams: dict, remaining_games: list, win_probs: dict,
                   n_sims: int) -> dict:
    current_w = {tid: t["w"] for tid, t in teams.items()}
    current_l = {tid: t["l"] for tid, t in teams.items()}

    # Group by league and division for playoff seeding
    teams_by_league = defaultdict(list)
    teams_by_div    = defaultdict(list)
    for t in teams.values():
        teams_by_league[t["league"]].append(t)
        teams_by_div[t["div_key"]].append(t)

    # Accumulators
    playoff_cnt  = defaultdict(int)
    div_cnt      = defaultdict(int)
    wc_cnt       = defaultdict(int)
    counters     = {"ds": defaultdict(int), "cs": defaultdict(int), "ws": defaultdict(int)}
    win_ws_cnt   = defaultdict(int)
    # Welford's online mean/variance — avoids storing all samples
    win_count    = defaultdict(int)
    win_mean     = defaultdict(float)
    win_m2       = defaultdict(float)  # sum of squared deviations

    # Reuse these dicts each iteration to avoid per-sim allocation
    sim_wins = dict(current_w)
    sim_loss = dict(current_l)

    for _ in range(n_sims):
        # Reset to current standings
        sim_wins.update(current_w)
        sim_loss.update(current_l)

        for g in remaining_games:
            hi, ai = g["home_id"], g["away_id"]
            if hi not in win_probs or ai not in win_probs:
                continue
            if random.random() < log5(win_probs[hi], win_probs[ai]):
                sim_wins[hi] += 1
                sim_loss[ai] += 1
            else:
                sim_wins[ai] += 1
                sim_loss[hi] += 1

        # Welford update
        for tid in teams:
            w = sim_wins.get(tid, 0)
            win_count[tid] += 1
            delta = w - win_mean[tid]
            win_mean[tid] += delta / win_count[tid]
            win_m2[tid] += delta * (w - win_mean[tid])

        # Determine playoffs per league
        ws_participants = []
        for league, lg_teams in teams_by_league.items():
            # Division winners
            div_groups = defaultdict(list)
            for t in lg_teams:
                div_groups[t["div_key"]].append(t)

            div_winners  = []
            non_winners  = []
            for dv, dv_teams in div_groups.items():
                ranked = sorted(dv_teams, key=lambda t: sim_wins.get(t["team_id"], 0), reverse=True)
                if ranked:
                    div_winners.append(ranked[0])
                    non_winners.extend(ranked[1:])
                    div_cnt[ranked[0]["team_id"]] += 1

            wc_teams = sorted(non_winners, key=lambda t: sim_wins.get(t["team_id"], 0), reverse=True)[:3]
            for t in wc_teams:
                wc_cnt[t["team_id"]] += 1

            for t in div_winners + wc_teams:
                playoff_cnt[t["team_id"]] += 1

            # Simulate playoff bracket
            champ = simulate_playoff(div_winners + wc_teams, sim_wins, win_probs, counters)
            if champ:
                ws_participants.append(champ)

        # World Series
        if len(ws_participants) == 2:
            ws_winner = series_winner(ws_participants[0], ws_participants[1], win_probs, 7)
            win_ws_cnt[ws_winner] += 1

    # Build per-team result dicts
    results = {}
    for tid, t in teams.items():
        avg_wins = win_mean[tid]
        n = win_count[tid]
        std_wins = (win_m2[tid] / n) ** 0.5 if n > 1 else 0.0
        # Approximate p10/p90 via normal distribution (win totals are roughly normal)
        p10_wins = round(max(current_w.get(tid, 0), avg_wins - 1.282 * std_wins))
        p90_wins = round(min(162, avg_wins + 1.282 * std_wins))

        results[tid] = {
            "team_id":        tid,
            "abbrev":         t["abbrev"],
            "name":           t["name"],
            "div_key":        t["div_key"],
            "current_wins":   current_w.get(tid, 0),
            "current_losses": current_l.get(tid, 0),
            "avg_wins":       round(avg_wins, 3),
            "avg_losses":     round(162 - avg_wins, 3),
            "win_std":        round(std_wins, 3),
            "p10_wins":       float(p10_wins),
            "p90_wins":       float(p90_wins),
            "playoff_pct":    round(playoff_cnt[tid] / n_sims, 3),
            "division_pct":   round(div_cnt[tid]     / n_sims, 3),
            "wild_card_pct":  round(wc_cnt[tid]      / n_sims, 3),
            "ds_pct":         round(counters["ds"][tid] / n_sims, 3),
            "cs_pct":         round(counters["cs"][tid] / n_sims, 3),
            "ws_pct":         round(counters["ws"][tid] / n_sims, 3),
            "win_ws_pct":     round(win_ws_cnt[tid]     / n_sims, 3),
        }
    return results


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Nightly Monte Carlo season projection")
    parser.add_argument("--season", type=int, default=datetime.now(timezone.utc).year)
    parser.add_argument("--sims",   type=int, default=N_SIMS)
    args = parser.parse_args()
    season = args.season
    n_sims = args.sims

    print(f"[{datetime.now()}] Season projection update — {season}, {n_sims:,} sims")

    # 1. Current standings
    print("  Fetching standings...")
    try:
        standings_payload = fetch_standings(season)
    except Exception as e:
        print(f"ERROR fetching standings: {e}", file=sys.stderr)
        return 1
    teams = parse_standings(standings_payload)
    if not teams:
        print("ERROR: No teams parsed from standings.", file=sys.stderr)
        return 1
    print(f"  {len(teams)} teams")

    # 2. Full regular season schedule
    print("  Fetching schedule...")
    try:
        sched_payload = fetch_schedule(season)
    except Exception as e:
        print(f"ERROR fetching schedule: {e}", file=sys.stderr)
        return 1
    all_games  = parse_schedule(sched_payload)
    remaining  = [g for g in all_games if g["status"] != "Final"]
    completed  = [g for g in all_games if g["status"] == "Final"]
    print(f"  {len(completed)} completed, {len(remaining)} remaining")

    # 3. Pre-season projections (for talent blending early in season)
    preseason_wins = load_preseason_wins(season)
    print(f"  Pre-season data for {len(preseason_wins)} teams")

    # 4. Win probabilities
    win_probs = compute_win_probs(teams, preseason_wins)

    # 5. Monte Carlo
    print(f"  Running {n_sims:,} simulations...")
    random.seed(42)
    results = run_simulation(teams, remaining, win_probs, n_sims)

    # 6. Package into division-keyed output matching the template format
    output: dict = {}
    for tid, r in results.items():
        dk = r.pop("div_key")
        output.setdefault(dk, []).append(r)

    # Sort each division by avg_wins descending
    for dk in output:
        output[dk].sort(key=lambda x: x["avg_wins"], reverse=True)

    # Meta block
    output["_meta"] = {
        "season":          season,
        "n_sims":          n_sims,
        "games_completed": len(completed),
        "games_remaining": len(remaining),
        "generated_at":    datetime.now(timezone.utc).isoformat(),
    }

    out_path = os.path.join(DATA_DIR, f"season_projection_{season}.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"  Wrote {out_path}")
    print(f"[{datetime.now()}] Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
