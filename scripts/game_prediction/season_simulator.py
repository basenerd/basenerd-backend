#!/usr/bin/env python3
"""
Season simulator: Monte Carlo full-season projection.

For each remaining game on the schedule:
1. Project lineups from recent usage patterns
2. Project starters from rotation patterns
3. Estimate bullpen availability from workload data
4. Run the game simulator
5. Update team state (record, roster, fatigue)

Outputs: Win total distributions, playoff odds, division/WC probabilities.

Usage:
  python scripts/game_prediction/season_simulator.py --season 2026 --n-sims 1000
"""

import os
import sys
import json
import argparse
import random
import requests
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(__file__))

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
API_BASE = "https://statsapi.mlb.com/api/v1"

# Division structure
DIVISIONS = {
    "AL East": [110, 111, 139, 141, 147],     # BAL, BOS, TB, TOR, NYY
    "AL Central": [114, 116, 118, 142, 145],   # CLE, DET, KC, MIN, CWS
    "AL West": [108, 117, 133, 136, 140],      # LAA, HOU, OAK, SEA, TEX
    "NL East": [120, 121, 143, 144, 146],      # WSH, NYM, PHI, ATL, MIA
    "NL Central": [112, 113, 134, 138, 158],   # CHC, CIN, PIT, STL, MIL
    "NL West": [109, 115, 119, 135, 137],      # ARI, COL, LAD, SD, SF
}

TEAM_ABBREVS = {
    108: "LAA", 109: "ARI", 110: "BAL", 111: "BOS", 112: "CHC",
    113: "CIN", 114: "CLE", 115: "COL", 116: "DET", 117: "HOU",
    118: "KC",  119: "LAD", 120: "WSH", 121: "NYM", 133: "OAK",
    134: "PIT", 135: "SD",  136: "SEA", 137: "SF",  138: "STL",
    139: "TB",  140: "TEX", 141: "TOR", 142: "MIN", 143: "PHI",
    144: "ATL", 145: "CWS", 146: "MIA", 147: "NYY", 158: "MIL",
}

TEAM_NAMES = {
    108: "Los Angeles Angels", 109: "Arizona Diamondbacks", 110: "Baltimore Orioles",
    111: "Boston Red Sox", 112: "Chicago Cubs", 113: "Cincinnati Reds",
    114: "Cleveland Guardians", 115: "Colorado Rockies", 116: "Detroit Tigers",
    117: "Houston Astros", 118: "Kansas City Royals", 119: "Los Angeles Dodgers",
    120: "Washington Nationals", 121: "New York Mets", 133: "Oakland Athletics",
    134: "Pittsburgh Pirates", 135: "San Diego Padres", 136: "Seattle Mariners",
    137: "San Francisco Giants", 138: "St. Louis Cardinals", 139: "Tampa Bay Rays",
    140: "Texas Rangers", 141: "Toronto Blue Jays", 142: "Minnesota Twins",
    143: "Philadelphia Phillies", 144: "Atlanta Braves", 145: "Chicago White Sox",
    146: "Miami Marlins", 147: "New York Yankees", 158: "Milwaukee Brewers",
}


@dataclass
class TeamState:
    team_id: int
    wins: int = 0
    losses: int = 0
    rotation: List[int] = field(default_factory=list)  # pitcher IDs in rotation order
    rotation_index: int = 0
    lineup: List[int] = field(default_factory=list)  # default lineup
    bullpen: List[Dict] = field(default_factory=list)
    catcher_framing: float = 0.0
    home_park_run_factor: float = 1.0
    home_park_hr_factor: float = 1.0
    runs_scored_pg: float = 4.5   # team offensive runs per game
    runs_allowed_pg: float = 4.5  # team pitching+defense runs allowed per game

    def next_starter(self) -> int:
        if not self.rotation:
            return 0
        starter = self.rotation[self.rotation_index % len(self.rotation)]
        self.rotation_index += 1
        return starter


def get_remaining_schedule(season: int, after_date: str = None) -> List[Dict]:
    """Get remaining games from MLB schedule API."""
    url = f"{API_BASE}/schedule"
    params = {
        "sportId": 1,
        "season": season,
        "gameType": "R",
        "startDate": after_date or f"{season}-03-25",
        "endDate": f"{season}-09-30",
    }
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    games = []
    for game_date in data.get("dates", []):
        for game in game_date.get("games", []):
            game_status = game.get("status", {}).get("abstractGameState", "")
            games.append({
                "game_pk": game["gamePk"],
                "game_date": game_date["date"],
                "home_team_id": game["teams"]["home"]["team"]["id"],
                "away_team_id": game["teams"]["away"]["team"]["id"],
                "status": game_status,
            })

    return games


def get_current_standings(season: int) -> Dict[int, Dict]:
    """Get current W-L record for each team."""
    url = f"{API_BASE}/standings"
    params = {
        "leagueId": "103,104",
        "season": season,
        "standingsTypes": "regularSeason",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    standings = {}
    for record in data.get("records", []):
        for team_rec in record.get("teamRecords", []):
            team_id = team_rec["team"]["id"]
            standings[team_id] = {
                "wins": team_rec["wins"],
                "losses": team_rec["losses"],
                "team_name": team_rec["team"]["name"],
            }

    return standings


def load_team_data(season: int) -> Dict[int, TeamState]:
    """Load team rosters, rotations, and park factors."""
    teams = {}

    # Get current standings
    try:
        standings = get_current_standings(season)
    except Exception:
        standings = {}

    # Load roster data (built by build_team_rosters.py)
    roster_path = os.path.join(OUTPUT_DIR, "team_rosters.json")
    roster_data = {}
    if os.path.exists(roster_path):
        with open(roster_path) as f:
            roster_data = json.load(f)
        print(f"  Loaded roster data for {len(roster_data)} teams")
    else:
        print("  WARNING: No team_rosters.json found — run build_team_rosters.py first")

    # Initialize team states with runs scored/allowed per game
    for div_name, team_ids in DIVISIONS.items():
        for team_id in team_ids:
            ts = TeamState(team_id=team_id)
            if team_id in standings:
                ts.wins = standings[team_id]["wins"]
                ts.losses = standings[team_id]["losses"]

            # Load roster-based run production/prevention
            rdata = roster_data.get(str(team_id), {})
            if rdata:
                ts.runs_scored_pg = rdata.get("runs_scored_pg", 4.5)
                ts.runs_allowed_pg = rdata.get("runs_allowed_pg", 4.5)
                ts.lineup = [h["id"] for h in rdata.get("lineup", [])]
                ts.rotation = [s["id"] for s in rdata.get("rotation", [])]
                ts.bullpen = [
                    {"id": r["id"], "stuff_plus": r.get("stuff_plus", 100),
                     "control_plus": r.get("control_plus", 100),
                     "days_rest": 2, "role": "RP"}
                    for r in rdata.get("bullpen", [])
                ]
                ts.catcher_framing = rdata.get("catcher_framing", 0.0)

            teams[team_id] = ts

    # Diagnostic output
    print(f"  Team run estimates loaded:")
    for tid in sorted(teams, key=lambda t: teams[t].runs_scored_pg - teams[t].runs_allowed_pg, reverse=True):
        ts = teams[tid]
        diff = ts.runs_scored_pg - ts.runs_allowed_pg
        print(f"    {TEAM_ABBREVS.get(tid, tid):>4}: RS={ts.runs_scored_pg:.2f}  RA={ts.runs_allowed_pg:.2f}  diff={diff:+.2f}")

    return teams


def determine_playoff_teams(team_states: Dict[int, TeamState]) -> Dict:
    """Determine playoff qualifiers from final standings."""
    results = {
        "division_winners": {},
        "wild_cards": {"AL": [], "NL": []},
    }

    for div_name, team_ids in DIVISIONS.items():
        league = "AL" if div_name.startswith("AL") else "NL"

        # Sort by wins descending
        div_teams = [(tid, team_states[tid]) for tid in team_ids]
        div_teams.sort(key=lambda x: x[1].wins, reverse=True)

        # Division winner
        winner_id = div_teams[0][0]
        results["division_winners"][div_name] = winner_id

    # Wild cards: top 3 non-division-winners per league
    div_winners = set(results["division_winners"].values())

    for league_prefix in ["AL", "NL"]:
        league_teams = []
        for div_name, team_ids in DIVISIONS.items():
            if not div_name.startswith(league_prefix):
                continue
            for tid in team_ids:
                if tid not in div_winners:
                    league_teams.append((tid, team_states[tid]))

        league_teams.sort(key=lambda x: x[1].wins, reverse=True)
        results["wild_cards"][league_prefix] = [t[0] for t in league_teams[:3]]

    return results


def simulate_series(team_a_id: int, team_b_id: int, games: int,
                    team_states: Dict[int, "TeamState"],
                    league_avg_rpg: float = 4.5) -> int:
    """Simulate a best-of-N playoff series using runs-based model. Returns winner's team_id."""
    wins_needed = (games // 2) + 1
    a_wins = 0
    b_wins = 0

    a = team_states[team_a_id]
    b = team_states[team_b_id]

    game_num = 0
    while a_wins < wins_needed and b_wins < wins_needed:
        game_num += 1
        # Home field pattern: team_a home for games 1,2,5,7
        if games == 3:  # Wild Card round: 1,2 home, 3 away
            a_is_home = game_num <= 2
        else:  # 5-game or 7-game: 1,2 home, 3,4 away, 5,6,7 home
            a_is_home = game_num in (1, 2, 5, 6, 7)

        # Odds-ratio run expectation
        if a_is_home:
            a_exp = a.runs_scored_pg * b.runs_allowed_pg / league_avg_rpg + 0.25
            b_exp = b.runs_scored_pg * a.runs_allowed_pg / league_avg_rpg - 0.25
        else:
            b_exp = b.runs_scored_pg * a.runs_allowed_pg / league_avg_rpg + 0.25
            a_exp = a.runs_scored_pg * b.runs_allowed_pg / league_avg_rpg - 0.25

        a_exp = max(1.5, a_exp)
        b_exp = max(1.5, b_exp)

        a_runs = np.random.poisson(a_exp)
        b_runs = np.random.poisson(b_exp)

        while a_runs == b_runs:
            a_runs += np.random.poisson(0.5)
            b_runs += np.random.poisson(0.5)

        if a_runs > b_runs:
            a_wins += 1
        else:
            b_wins += 1

    return team_a_id if a_wins >= wins_needed else team_b_id


def simulate_postseason(team_states: Dict[int, "TeamState"],
                        playoffs: Dict,
                        league_avg_rpg: float = 4.5) -> Dict[int, str]:
    """Simulate full postseason bracket. Returns {team_id: deepest_round_reached}.

    Rounds: 'wc', 'ds', 'cs', 'ws', 'champion'
    """
    advancement = {}  # team_id -> deepest round
    pennant_winners = {}  # "AL"/"NL" -> team_id

    for league in ["AL", "NL"]:
        # Get division winners sorted by wins (for seeding)
        div_winner_ids = []
        for div_name in sorted(DIVISIONS.keys()):
            if not div_name.startswith(league):
                continue
            winner = playoffs["division_winners"].get(div_name)
            if winner:
                div_winner_ids.append(winner)

        div_winner_ids.sort(key=lambda t: team_states[t].wins, reverse=True)
        wc_ids = playoffs["wild_cards"].get(league, [])

        # All playoff teams start with at least "made playoffs"
        for tid in div_winner_ids:
            advancement[tid] = "div_winner"
        for tid in wc_ids:
            advancement[tid] = "wc"

        if len(div_winner_ids) < 3 or len(wc_ids) < 3:
            continue

        # Seeds: 1=best div winner, 2=2nd, 3=3rd, 4=WC1, 5=WC2, 6=WC3
        seed1, seed2, seed3 = div_winner_ids[0], div_winner_ids[1], div_winner_ids[2]
        wc1, wc2, wc3 = wc_ids[0], wc_ids[1], wc_ids[2]

        # --- Wild Card Round (best of 3) ---
        # Seed 3 vs WC3 (worst WC), Seed 2 vs WC2 (middle WC)
        # Seed 1 gets bye? No — current MLB format: no byes.
        # Actual format: #3 vs #6, #4 vs #5, #1 vs #4/5 winner? No.
        # Current MLB (2024+): WC round = #3 vs #6, #4 vs #5 (best of 3)
        # DS: #1 vs winner(#4/#5), #2 vs winner(#3/#6) (best of 5)
        wc_a_winner = simulate_series(seed3, wc3, 3, team_states, league_avg_rpg)  # 3 vs 6
        wc_b_winner = simulate_series(wc1, wc2, 3, team_states, league_avg_rpg)    # 4 vs 5

        # Losers stay at "wc" round
        wc_a_loser = wc3 if wc_a_winner == seed3 else seed3
        wc_b_loser = wc2 if wc_b_winner == wc1 else wc1

        # Winners advance to DS
        advancement[wc_a_winner] = "ds"
        advancement[wc_b_winner] = "ds"

        # --- Division Series (best of 5) ---
        # #1 vs WC_B winner (4/5 bracket), #2 vs WC_A winner (3/6 bracket)
        ds_a_winner = simulate_series(seed1, wc_b_winner, 5, team_states, league_avg_rpg)
        ds_b_winner = simulate_series(seed2, wc_a_winner, 5, team_states, league_avg_rpg)

        advancement[ds_a_winner] = "cs"
        advancement[ds_b_winner] = "cs"

        # --- Championship Series (best of 7) ---
        # Higher seed gets home field
        if team_states[ds_a_winner].wins >= team_states[ds_b_winner].wins:
            cs_winner = simulate_series(ds_a_winner, ds_b_winner, 7, team_states, league_avg_rpg)
        else:
            cs_winner = simulate_series(ds_b_winner, ds_a_winner, 7, team_states, league_avg_rpg)

        advancement[cs_winner] = "ws"
        pennant_winners[league] = cs_winner

    # --- World Series (best of 7) ---
    if "AL" in pennant_winners and "NL" in pennant_winners:
        al_p, nl_p = pennant_winners["AL"], pennant_winners["NL"]
        if team_states[al_p].wins >= team_states[nl_p].wins:
            ws_winner = simulate_series(al_p, nl_p, 7, team_states, league_avg_rpg)
        else:
            ws_winner = simulate_series(nl_p, al_p, 7, team_states, league_avg_rpg)
        advancement[ws_winner] = "champion"

    return advancement


def simulate_season(season: int, n_sims: int = 1000,
                    after_date: str = None) -> Dict:
    """Run full season Monte Carlo simulation."""

    print(f"Loading schedule for {season}...")
    all_games = get_remaining_schedule(season, after_date)

    # Separate completed and remaining games
    completed = [g for g in all_games if g["status"] == "Final"]
    remaining = [g for g in all_games if g["status"] != "Final"]

    print(f"  {len(completed)} completed, {len(remaining)} remaining")

    if not remaining:
        print("No remaining games to simulate.")
        return {}

    print("Loading team data...")
    base_teams = load_team_data(season)

    # Track results across simulations
    season_results = defaultdict(lambda: {"wins": [], "losses": [],
                                            "made_playoffs": 0,
                                            "won_division": 0,
                                            "won_wild_card": 0,
                                            "made_ds": 0,
                                            "made_cs": 0,
                                            "made_ws": 0,
                                            "won_ws": 0})

    # Compute league average R/G for odds-ratio normalization
    all_rs = [ts.runs_scored_pg for ts in base_teams.values()]
    all_ra = [ts.runs_allowed_pg for ts in base_teams.values()]
    league_avg_rpg = (np.mean(all_rs) + np.mean(all_ra)) / 2
    print(f"  League avg R/G: {league_avg_rpg:.2f}")

    print(f"\nRunning {n_sims} season simulations...")

    for sim_num in range(n_sims):
        if (sim_num + 1) % 100 == 0:
            print(f"  Simulation {sim_num + 1}/{n_sims}")

        # Copy team states for this simulation
        team_states = {}
        for tid, ts in base_teams.items():
            team_states[tid] = TeamState(
                team_id=tid,
                wins=ts.wins,
                losses=ts.losses,
                rotation=ts.rotation[:],
                lineup=ts.lineup[:],
                bullpen=[b.copy() for b in ts.bullpen],
                catcher_framing=ts.catcher_framing,
                home_park_run_factor=ts.home_park_run_factor,
                home_park_hr_factor=ts.home_park_hr_factor,
                runs_scored_pg=ts.runs_scored_pg,
                runs_allowed_pg=ts.runs_allowed_pg,
            )

        # Simulate each remaining game using runs-based model
        # Odds-ratio method: expected runs = (team_offense * opp_pitching) / league_avg
        # This naturally produces zero-sum results (every game has exactly 1 winner)
        for game in remaining:
            home_id = game["home_team_id"]
            away_id = game["away_team_id"]

            if home_id not in team_states or away_id not in team_states:
                continue

            home = team_states[home_id]
            away = team_states[away_id]

            # Odds-ratio run expectation:
            # Home expected runs = home_RS * away_RA / league_avg_rpg
            # Away expected runs = away_RS * home_RA / league_avg_rpg
            home_exp_runs = home.runs_scored_pg * away.runs_allowed_pg / league_avg_rpg
            away_exp_runs = away.runs_scored_pg * home.runs_allowed_pg / league_avg_rpg

            # Home field advantage: +0.25 runs for home, -0.25 for away
            home_exp_runs += 0.25
            away_exp_runs -= 0.25

            # Floor at 1.5 runs expected
            home_exp_runs = max(1.5, home_exp_runs)
            away_exp_runs = max(1.5, away_exp_runs)

            # Sample runs from Poisson distribution
            home_runs = np.random.poisson(home_exp_runs)
            away_runs = np.random.poisson(away_exp_runs)

            # Handle ties with extra innings (keep re-rolling until someone wins)
            while home_runs == away_runs:
                # Extra innings: each team scores ~0.5 runs per extra inning on avg
                home_runs += np.random.poisson(0.5)
                away_runs += np.random.poisson(0.5)

            if home_runs > away_runs:
                home.wins += 1
                away.losses += 1
            else:
                home.losses += 1
                away.wins += 1

        # Determine playoffs and simulate postseason
        playoffs = determine_playoff_teams(team_states)
        postseason = simulate_postseason(team_states, playoffs, league_avg_rpg)

        # Record results
        # Round hierarchy for counting "at least made it to X"
        ROUND_DEPTH = {"wc": 1, "div_winner": 1, "ds": 2, "cs": 3, "ws": 4, "champion": 5}

        for tid, ts in team_states.items():
            season_results[tid]["wins"].append(ts.wins)
            season_results[tid]["losses"].append(ts.losses)

            deepest = postseason.get(tid)
            if deepest is None:
                continue  # didn't make playoffs

            depth = ROUND_DEPTH.get(deepest, 0)

            season_results[tid]["made_playoffs"] += 1

            # Division winner vs wild card
            is_div_winner = tid in playoffs["division_winners"].values()
            if is_div_winner:
                season_results[tid]["won_division"] += 1
            else:
                season_results[tid]["won_wild_card"] += 1

            # Postseason advancement
            if depth >= 2:
                season_results[tid]["made_ds"] += 1
            if depth >= 3:
                season_results[tid]["made_cs"] += 1
            if depth >= 4:
                season_results[tid]["made_ws"] += 1
            if depth >= 5:
                season_results[tid]["won_ws"] += 1

    # Compile results
    print(f"\n{'='*70}")
    print(f"SEASON PROJECTION: {season} ({n_sims} simulations)")
    print(f"{'='*70}")

    output = {}
    for div_name in sorted(DIVISIONS.keys()):
        team_ids = DIVISIONS[div_name]
        div_results = []

        for tid in team_ids:
            r = season_results[tid]
            wins = r["wins"]
            current = base_teams.get(tid)
            current_w = current.wins if current else 0
            current_l = current.losses if current else 0
            div_results.append({
                "team_id": tid,
                "abbrev": TEAM_ABBREVS.get(tid, str(tid)),
                "name": TEAM_NAMES.get(tid, str(tid)),
                "current_wins": current_w,
                "current_losses": current_l,
                "avg_wins": np.mean(wins),
                "avg_losses": np.mean(r["losses"]),
                "win_std": np.std(wins),
                "p10_wins": np.percentile(wins, 10),
                "p90_wins": np.percentile(wins, 90),
                "playoff_pct": r["made_playoffs"] / n_sims,
                "division_pct": r["won_division"] / n_sims,
                "wild_card_pct": r["won_wild_card"] / n_sims,
                "ds_pct": r["made_ds"] / n_sims,
                "cs_pct": r["made_cs"] / n_sims,
                "ws_pct": r["made_ws"] / n_sims,
                "win_ws_pct": r["won_ws"] / n_sims,
            })

        div_results.sort(key=lambda x: x["avg_wins"], reverse=True)

        print(f"\n{div_name}:")
        print(f"  {'Team':>5}  {'W':>5}  {'L':>5}  {'±':>4}  {'Playoff%':>8}  {'Div%':>5}  {'WC%':>5}  {'WS%':>5}")
        for dr in div_results:
            print(f"  {dr['abbrev']:>5}  {dr['avg_wins']:>5.1f}  {dr['avg_losses']:>5.1f}  "
                  f"{dr['win_std']:>4.1f}  {dr['playoff_pct']:>7.1%}  "
                  f"{dr['division_pct']:>5.1%}  {dr['wild_card_pct']:>5.1%}  "
                  f"{dr['win_ws_pct']:>5.1%}")

        output[div_name] = div_results

    # Add metadata
    output["_meta"] = {
        "season": season,
        "n_sims": n_sims,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "games_completed": len(completed),
        "games_remaining": len(remaining),
    }

    # Save results
    results_path = os.path.join(OUTPUT_DIR, f"season_projection_{season}.json")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(results_path, "w") as f:
        json.dump(output, f, indent=2, default=float)
    print(f"\nResults saved to {results_path}")

    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Season simulator")
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--n-sims", type=int, default=1000)
    parser.add_argument("--after-date", type=str, default=None,
                        help="Only simulate games after this date (YYYY-MM-DD)")
    args = parser.parse_args()

    simulate_season(args.season, args.n_sims, args.after_date)
