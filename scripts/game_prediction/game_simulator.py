#!/usr/bin/env python3
"""
Monte Carlo game simulation engine.

Given two teams (lineups, starters, bullpens, park, umpire, weather),
simulates a full game PA-by-PA using matchup model output probabilities.

Core flow per PA:
1. Look up batter/pitcher matchup probabilities (from matchup model)
2. Apply park factor adjustments
3. Apply umpire adjustments (K/BB rate shift)
4. Apply catcher framing adjustment
5. Apply pitcher fatigue/TTOP degradation
6. Sample PA outcome from adjusted probability distribution
7. Advance runners based on outcome + baserunning model
8. Track score, outs, pitcher pitch count

Handles pitcher changes (starter -> bullpen) based on workload model.

Output: GameResult with score, win probability, key stats.
"""

import os
import sys
import json
import math
import random
import joblib
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple

# Outcome classes (must match matchup model)
OUTCOMES = ["1B", "2B", "3B", "BB", "HBP", "HR", "IBB", "K", "OUT"]

# Base advancement probabilities on hits (simplified)
# Format: {outcome: {(runner_positions): (new_runner_positions, runs_scored)}}
# runner_positions represented as tuple of occupied bases (1, 2, 3)
# These are league-average defaults; the baserunning model adjusts them

# Runs scored by outcome with no runners on
BASE_RUNS = {"1B": 0, "2B": 0, "3B": 0, "HR": 1, "BB": 0, "HBP": 0, "IBB": 0, "K": 0, "OUT": 0}


@dataclass
class PitcherState:
    player_id: int
    pitch_count: int = 0
    batters_faced: int = 0
    runs_allowed: int = 0
    is_starter: bool = True
    times_thru_order: int = 1
    stuff_plus: float = 100.0
    control_plus: float = 100.0
    fatigue_factor: float = 1.0  # multiplier on performance (1.0 = fresh)
    days_rest: int = 4

    @property
    def is_fatigued(self):
        return self.pitch_count > 85 if self.is_starter else self.pitch_count > 25


@dataclass
class GameState:
    inning: int = 1
    half: str = "top"  # "top" or "bottom"
    outs: int = 0
    runners: List[int] = field(default_factory=list)  # base numbers with runners (1, 2, 3)
    home_score: int = 0
    away_score: int = 0
    batting_order_pos: Dict[str, int] = field(default_factory=lambda: {"away": 0, "home": 0})
    pa_count: int = 0

    @property
    def is_over(self):
        if self.inning < 9:
            return False
        if self.inning >= 9 and self.half == "bottom" and self.home_score > self.away_score:
            return True  # walk-off
        if self.inning >= 9 and self.half == "top" and self.outs >= 3:
            # Bottom of 9+ with home team ahead
            if self.home_score > self.away_score:
                return True
        if self.inning >= 10 and self.half == "top" and self.outs >= 3:
            if self.home_score != self.away_score:
                return True
        return False

    @property
    def batting_team(self):
        return "away" if self.half == "top" else "home"

    @property
    def fielding_team(self):
        return "home" if self.half == "top" else "away"


@dataclass
class TeamSetup:
    lineup: List[int]  # 9 batter IDs in order
    starter: int  # pitcher ID
    bullpen: List[Dict]  # [{id, stuff_plus, control_plus, days_rest, role}]
    catcher_framing_runs_per_game: float = 0.0


@dataclass
class GameConfig:
    home: TeamSetup
    away: TeamSetup
    park_run_factor: float = 1.0
    park_hr_factor: float = 1.0
    umpire_zone_factor: float = 1.0  # >1 = bigger zone (more Ks, fewer BBs)
    umpire_run_factor: float = 1.0
    weather_hr_factor: float = 1.0
    home_field_advantage: float = 0.02  # small boost to home team


@dataclass
class GameResult:
    home_score: int
    away_score: int
    home_win: bool
    innings: int
    total_pa: int


class GameSimulator:
    """Monte Carlo game simulator."""

    def __init__(self, matchup_probs: Dict = None):
        """
        matchup_probs: dict mapping (batter_id, pitcher_id) -> np.array of outcome probs
                       matching OUTCOMES order. If None, uses league average.
        """
        self.matchup_probs = matchup_probs or {}

        # League average outcome probabilities (2021-2025 approx)
        self.league_avg_probs = np.array([
            0.155,  # 1B
            0.045,  # 2B
            0.005,  # 3B
            0.080,  # BB
            0.012,  # HBP
            0.032,  # HR
            0.005,  # IBB
            0.225,  # K
            0.441,  # OUT
        ])

    def get_pa_probs(self, batter_id: int, pitcher: PitcherState,
                     game_state: GameState, config: GameConfig,
                     batting_team: str) -> np.ndarray:
        """Get adjusted PA outcome probabilities."""

        # Base probabilities from matchup model or league average
        key = (batter_id, pitcher.player_id)
        if key in self.matchup_probs:
            probs = self.matchup_probs[key].copy()
        else:
            probs = self.league_avg_probs.copy()

        # --- Apply adjustments ---

        # 1. Times through order penalty (starters get worse each time through)
        if pitcher.is_starter and pitcher.times_thru_order >= 3:
            ttop_penalty = 0.02 * (pitcher.times_thru_order - 2)
            # Shift probability toward hits, away from outs
            probs[OUTCOMES.index("K")] *= (1 - ttop_penalty)
            probs[OUTCOMES.index("OUT")] *= (1 - ttop_penalty * 0.5)
            probs[OUTCOMES.index("1B")] *= (1 + ttop_penalty)
            probs[OUTCOMES.index("HR")] *= (1 + ttop_penalty * 1.5)

        # 2. Pitcher fatigue (high pitch count)
        if pitcher.pitch_count > 80 and pitcher.is_starter:
            fatigue = (pitcher.pitch_count - 80) * 0.002
            probs[OUTCOMES.index("BB")] *= (1 + fatigue)
            probs[OUTCOMES.index("K")] *= (1 - fatigue * 0.5)
            probs[OUTCOMES.index("1B")] *= (1 + fatigue * 0.3)

        # 3. Park factors
        probs[OUTCOMES.index("HR")] *= config.park_hr_factor * config.weather_hr_factor
        # Run factor adjusts hits generally
        run_adj = (config.park_run_factor - 1.0) * 0.3  # dampen effect
        probs[OUTCOMES.index("1B")] *= (1 + run_adj)
        probs[OUTCOMES.index("2B")] *= (1 + run_adj)

        # 4. Umpire zone (bigger zone = more Ks, fewer BBs)
        ump_adj = config.umpire_zone_factor - 1.0
        probs[OUTCOMES.index("K")] *= (1 + ump_adj * 0.15)
        probs[OUTCOMES.index("BB")] *= (1 - ump_adj * 0.15)

        # 5. Catcher framing
        team = config.home if game_state.fielding_team == "home" else config.away
        framing = team.catcher_framing_runs_per_game
        # Positive framing = more Ks, fewer BBs
        probs[OUTCOMES.index("K")] *= (1 + framing * 0.02)
        probs[OUTCOMES.index("BB")] *= (1 - framing * 0.02)

        # 6. Home field advantage (slight boost to home batting)
        if batting_team == "home":
            hit_indices = [OUTCOMES.index(o) for o in ["1B", "2B", "3B", "HR"]]
            for idx in hit_indices:
                probs[idx] *= (1 + config.home_field_advantage)

        # Renormalize
        probs = np.maximum(probs, 0.001)  # floor
        probs /= probs.sum()

        return probs

    def advance_runners(self, runners: List[int], outcome: str,
                        outs: int) -> Tuple[List[int], int, int]:
        """
        Advance runners based on PA outcome.
        Returns: (new_runners, runs_scored, outs_added)
        """
        runs = 0
        new_outs = 0

        if outcome == "HR":
            runs = len(runners) + 1
            return [], runs, 0

        if outcome in ("K",):
            return runners, 0, 1

        if outcome == "OUT":
            new_outs = 1
            # Advance runner from 3B on sac fly with < 2 outs (simplified)
            new_runners = []
            if 3 in runners and outs < 2 and random.random() < 0.4:
                runs += 1
                new_runners = [r for r in runners if r != 3]
            else:
                # Ground ball: chance of double play with runner on 1st
                if 1 in runners and outs < 2 and random.random() < 0.35:
                    new_outs = 2
                    new_runners = [r for r in runners if r != 1]
                    # Runner from 3B scores on GDP sometimes
                    if 3 in new_runners and random.random() < 0.5:
                        runs += 1
                        new_runners = [r for r in new_runners if r != 3]
                else:
                    new_runners = runners[:]
                    # Advance some runners on outs
                    if 3 in new_runners and random.random() < 0.15:
                        runs += 1
                        new_runners = [r for r in new_runners if r != 3]

            return new_runners, runs, new_outs

        if outcome in ("BB", "HBP", "IBB"):
            new_runners = runners[:]
            # Force runners forward from first
            if 1 in new_runners:
                new_runners.remove(1)
                if 2 in new_runners:
                    new_runners.remove(2)
                    if 3 in new_runners:
                        runs += 1
                        new_runners.remove(3)
                    new_runners.append(3)
                new_runners.append(2)
            new_runners.append(1)
            return sorted(set(new_runners)), runs, 0

        if outcome == "1B":
            new_runners = []
            for r in runners:
                if r == 3:
                    runs += 1
                elif r == 2:
                    if random.random() < 0.60:
                        runs += 1
                    else:
                        new_runners.append(3)
                elif r == 1:
                    if random.random() < 0.30:
                        new_runners.append(3)
                    else:
                        new_runners.append(2)
            new_runners.append(1)
            return sorted(set(new_runners)), runs, 0

        if outcome == "2B":
            new_runners = []
            for r in runners:
                if r >= 2:
                    runs += 1
                elif r == 1:
                    if random.random() < 0.65:
                        runs += 1
                    else:
                        new_runners.append(3)
            new_runners.append(2)
            return sorted(set(new_runners)), runs, 0

        if outcome == "3B":
            runs += len(runners)
            return [3], runs, 0

        return runners, 0, 0

    def should_pull_starter(self, pitcher: PitcherState, game_state: GameState) -> bool:
        """Decide if starter should be pulled."""
        if not pitcher.is_starter:
            return False

        # Hard limits
        if pitcher.pitch_count >= 110:
            return True
        if pitcher.batters_faced >= 30:
            return True

        # Soft triggers
        if pitcher.pitch_count >= 95 and game_state.inning >= 6:
            return True
        if pitcher.pitch_count >= 85 and pitcher.times_thru_order >= 3:
            return True

        # Poor performance trigger
        if pitcher.runs_allowed >= 6:
            return True
        if pitcher.runs_allowed >= 4 and game_state.inning <= 4:
            return True

        return False

    def get_next_reliever(self, bullpen: List[Dict], used: set) -> Optional[Dict]:
        """Select next reliever from bullpen."""
        available = [p for p in bullpen if p["id"] not in used and p.get("days_rest", 1) >= 1]
        if not available:
            # Emergency: re-use someone
            available = [p for p in bullpen if p["id"] not in used]
        if not available:
            return None
        # Pick highest stuff+ available
        return max(available, key=lambda p: p.get("stuff_plus", 100))

    def simulate_game(self, config: GameConfig) -> GameResult:
        """Simulate one full game."""
        state = GameState()

        # Initialize pitchers
        pitchers = {
            "home": PitcherState(player_id=config.home.starter, is_starter=True),
            "away": PitcherState(player_id=config.away.starter, is_starter=True),
        }
        used_relievers = {"home": set(), "away": set()}

        max_innings = 15  # cap for extra innings

        while not state.is_over and state.inning <= max_innings:
            for half in ["top", "bottom"]:
                state.half = half
                state.outs = 0
                state.runners = []

                # Manfred runner in extras (runner on 2B)
                if state.inning >= 10:
                    state.runners = [2]

                batting_team = state.batting_team
                fielding_team = state.fielding_team
                pitcher = pitchers[fielding_team]

                lineup = config.home.lineup if batting_team == "home" else config.away.lineup
                bullpen = config.home.bullpen if fielding_team == "home" else config.away.bullpen

                while state.outs < 3:
                    # Check pitcher change
                    if self.should_pull_starter(pitcher, state) or (
                        not pitcher.is_starter and pitcher.batters_faced >= 6
                    ):
                        reliever = self.get_next_reliever(bullpen, used_relievers[fielding_team])
                        if reliever:
                            used_relievers[fielding_team].add(reliever["id"])
                            pitcher = PitcherState(
                                player_id=reliever["id"],
                                is_starter=False,
                                stuff_plus=reliever.get("stuff_plus", 100),
                                control_plus=reliever.get("control_plus", 100),
                                days_rest=reliever.get("days_rest", 1),
                            )
                            pitchers[fielding_team] = pitcher

                    # Get current batter
                    order_pos = state.batting_order_pos[batting_team]
                    batter_id = lineup[order_pos % len(lineup)]
                    state.batting_order_pos[batting_team] = order_pos + 1

                    # Track times through order for starters
                    if pitcher.is_starter and (order_pos + 1) % 9 == 0:
                        pitcher.times_thru_order += 1

                    # Get PA probabilities
                    probs = self.get_pa_probs(batter_id, pitcher, state, config, batting_team)

                    # Sample outcome
                    outcome = np.random.choice(OUTCOMES, p=probs)

                    # Advance runners
                    state.runners, runs, outs_added = self.advance_runners(
                        state.runners, outcome, state.outs
                    )

                    # Update scores
                    if batting_team == "home":
                        state.home_score += runs
                    else:
                        state.away_score += runs

                    state.outs += outs_added
                    state.pa_count += 1
                    pitcher.pitch_count += random.randint(3, 6)  # approximate pitches per PA
                    pitcher.batters_faced += 1
                    pitcher.runs_allowed += runs

                    # Walk-off check
                    if (state.inning >= 9 and half == "bottom" and
                            state.home_score > state.away_score):
                        break

                # Check walk-off
                if (state.inning >= 9 and half == "bottom" and
                        state.home_score > state.away_score):
                    break

                # Bottom of 9+ home team ahead, skip home batting
                if (half == "top" and state.inning >= 9 and
                        state.home_score > state.away_score):
                    break

            state.inning += 1

        # Handle ties at max innings (coin flip, rare)
        if state.home_score == state.away_score:
            if random.random() < 0.5:
                state.home_score += 1
            else:
                state.away_score += 1

        return GameResult(
            home_score=state.home_score,
            away_score=state.away_score,
            home_win=state.home_score > state.away_score,
            innings=state.inning - 1,
            total_pa=state.pa_count,
        )

    def simulate_many(self, config: GameConfig, n_sims: int = 10000) -> Dict:
        """Run many simulations and return aggregate results."""
        results = [self.simulate_game(config) for _ in range(n_sims)]

        home_wins = sum(1 for r in results if r.home_win)
        home_scores = [r.home_score for r in results]
        away_scores = [r.away_score for r in results]
        total_runs = [r.home_score + r.away_score for r in results]

        return {
            "n_sims": n_sims,
            "home_win_pct": home_wins / n_sims,
            "away_win_pct": 1 - home_wins / n_sims,
            "avg_home_score": np.mean(home_scores),
            "avg_away_score": np.mean(away_scores),
            "avg_total_runs": np.mean(total_runs),
            "home_score_dist": {
                "mean": np.mean(home_scores),
                "std": np.std(home_scores),
                "median": np.median(home_scores),
                "p10": np.percentile(home_scores, 10),
                "p90": np.percentile(home_scores, 90),
            },
            "away_score_dist": {
                "mean": np.mean(away_scores),
                "std": np.std(away_scores),
                "median": np.median(away_scores),
                "p10": np.percentile(away_scores, 10),
                "p90": np.percentile(away_scores, 90),
            },
            "run_line": {  # spread analysis
                "home_minus_1.5_pct": sum(1 for r in results if r.home_score - r.away_score > 1.5) / n_sims,
                "away_minus_1.5_pct": sum(1 for r in results if r.away_score - r.home_score > 1.5) / n_sims,
            },
            "over_under": {
                score: sum(1 for r in results if r.home_score + r.away_score > score) / n_sims
                for score in [6.5, 7.0, 7.5, 8.0, 8.5, 9.0, 9.5]
            },
        }


# --- Convenience function to run a pregame prediction ---

def predict_game(
    home_lineup: List[int],
    away_lineup: List[int],
    home_starter: int,
    away_starter: int,
    home_bullpen: List[Dict] = None,
    away_bullpen: List[Dict] = None,
    matchup_probs: Dict = None,
    park_run_factor: float = 1.0,
    park_hr_factor: float = 1.0,
    umpire_zone_factor: float = 1.0,
    weather_hr_factor: float = 1.0,
    home_catcher_framing: float = 0.0,
    away_catcher_framing: float = 0.0,
    n_sims: int = 10000,
) -> Dict:
    """
    Run a pregame prediction.

    Returns dict with win probabilities, score distributions, run lines, O/U.
    """
    config = GameConfig(
        home=TeamSetup(
            lineup=home_lineup,
            starter=home_starter,
            bullpen=home_bullpen or [],
            catcher_framing_runs_per_game=home_catcher_framing,
        ),
        away=TeamSetup(
            lineup=away_lineup,
            starter=away_starter,
            bullpen=away_bullpen or [],
            catcher_framing_runs_per_game=away_catcher_framing,
        ),
        park_run_factor=park_run_factor,
        park_hr_factor=park_hr_factor,
        umpire_zone_factor=umpire_zone_factor,
        weather_hr_factor=weather_hr_factor,
    )

    sim = GameSimulator(matchup_probs=matchup_probs)
    return sim.simulate_many(config, n_sims=n_sims)


if __name__ == "__main__":
    # Demo with league-average everything
    print("Running demo simulation (league average teams)...")
    result = predict_game(
        home_lineup=[1, 2, 3, 4, 5, 6, 7, 8, 9],
        away_lineup=[10, 11, 12, 13, 14, 15, 16, 17, 18],
        home_starter=100,
        away_starter=200,
        n_sims=10000,
    )
    print(f"\nHome win%: {result['home_win_pct']:.3f}")
    print(f"Avg score: {result['avg_home_score']:.1f} - {result['avg_away_score']:.1f}")
    print(f"Avg total: {result['avg_total_runs']:.1f}")
    print(f"\nOver/Under probabilities:")
    for line, pct in result["over_under"].items():
        print(f"  O {line}: {pct:.3f}")
