"""
Stolen Base Break-Even Matrix

For each base/out state, calculates the minimum probability of a successful
stolen base attempt needed for the attempt to yield a positive expected run
differential.

Formula:
  p * (RE_success + runs_scored) + (1-p) * RE_failure = RE_current
  p_breakeven = (RE_current - RE_failure) / (RE_success + runs_scored - RE_failure)

Self-contained: loads advancement_probs via psycopg2, embeds MC simulator.
"""

import os

import numpy as np
import pandas as pd
import psycopg2

# ============================================
# DATABASE CONFIG
# ============================================

def _db_url() -> str:
    url = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_URL_PG") or ""
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


# ============================================
# LOAD ADVANCEMENT MATRIX
# ============================================

def load_advancement_lookup():
    conn = psycopg2.connect(_db_url())
    df = pd.read_sql(
        """
        SELECT event_type, base_state, outs, spray_bucket,
               next_base_state, runs_scored, outs_added, probability
        FROM advancement_probs
        """,
        conn
    )
    conn.close()

    lookup = {}
    for key, g in df.groupby(["event_type", "base_state", "outs", "spray_bucket"]):
        lookup[key] = (
            g["next_base_state"].astype(int).to_numpy(),
            g["runs_scored"].astype(int).to_numpy(),
            g["outs_added"].astype(int).to_numpy(),
            g["probability"].astype(float).to_numpy(),
        )
    return lookup


# ============================================
# EVENT PROBABILITIES
# ============================================

K_RATE   = 0.22
BB_RATE  = 0.085
HBP_RATE = 0.01

BIP_MIX = {
    "GB_out": 0.3215, "1B": 0.2108, "FB_out": 0.1940, "LD_out": 0.0879,
    "PU_out": 0.0690, "2B": 0.0651, "HR": 0.0460, "3B": 0.0056
}

def build_event_probs():
    bip_scale = 1 - (K_RATE + BB_RATE + HBP_RATE)
    probs = {"K": K_RATE, "BB": BB_RATE, "HBP": HBP_RATE}
    for k, v in BIP_MIX.items():
        probs[k] = v * bip_scale
    return probs


# ============================================
# WALK / HBP RUNNER ADVANCEMENT
# ============================================

def force_walk(base_state):
    on1 = base_state & 1
    on2 = base_state & 2
    on3 = base_state & 4
    runs = 0
    if on1 and on2 and on3:
        runs += 1
        return 7, runs
    if on1 and on2:
        return 7, runs
    if on1:
        return 3, runs
    return base_state | 1, runs


SPRAY_DIST = [0.38, 0.34, 0.28]


# ============================================
# MONTE CARLO HALF-INNING
# ============================================

def simulate_half_inning(base_state, outs, sims, adv_lookup, event_probs):
    events = list(event_probs.keys())
    probs = np.array(list(event_probs.values()))
    probs = probs / probs.sum()

    total_runs = 0
    for _ in range(sims):
        bs = base_state
        o = outs
        runs = 0
        while o < 3:
            event = np.random.choice(events, p=probs)
            if event == "K":
                o += 1
                continue
            if event in ("BB", "HBP"):
                bs, r = force_walk(bs)
                runs += r
                continue
            spray_bucket = np.random.choice([0, 1, 2], p=SPRAY_DIST)
            key = (event, bs, o, spray_bucket)
            if key not in adv_lookup:
                if event.endswith("_out"):
                    o += 1
                    continue
                if event == "HR":
                    runs += ((bs & 1) > 0) + ((bs & 2) > 0) + ((bs & 4) > 0) + 1
                    bs = 0
                    continue
                # Rare hit combos missing from lookup (e.g. triple with bases loaded)
                # Fall back: batter gets on, all runners score
                if event == "3B":
                    runs += ((bs & 1) > 0) + ((bs & 2) > 0) + ((bs & 4) > 0)
                    bs = 4  # batter on 3rd
                    continue
                if event == "2B":
                    runs += ((bs & 2) > 0) + ((bs & 4) > 0)
                    bs = 2 | (4 if (bs & 1) else 0)  # runner on 1st goes to 3rd
                    continue
                if event == "1B":
                    runs += ((bs & 4) > 0)
                    new_bs = 1
                    if bs & 2:
                        new_bs |= 4  # 2nd to 3rd
                    if bs & 1:
                        new_bs |= 2  # 1st to 2nd
                    bs = new_bs
                    continue
                continue
            next_states, run_vals, outs_vals, p = adv_lookup[key]
            idx = np.random.choice(len(p), p=p)
            bs = int(next_states[idx])
            runs += int(run_vals[idx])
            o += int(outs_vals[idx])
        total_runs += runs
    return total_runs / sims


# ============================================
# BASE STATE LABELS
# ============================================

BASE_LABELS = {
    0: "---", 1: "1--", 2: "-2-", 3: "12-",
    4: "--3", 5: "1-3", 6: "-23", 7: "123",
}

# ============================================
# STEAL SCENARIOS
# ============================================

# (base_state, description, success_bs, runs_scored, failure_bs)
STEAL_SCENARIOS = [
    (1, "1st steals 2nd",    2, 0, 0),
    (2, "2nd steals 3rd",    4, 0, 0),
    (3, "Lead steals 3rd",   5, 0, 1),  # only runner on 2nd goes; caught = 1st only
    (3, "Double steal",      6, 0, 2),  # both advance; caught = lead out at 3rd, trail at 2nd
    (5, "1st steals 2nd",    6, 0, 4),
]


# ============================================
# BUILD RUN EXPECTANCY MATRIX
# ============================================

def build_re_matrix(sims, adv_lookup, event_probs):
    re = {}
    for bs in range(8):
        for outs in range(3):
            re[(bs, outs)] = simulate_half_inning(bs, outs, sims, adv_lookup, event_probs)
            print(f"  RE({BASE_LABELS[bs]}, {outs} out) = {re[(bs, outs)]:.4f}")
    return re


# ============================================
# CALCULATE BREAK-EVEN PROBABILITIES
# ============================================

def build_breakeven_matrix(re):
    results = []
    for bs, desc, success_bs, runs_scored, failure_bs in STEAL_SCENARIOS:
        for outs in range(3):
            re_current = re[(bs, outs)]
            re_success = re[(success_bs, outs)] + runs_scored
            if outs + 1 >= 3:
                re_failure = 0.0
            else:
                re_failure = re[(failure_bs, outs + 1)]
            denom = re_success - re_failure
            if denom <= 0:
                breakeven = 1.0
            else:
                breakeven = (re_current - re_failure) / denom
            breakeven = max(0.0, min(1.0, breakeven))
            results.append({
                "base_state": BASE_LABELS[bs],
                "outs": outs,
                "steal": desc,
                "re_current": re_current,
                "re_success": re_success,
                "re_failure": re_failure,
                "breakeven_pct": breakeven * 100,
            })
    return results


# ============================================
# DISPLAY
# ============================================

def print_matrix(results):
    print("\n" + "=" * 80)
    print("STOLEN BASE BREAK-EVEN MATRIX")
    print("Minimum success probability needed for a +EV steal attempt")
    print("=" * 80)

    print(f"\n{'Bases':<8} {'Steal':<20} {'0 Out':>8} {'1 Out':>8} {'2 Out':>8}")
    print("-" * 56)

    by_row = {}
    for r in results:
        key = (r["base_state"], r["steal"])
        if key not in by_row:
            by_row[key] = {}
        by_row[key][r["outs"]] = r["breakeven_pct"]

    for bs, desc, *_ in STEAL_SCENARIOS:
        label = BASE_LABELS[bs]
        info = by_row[(label, desc)]
        vals = [f"{info.get(o, 0):.1f}%" for o in range(3)]
        print(f"{label:<8} {desc:<20} {vals[0]:>8} {vals[1]:>8} {vals[2]:>8}")

    print(f"\n{'=' * 80}")
    print("DETAILED BREAKDOWN")
    print(f"{'=' * 80}")
    print(f"\n{'Bases':<8} {'Outs':>4} {'Steal':<20} {'RE Now':>8} {'RE Succ':>8} {'RE Fail':>8} {'Break-Even':>10}")
    print("-" * 72)
    for r in results:
        print(f"{r['base_state']:<8} {r['outs']:>4} {r['steal']:<20} "
              f"{r['re_current']:>8.3f} {r['re_success']:>8.3f} {r['re_failure']:>8.3f} "
              f"{r['breakeven_pct']:>9.1f}%")


# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Stolen Base Break-Even Matrix")
    parser.add_argument("--sims", type=int, default=10000, help="MC sims per state (default: 10000)")
    args = parser.parse_args()

    print("Loading advancement matrix from database...")
    adv_lookup = load_advancement_lookup()
    event_probs = build_event_probs()

    print(f"Running {args.sims} simulations per base/out state...")
    print("Building run expectancy matrix...\n")

    re = build_re_matrix(args.sims, adv_lookup, event_probs)
    results = build_breakeven_matrix(re)
    print_matrix(results)
