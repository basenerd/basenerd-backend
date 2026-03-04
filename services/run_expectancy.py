import numpy as np
import pandas as pd
from sqlalchemy import create_engine

# ============================================
# DATABASE CONFIG
# ============================================

DB_USER = "basenerd_user"
DB_PASS = "d5LmELIOiEszYPBSLSDT1oIi79gkgDV6"
DB_HOST = "dpg-d5i0tku3jp1c73f1d3gg-a.oregon-postgres.render.com"
DB_PORT = 5432
DB_NAME = "basenerd"

def build_engine():
    url = f"postgresql+pg8000://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


# ============================================
# LOAD ADVANCEMENT MATRIX
# ============================================

def load_advancement_lookup():

    engine = build_engine()

    df = pd.read_sql(
        """
        SELECT
            event_type,
            base_state,
            outs,
            spray_bucket,
            next_base_state,
            runs_scored,
            outs_added,
            probability
        FROM advancement_probs
        """,
        engine
    )

    lookup = {}

    for key, g in df.groupby(
        ["event_type","base_state","outs","spray_bucket"]
    ):

        lookup[key] = (
            g["next_base_state"].astype(int).to_numpy(),
            g["runs_scored"].astype(int).to_numpy(),
            g["outs_added"].astype(int).to_numpy(),
            g["probability"].astype(float).to_numpy(),
        )

    return lookup


ADV_LOOKUP = load_advancement_lookup()


# ============================================
# EVENT PROBABILITIES
# ============================================

# League average non-contact rates
K_RATE   = 0.22
BB_RATE  = 0.085
HBP_RATE = 0.01

# BIP event mix (your measured distribution)
BIP_MIX = {
    "GB_out": 0.3215,
    "1B":     0.2108,
    "FB_out": 0.1940,
    "LD_out": 0.0879,
    "PU_out": 0.0690,
    "2B":     0.0651,
    "HR":     0.0460,
    "3B":     0.0056
}

def build_event_probs():

    bip_scale = 1 - (K_RATE + BB_RATE + HBP_RATE)

    probs = {
        "K": K_RATE,
        "BB": BB_RATE,
        "HBP": HBP_RATE
    }

    for k,v in BIP_MIX.items():
        probs[k] = v * bip_scale

    return probs


EVENT_PROBS = build_event_probs()


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


# ============================================
# SPRAY DISTRIBUTION
# ============================================

SPRAY_DIST = [0.38, 0.34, 0.28]   # pull, center, oppo


# ============================================
# MONTE CARLO HALF-INNING
# ============================================

def simulate_half_inning(base_state, outs, sims=500):

    events = list(EVENT_PROBS.keys())
    probs = np.array(list(EVENT_PROBS.values()))
    probs = probs / probs.sum()

    total_runs = 0

    for _ in range(sims):

        bs = base_state
        o = outs
        runs = 0

        while o < 3:

            event = np.random.choice(events, p=probs)

            # Strikeout
            if event == "K":
                o += 1
                continue

            # Walk / HBP
            if event in ("BB","HBP"):
                bs, r = force_walk(bs)
                runs += r
                continue

            # Contact event
            spray_bucket = np.random.choice([0,1,2], p=SPRAY_DIST)

            key = (event, bs, o, spray_bucket)

            if key not in ADV_LOOKUP:

                if event.endswith("_out"):
                    o += 1
                    continue

                if event == "HR":
                    runs += ((bs & 1)>0) + ((bs & 2)>0) + ((bs & 4)>0) + 1
                    bs = 0
                    continue

            next_states, run_vals, outs_vals, p = ADV_LOOKUP[key]

            idx = np.random.choice(len(p), p=p)

            bs = int(next_states[idx])
            runs += int(run_vals[idx])
            o += int(outs_vals[idx])

        total_runs += runs

    return total_runs / sims


# ============================================
# PUBLIC FUNCTION
# ============================================

def get_expected_runs(base_state, outs):

    return simulate_half_inning(base_state, outs, sims=500)
