"""
Matchup prediction service.

Loads the XGBoost matchup model + pitch selection model + precomputed profiles
and predicts:
1. PA outcome probabilities (K, OUT, BB, 1B, 2B, 3B, HR, etc.)
2. Expected pitch usage from the pitcher

Uses pitcher's arsenal + batter's per-pitch-type performance for richer predictions.
"""

import os
import json
import logging
from datetime import date
from functools import lru_cache

import joblib
import numpy as np
import pandas as pd
import requests

log = logging.getLogger(__name__)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MODEL_PATH = os.path.join(_ROOT, "models", "matchup_model.joblib")
_META_PATH = os.path.join(_ROOT, "models", "matchup_model_meta.json")
_PITCH_SEL_PATH = os.path.join(_ROOT, "models", "pitch_selection_model.joblib")
_PITCH_SEL_META_PATH = os.path.join(_ROOT, "models", "pitch_selection_meta.json")
_DATA_DIR = os.path.join(_ROOT, "data")

# Lazy-loaded globals
_model = None
_meta = None
_pitch_sel = None      # dict with model, label_encoder, etc.
_pitch_sel_meta = None
_batter_profiles = None
_batter_pitch_types = None
_pitcher_arsenal = None
_park_factors = None

CLASSES = ["1B", "2B", "3B", "BB", "HBP", "HR", "IBB", "K", "OUT"]

PITCH_CATEGORY = {
    "FF": "fastball", "SI": "fastball", "FC": "fastball",
    "SL": "breaking", "CU": "breaking", "KC": "breaking", "ST": "breaking", "SV": "breaking",
    "CH": "offspeed", "FS": "offspeed", "KN": "offspeed",
}

# Friendly pitch type names
PITCH_NAMES = {
    "FF": "4-Seam", "SI": "Sinker", "FC": "Cutter",
    "SL": "Slider", "CU": "Curveball", "CH": "Changeup",
    "FS": "Splitter", "KC": "Knuckle Curve", "ST": "Sweeper",
    "SV": "Slurve", "KN": "Knuckleball",
}

# League-average fallbacks (2021-2025 approx)
_LEAGUE_AVG_BATTER = {
    "bat_k_pct": 0.225, "bat_bb_pct": 0.082, "bat_whiff_rate": 0.245,
    "bat_chase_rate": 0.295, "bat_zone_swing_rate": 0.68, "bat_zone_contact_rate": 0.82,
    "bat_avg_ev": 88.5, "bat_avg_la": 12.0, "bat_barrel_rate": 0.07,
    "bat_hard_hit_rate": 0.35, "bat_sweet_spot_rate": 0.33,
    "bat_gb_rate": 0.43, "bat_fb_rate": 0.36, "bat_hr_per_fb": 0.12,
    "bat_iso": 0.155, "bat_babip": 0.295, "bat_xwoba": 0.315,
}

_LEAGUE_AVG_BATTER_PLAT = {
    "bat_plat_k_pct": 0.225, "bat_plat_bb_pct": 0.082,
    "bat_plat_whiff_rate": 0.245, "bat_plat_chase_rate": 0.295,
    "bat_plat_avg_ev": 88.5, "bat_plat_barrel_rate": 0.07,
    "bat_plat_xwoba": 0.315,
}

_LEAGUE_AVG_BVPT = {
    "bvpt_whiff_rate": 0.245, "bvpt_chase_rate": 0.295,
    "bvpt_zone_contact_rate": 0.82, "bvpt_hard_hit_rate": 0.35,
    "bvpt_xwoba": 0.315,
}

_LEAGUE_AVG_PITCHER = {
    "p_avg_stuff_plus": 100.0, "p_avg_control_plus": 100.0,
    "p_avg_velo": 93.5, "p_whiff_rate": 0.245, "p_chase_rate": 0.295,
    "p_zone_rate": 0.45, "p_xwoba": 0.315,
    "p_num_pitches": 5, "p_total_thrown": 2500,
}

_LEAGUE_AVG_BATTER_R14 = {
    "bat_r14_k_pct": 0.225, "bat_r14_bb_pct": 0.082,
    "bat_r14_xwoba": 0.315, "bat_r14_barrel_rate": 0.07,
    "bat_r14_whiff_rate": 0.245, "bat_r14_chase_rate": 0.295,
}

_LEAGUE_AVG_PITCHER_R14 = {
    "p_r14_k_pct": 0.225, "p_r14_bb_pct": 0.082,
    "p_r14_xwoba": 0.315, "p_r14_whiff_rate": 0.245,
    "p_r14_chase_rate": 0.295,
}


def _load():
    """Lazy-load model, metadata, and profile DataFrames."""
    global _model, _meta, _pitch_sel, _pitch_sel_meta
    global _batter_profiles, _batter_pitch_types, _pitcher_arsenal, _park_factors

    if _model is not None:
        return

    try:
        _model = joblib.load(_MODEL_PATH)
        with open(_META_PATH) as f:
            _meta = json.load(f)
        log.info("Matchup model loaded (%d features)", _model.n_features_in_)
    except Exception as e:
        log.warning("Could not load matchup model: %s", e)
        _model = None
        return

    # Pitch selection model (optional — enhances output but not required)
    try:
        _pitch_sel = joblib.load(_PITCH_SEL_PATH)
        with open(_PITCH_SEL_META_PATH) as f:
            _pitch_sel_meta = json.load(f)
        log.info("Pitch selection model loaded")
    except Exception as e:
        log.info("Pitch selection model not available: %s", e)
        _pitch_sel = None

    for name, path, attr in [
        ("Batter profiles", "batter_profiles.parquet", "_batter_profiles"),
        ("Batter pitch-type profiles", "batter_pitch_type_profiles.parquet", "_batter_pitch_types"),
        ("Pitcher arsenal", "pitcher_arsenal.parquet", "_pitcher_arsenal"),
        ("Park factors", "park_factors.parquet", "_park_factors"),
    ]:
        try:
            df = pd.read_parquet(os.path.join(_DATA_DIR, path))
            globals()[attr] = df
            log.info("%s: %d rows", name, len(df))
        except Exception as e:
            log.warning("Could not load %s: %s", name, e)
            globals()[attr] = pd.DataFrame()


def _get_batter_features(batter_id, season, p_throws):
    """Look up batter profile features. Falls back to league average."""
    if _batter_profiles is None or _batter_profiles.empty:
        return {**_LEAGUE_AVG_BATTER, **_LEAGUE_AVG_BATTER_PLAT}

    bp = _batter_profiles
    mask_all = (bp["batter"] == batter_id) & (bp["vs_hand"] == "ALL")

    row_all = bp[mask_all & (bp["season"] == season)]
    if row_all.empty:
        row_all = bp[mask_all].sort_values("season", ascending=False).head(1)

    if row_all.empty:
        overall = dict(_LEAGUE_AVG_BATTER)
    else:
        r = row_all.iloc[0]
        overall = {}
        for k, default in _LEAGUE_AVG_BATTER.items():
            col = k.replace("bat_", "", 1)
            val = r.get(col)
            overall[k] = float(val) if pd.notna(val) else default

    # Platoon split
    plat_hand = p_throws if p_throws in ("L", "R") else "R"
    mask_plat = (bp["batter"] == batter_id) & (bp["vs_hand"] == plat_hand)
    row_plat = bp[mask_plat & (bp["season"] == season)]
    if row_plat.empty:
        row_plat = bp[mask_plat].sort_values("season", ascending=False).head(1)

    if row_plat.empty:
        plat = dict(_LEAGUE_AVG_BATTER_PLAT)
    else:
        r = row_plat.iloc[0]
        plat = {}
        for k, default in _LEAGUE_AVG_BATTER_PLAT.items():
            col = k.replace("bat_plat_", "", 1)
            val = r.get(col)
            plat[k] = float(val) if pd.notna(val) else default

    return {**overall, **plat}


def _get_batter_pitch_type_features(batter_id, season, pitcher_usage):
    """
    Look up batter-vs-pitch-type category stats and compute pitch-weighted composites.

    pitcher_usage: dict like {"fastball": 0.55, "breaking": 0.30, "offspeed": 0.15}
    """
    features = {}
    stats = ["whiff_rate", "chase_rate", "zone_contact_rate", "hard_hit_rate", "xwoba"]

    if _batter_pitch_types is None or _batter_pitch_types.empty:
        # Fill all with league average
        for cat in ["fastball", "breaking", "offspeed"]:
            for stat in stats:
                features[f"bvpt_{stat}_{cat}"] = _LEAGUE_AVG_BVPT[f"bvpt_{stat}"]
        for stat in stats:
            features[f"bvpt_w_{stat}"] = _LEAGUE_AVG_BVPT[f"bvpt_{stat}"]
        return features

    bpt = _batter_pitch_types
    for cat in ["fastball", "breaking", "offspeed"]:
        cat_key = f"CAT_{cat}"
        mask = (bpt["batter"] == batter_id) & (bpt["pitch_type"] == cat_key)
        row = bpt[mask & (bpt["season"] == season)]
        if row.empty:
            row = bpt[mask].sort_values("season", ascending=False).head(1)

        for stat in stats:
            if row.empty:
                features[f"bvpt_{stat}_{cat}"] = _LEAGUE_AVG_BVPT[f"bvpt_{stat}"]
            else:
                val = row.iloc[0].get(stat)
                features[f"bvpt_{stat}_{cat}"] = float(val) if pd.notna(val) else _LEAGUE_AVG_BVPT[f"bvpt_{stat}"]

    # Pitch-weighted composites
    for stat in stats:
        weighted = 0.0
        total_w = 0.0
        for cat in ["fastball", "breaking", "offspeed"]:
            w = pitcher_usage.get(cat, 0.33)
            v = features[f"bvpt_{stat}_{cat}"]
            weighted += v * w
            total_w += w
        features[f"bvpt_w_{stat}"] = weighted / total_w if total_w > 0 else _LEAGUE_AVG_BVPT[f"bvpt_{stat}"]

    return features


_API_BASE = "https://statsapi.mlb.com/api/v1"

# Cache for live API pitcher lookups (cleared on app restart)
_api_pitcher_cache = {}


def _fetch_pitcher_from_api(pitcher_id, season):
    """
    Fetch pitcher arsenal + season stats from the MLB API.
    Returns a DataFrame matching pitcher_arsenal.parquet schema, or None.
    """
    cache_key = (pitcher_id, season)
    if cache_key in _api_pitcher_cache:
        return _api_pitcher_cache[cache_key]

    try:
        url = (
            f"{_API_BASE}/people/{pitcher_id}/stats"
            f"?stats=statsSingleSeason,expectedStatistics,pitchArsenal"
            f"&season={season}&group=pitching"
        )
        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning("MLB API fetch for pitcher %s failed: %s", pitcher_id, e)
        _api_pitcher_cache[cache_key] = None
        return None

    # Parse pitchArsenal
    arsenal_rows = []
    season_stats = {}
    xstats = {}

    for sg in data.get("stats", []):
        display = sg.get("type", {}).get("displayName", "")
        splits = sg.get("splits", [])

        if display == "pitchArsenal":
            for s in splits:
                st = s.get("stat", {})
                pt_info = st.get("type", {})
                arsenal_rows.append({
                    "pitch_type": pt_info.get("code", "??"),
                    "avg_velo": st.get("averageSpeed"),
                    "n": st.get("count", 0),
                    "usage": st.get("percentage", 0),
                })

        elif display == "statsSingleSeason":
            for s in splits:
                season_stats = s.get("stat", {})

        elif display == "expectedStatistics":
            for s in splits:
                xstats = s.get("stat", {})

    if not arsenal_rows:
        _api_pitcher_cache[cache_key] = None
        return None

    # Build a mini DataFrame matching parquet schema
    total_pitches = sum(r["n"] for r in arsenal_rows)
    bf = int(season_stats.get("battersFaced", 0)) or 1
    k_total = int(season_stats.get("strikeOuts", 0))
    bb_total = int(season_stats.get("baseOnBalls", 0))

    # Derive aggregate rates from season stats.
    # For very small samples (early season), regress toward league average
    # to prevent extreme rates from producing unrealistic whiff estimates.
    lg_k_rate = 0.225
    lg_bb_rate = 0.082
    if bf >= 15:
        k_rate = k_total / bf
        bb_rate = bb_total / bf
    else:
        # Bayesian blend: weight observed rate by BF, league avg by prior strength
        prior_bf = 20  # pseudo-count for prior
        k_rate = (k_total + prior_bf * lg_k_rate) / (bf + prior_bf)
        bb_rate = (bb_total + prior_bf * lg_bb_rate) / (bf + prior_bf)

    # xwOBA from expected stats
    xwoba_val = None
    try:
        xwoba_val = float(xstats.get("woba", 0.315))
    except (TypeError, ValueError):
        xwoba_val = 0.315

    rows = []
    for ar in arsenal_rows:
        rows.append({
            "pitcher": pitcher_id,
            "season": season,
            "stand": "ALL",
            "pitch_type": ar["pitch_type"],
            "n": ar["n"],
            "usage": ar["usage"],
            "avg_velo": ar["avg_velo"],
            "avg_spin": None,
            "avg_hb": None,
            "avg_ivb": None,
            "avg_stuff_plus": None,
            "avg_control_plus": None,
            # We don't have per-pitch whiff/chase/zone from the API,
            # so we estimate from overall K rate + pitch type heuristics
            "whiff_rate": _estimate_pitch_whiff(ar["pitch_type"], k_rate),
            "zone_rate": 0.45,  # league average fallback
            "chase_rate": 0.295,
            "xwoba": xwoba_val,
        })

    df = pd.DataFrame(rows)
    _api_pitcher_cache[cache_key] = df
    return df


def _estimate_pitch_whiff(pitch_type, overall_k_rate):
    """Estimate per-pitch whiff rate from pitch type and overall K rate."""
    # Relative whiff multipliers by pitch category
    # Breaking/offspeed pitch types tend to have higher whiff rates
    multipliers = {
        "FF": 0.85, "SI": 0.65, "FC": 0.90,
        "SL": 1.25, "CU": 1.15, "CH": 1.20,
        "FS": 1.15, "ST": 1.30, "SV": 1.20,
        "KC": 1.15, "KN": 0.90,
    }
    base_whiff = overall_k_rate * 1.1  # K rate is correlated but not equal to whiff rate
    mult = multipliers.get(pitch_type, 1.0)
    return min(base_whiff * mult, 0.55)


_stuff_plus_cache = {}


def _get_stuff_plus_from_live(pitcher_id):
    """
    Look up average stuff+ for a pitcher from statcast_pitches_live (2026 spring data).
    Returns a dict {pitch_type: avg_stuff_plus} or empty dict.
    """
    if pitcher_id in _stuff_plus_cache:
        return _stuff_plus_cache[pitcher_id]

    try:
        import psycopg
        db_url = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_URL_PG") or ""
        if not db_url:
            return {}
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT pitch_type, AVG(stuff_plus) AS avg_stuff
                    FROM statcast_pitches_live
                    WHERE pitcher = %s AND stuff_plus IS NOT NULL
                    GROUP BY pitch_type
                """, (pitcher_id,))
                rows = cur.fetchall()
                result = {row[0]: float(row[1]) for row in rows}
                _stuff_plus_cache[pitcher_id] = result
                return result
    except Exception as e:
        log.debug("stuff+ live lookup failed for %s: %s", pitcher_id, e)
        _stuff_plus_cache[pitcher_id] = {}
        return {}


def _get_pitcher_features(pitcher_id, season):
    """
    Look up pitcher aggregate + arsenal features.

    Data sources (in priority order):
    1. pitcher_arsenal.parquet (Statcast-derived, has whiff/chase/zone per pitch)
    2. MLB API pitchArsenal + season stats (fallback for pitchers not in parquet)
    3. League-average defaults

    Stuff+ is enriched from statcast_pitches_live when parquet values are NULL.
    """
    _DEFAULT_RESULT = dict(_LEAGUE_AVG_PITCHER)
    _DEFAULT_RESULT.update({
        "p_usage_fastball": 0.55, "p_usage_breaking": 0.30, "p_usage_offspeed": 0.15,
        "p_pitch1_usage": 0.40, "p_pitch1_velo": 93.5, "p_pitch1_whiff": 0.20, "p_pitch1_stuff": 100.0,
        "p_pitch2_usage": 0.25, "p_pitch2_velo": 85.0, "p_pitch2_whiff": 0.30, "p_pitch2_stuff": 100.0,
        "p_pitch3_usage": 0.15, "p_pitch3_velo": 84.0, "p_pitch3_whiff": 0.25, "p_pitch3_stuff": 100.0,
    })
    _DEFAULT_USAGE = {"fastball": 0.55, "breaking": 0.30, "offspeed": 0.15}

    # --- Try parquet first ---
    df_season = None
    source = "parquet"

    if _pitcher_arsenal is not None and not _pitcher_arsenal.empty:
        pa = _pitcher_arsenal
        mask = (pa["pitcher"] == pitcher_id) & (pa["stand"] == "ALL")
        df_season = pa[mask & (pa["season"] == season)]
        if df_season.empty:
            df_season = pa[mask].sort_values("season", ascending=False)
            if not df_season.empty:
                latest = df_season["season"].iloc[0]
                df_season = df_season[df_season["season"] == latest]

    # --- Fall back to MLB API if parquet has no data ---
    if df_season is None or df_season.empty:
        source = "api"
        # Try current season first, then walk back up to 3 years
        # (handles pitchers returning from multi-year injury, e.g. TJ surgery)
        for try_season in [season, season - 1, season - 2, season - 3]:
            df_season = _fetch_pitcher_from_api(pitcher_id, try_season)
            if df_season is not None and not df_season.empty:
                break

    # --- Final fallback: league average ---
    if df_season is None or df_season.empty:
        log.info("No data for pitcher %s — using league average", pitcher_id)
        return dict(_DEFAULT_RESULT), dict(_DEFAULT_USAGE), []

    n = df_season["n"].values.astype(float)
    total = n.sum()
    if total == 0:
        return dict(_DEFAULT_RESULT), dict(_DEFAULT_USAGE), []

    def _wavg(col):
        vals = pd.to_numeric(df_season[col], errors="coerce")
        valid = vals.notna()
        if not valid.any():
            return None
        return float(np.average(vals[valid], weights=n[valid]))

    result = {}
    for feat, col, default in [
        ("p_avg_stuff_plus", "avg_stuff_plus", 100.0),
        ("p_avg_control_plus", "avg_control_plus", 100.0),
        ("p_avg_velo", "avg_velo", 93.5),
        ("p_whiff_rate", "whiff_rate", 0.245),
        ("p_chase_rate", "chase_rate", 0.295),
        ("p_zone_rate", "zone_rate", 0.45),
        ("p_xwoba", "xwoba", 0.315),
    ]:
        val = _wavg(col) if col in df_season.columns else None
        result[feat] = val if val is not None else default

    # --- Enrich stuff+ from statcast_pitches_live if still NULL ---
    live_stuff = _get_stuff_plus_from_live(pitcher_id)
    if result["p_avg_stuff_plus"] == 100.0 and live_stuff:
        stuff_vals = []
        stuff_weights = []
        for _, row in df_season.iterrows():
            pt = row.get("pitch_type", "")
            if pt in live_stuff:
                stuff_vals.append(live_stuff[pt])
                stuff_weights.append(float(row["n"]))
        if stuff_vals:
            result["p_avg_stuff_plus"] = float(
                np.average(stuff_vals, weights=stuff_weights)
            )
            log.debug("Enriched stuff+ for %s from live: %.1f",
                      pitcher_id, result["p_avg_stuff_plus"])

    result["p_num_pitches"] = int(df_season["pitch_type"].nunique()) if "pitch_type" in df_season.columns else 5
    result["p_total_thrown"] = int(total)

    # Category usage
    cat_usage = {"fastball": 0.0, "breaking": 0.0, "offspeed": 0.0}
    for _, row in df_season.iterrows():
        pt = row.get("pitch_type", "")
        cat = PITCH_CATEGORY.get(pt)
        if cat:
            cat_usage[cat] += float(row["n"]) / total
    result["p_usage_fastball"] = cat_usage["fastball"]
    result["p_usage_breaking"] = cat_usage["breaking"]
    result["p_usage_offspeed"] = cat_usage["offspeed"]

    # Top 3 pitches by usage
    top3 = df_season.nlargest(3, "n")
    arsenal_list = []
    for rank, (_, row) in enumerate(top3.iterrows(), 1):
        usage = float(row["n"]) / total
        velo = float(row["avg_velo"]) if pd.notna(row.get("avg_velo")) else 93.5
        whiff = float(row["whiff_rate"]) if pd.notna(row.get("whiff_rate")) else 0.245
        pt = row.get("pitch_type", "??")
        stuff = float(row["avg_stuff_plus"]) if pd.notna(row.get("avg_stuff_plus")) else live_stuff.get(pt, 100.0)
        result[f"p_pitch{rank}_usage"] = usage
        result[f"p_pitch{rank}_velo"] = velo
        result[f"p_pitch{rank}_whiff"] = whiff
        result[f"p_pitch{rank}_stuff"] = stuff
        arsenal_list.append({
            "pitch_type": pt,
            "name": PITCH_NAMES.get(pt, pt),
            "usage": round(usage, 3),
            "velo": round(velo, 1),
            "whiff": round(whiff, 3),
            "stuff": round(stuff, 0),
        })

    # Fill missing ranks
    for rank in range(len(top3) + 1, 4):
        result[f"p_pitch{rank}_usage"] = 0.0
        result[f"p_pitch{rank}_velo"] = 0.0
        result[f"p_pitch{rank}_whiff"] = 0.0
        result[f"p_pitch{rank}_stuff"] = 0.0

    return result, cat_usage, arsenal_list


def _get_park_features(venue, season):
    """Look up park factors. Falls back to neutral."""
    if _park_factors is None or _park_factors.empty:
        return {"park_run_factor": 1.0, "park_hr_factor": 1.0}

    pf = _park_factors
    mask = (pf["venue"] == venue) & (pf["stand"] == "ALL")
    row = pf[mask & (pf["season"] == season)]
    if row.empty:
        row = pf[mask].sort_values("season", ascending=False).head(1)
    if row.empty:
        return {"park_run_factor": 1.0, "park_hr_factor": 1.0}

    r = row.iloc[0]
    return {
        "park_run_factor": float(r["run_factor"]) if pd.notna(r.get("run_factor")) else 1.0,
        "park_hr_factor": float(r["hr_factor"]) if pd.notna(r.get("hr_factor")) else 1.0,
    }


def _get_pitch_usage(pitcher_id, season, stand, arsenal_list):
    """
    Get expected pitch usage from the pitcher's arsenal.
    Returns list of {pitch_type, name, usage, velo, whiff, stuff} sorted by usage.
    """
    if not arsenal_list:
        return []

    # If we have the pitch selection model, we could refine usage by count/context.
    # For now, return the baseline arsenal usage (already context-free).
    return arsenal_list


def predict_matchup(
    batter_id,
    pitcher_id,
    stand="R",
    p_throws="R",
    venue=None,
    season=2025,
    inning=1,
    outs=0,
    runner_1b=0,
    runner_2b=0,
    runner_3b=0,
    n_thru_order=1,
):
    """
    Predict PA outcome probabilities for a batter-vs-pitcher matchup.

    Returns dict with:
      - probs: {1B, 2B, 3B, BB, HBP, HR, IBB, K, OUT} probabilities
      - summary: {k_pct, bb_pct, hit_pct, hr_pct, obp, xba, xslg}
      - arsenal: [{pitch_type, name, usage, velo, whiff, stuff}, ...]
      - ok: True
    """
    _load()

    if _model is None:
        return {"ok": False, "reason": "model_not_loaded"}

    # Assemble feature vector
    features = {}

    # Batter overall + platoon features
    features.update(_get_batter_features(batter_id, season, p_throws))

    # Pitcher features (also returns category usage and arsenal list)
    pitcher_feats, cat_usage, arsenal_list = _get_pitcher_features(pitcher_id, season)
    features.update(pitcher_feats)

    # Batter vs pitch-type features (weighted by this pitcher's usage)
    features.update(_get_batter_pitch_type_features(batter_id, season, cat_usage))

    # Park factors
    if venue:
        features.update(_get_park_features(venue, season))
    else:
        features.update({"park_run_factor": 1.0, "park_hr_factor": 1.0})

    # Recent form (rolling 14-day) — regressed toward player's own profile
    # when sample size is small (early season / recent call-up).
    try:
        from services.recent_form import get_batter_recent_form, get_pitcher_recent_form, RELIABLE_PA
        bat_r14 = get_batter_recent_form(batter_id)
        pit_r14 = get_pitcher_recent_form(pitcher_id)

        # Regress batter recent form toward their own season-level profile
        bat_pa = bat_r14.pop("_pa", RELIABLE_PA)
        if bat_pa < RELIABLE_PA:
            w = bat_pa / RELIABLE_PA
            # Map r14 keys → player profile keys already in `features`
            _BAT_R14_TO_PROFILE = {
                "bat_r14_k_pct": "bat_k_pct",
                "bat_r14_bb_pct": "bat_bb_pct",
                "bat_r14_xwoba": "bat_xwoba",
                "bat_r14_barrel_rate": "bat_barrel_rate",
                "bat_r14_whiff_rate": "bat_whiff_rate",
                "bat_r14_chase_rate": "bat_chase_rate",
            }
            for r14_key, profile_key in _BAT_R14_TO_PROFILE.items():
                observed = bat_r14.get(r14_key)
                prior = features.get(profile_key, _LEAGUE_AVG_BATTER_R14.get(r14_key))
                if observed is not None and prior is not None:
                    bat_r14[r14_key] = w * observed + (1 - w) * prior
                elif prior is not None:
                    bat_r14[r14_key] = prior

        # Regress pitcher recent form toward their own season-level profile
        pit_pa = pit_r14.pop("_pa", RELIABLE_PA)
        if pit_pa < RELIABLE_PA:
            w = pit_pa / RELIABLE_PA
            _PIT_R14_TO_PROFILE = {
                "p_r14_xwoba": "p_xwoba",
                "p_r14_whiff_rate": "p_whiff_rate",
                "p_r14_chase_rate": "p_chase_rate",
            }
            for r14_key, profile_key in _PIT_R14_TO_PROFILE.items():
                observed = pit_r14.get(r14_key)
                prior = features.get(profile_key, _LEAGUE_AVG_PITCHER_R14.get(r14_key))
                if observed is not None and prior is not None:
                    pit_r14[r14_key] = w * observed + (1 - w) * prior
                elif prior is not None:
                    pit_r14[r14_key] = prior
            # k_pct / bb_pct don't have direct pitcher profile equivalents,
            # so fall back to league average as the prior for those two.
            for r14_key in ("p_r14_k_pct", "p_r14_bb_pct"):
                observed = pit_r14.get(r14_key)
                prior = _LEAGUE_AVG_PITCHER_R14.get(r14_key)
                if observed is not None and prior is not None:
                    pit_r14[r14_key] = w * observed + (1 - w) * prior

        features.update(bat_r14)
        features.update(pit_r14)
    except Exception as e:
        log.info("Recent form unavailable: %s — using league avg", e)
        features.update(_LEAGUE_AVG_BATTER_R14)
        features.update(_LEAGUE_AVG_PITCHER_R14)

    # Context
    features["inning"] = inning
    features["outs_when_up"] = outs
    features["n_thruorder_pitcher"] = n_thru_order
    features["runner_on_1b"] = runner_1b
    features["runner_on_2b"] = runner_2b
    features["runner_on_3b"] = runner_3b

    # Categorical encoding (same as training: L=0, R=1)
    features["stand"] = 0 if stand == "L" else 1
    features["p_throws"] = 0 if p_throws == "L" else 1

    # Build feature array in model's expected order
    feature_names = _meta["numeric_features"] + _meta["categorical_features"]
    X = np.array([[features.get(f, np.nan) for f in feature_names]], dtype=np.float64)

    # Guard: replace any remaining NaN/None with league-average defaults.
    # NaN features cause XGBoost to follow default split directions which can
    # produce unrealistically high hit probabilities (observed: +46% hit rate
    # when all pitcher features are NaN).
    _FALLBACK = {
        **_LEAGUE_AVG_BATTER, **_LEAGUE_AVG_BATTER_PLAT,
        **{f"bvpt_{s}_{c}": _LEAGUE_AVG_BVPT[f"bvpt_{s}"]
           for c in ("fastball", "breaking", "offspeed")
           for s in ("whiff_rate", "chase_rate", "zone_contact_rate", "hard_hit_rate", "xwoba")},
        **{f"bvpt_w_{s}": _LEAGUE_AVG_BVPT[f"bvpt_{s}"]
           for s in ("whiff_rate", "chase_rate", "zone_contact_rate", "hard_hit_rate", "xwoba")},
        **_LEAGUE_AVG_PITCHER,
        "p_usage_fastball": 0.55, "p_usage_breaking": 0.30, "p_usage_offspeed": 0.15,
        "p_pitch1_usage": 0.40, "p_pitch1_velo": 93.5, "p_pitch1_whiff": 0.20, "p_pitch1_stuff": 100.0,
        "p_pitch2_usage": 0.25, "p_pitch2_velo": 85.0, "p_pitch2_whiff": 0.30, "p_pitch2_stuff": 100.0,
        "p_pitch3_usage": 0.15, "p_pitch3_velo": 84.0, "p_pitch3_whiff": 0.25, "p_pitch3_stuff": 100.0,
        "park_run_factor": 1.0, "park_hr_factor": 1.0,
        "inning": 1, "outs_when_up": 0, "n_thruorder_pitcher": 1,
        "runner_on_1b": 0, "runner_on_2b": 0, "runner_on_3b": 0,
        **_LEAGUE_AVG_BATTER_R14, **_LEAGUE_AVG_PITCHER_R14,
        "stand": 1, "p_throws": 1,
    }
    nan_mask = np.isnan(X[0])
    if nan_mask.any():
        for i, is_nan in enumerate(nan_mask):
            if is_nan:
                fname = feature_names[i]
                X[0, i] = _FALLBACK.get(fname, 0.0)
        log.debug("Replaced %d NaN features with defaults", int(nan_mask.sum()))

    # Predict
    probs = _model.predict_proba(X)[0]

    # Map to class names
    prob_dict = {}
    for i, cls in enumerate(CLASSES):
        prob_dict[cls] = round(float(probs[i]), 4)

    # Derived summary stats
    hit_pct = prob_dict["1B"] + prob_dict["2B"] + prob_dict["3B"] + prob_dict["HR"]
    on_base_pct = hit_pct + prob_dict["BB"] + prob_dict["HBP"] + prob_dict["IBB"]
    ab_pct = 1.0 - prob_dict["BB"] - prob_dict["HBP"] - prob_dict["IBB"]
    xba_adj = hit_pct / ab_pct if ab_pct > 0 else 0

    tb = (prob_dict["1B"] * 1 + prob_dict["2B"] * 2 +
          prob_dict["3B"] * 3 + prob_dict["HR"] * 4)
    xslg = tb / ab_pct if ab_pct > 0 else 0

    summary = {
        "k_pct": round(prob_dict["K"], 3),
        "bb_pct": round(prob_dict["BB"] + prob_dict["IBB"], 3),
        "hit_pct": round(hit_pct, 3),
        "hr_pct": round(prob_dict["HR"], 3),
        "obp": round(on_base_pct, 3),
        "xba": round(xba_adj, 3),
        "xslg": round(xslg, 3),
    }

    # Get arsenal/pitch usage for this matchup
    arsenal = _get_pitch_usage(pitcher_id, season, stand, arsenal_list)

    return {
        "ok": True,
        "probs": prob_dict,
        "summary": summary,
        "arsenal": arsenal,
    }


def predict_matchup_live(
    batter_id,
    pitcher_id,
    stand="R",
    p_throws="R",
    venue=None,
    season=2025,
    inning=1,
    outs=0,
    runner_1b=0,
    runner_2b=0,
    runner_3b=0,
    n_thru_order=1,
    pitcher_velo_tonight=None,
    pitcher_pitch_count=None,
):
    """
    Predict PA outcome with Bayesian in-game adjustments.

    Gets base prediction from XGBoost, then applies multiplicative adjustments
    based on observed pitcher performance tonight (velo delta, fatigue).

    Returns same structure as predict_matchup plus:
      - adjustments: dict showing what shifted and by how much
      - pregame_probs: baseline prediction (inning 1, no runners, 1st TTO)
    """
    # Get live prediction (already uses real context: inning, runners, TTO)
    result = predict_matchup(
        batter_id, pitcher_id, stand, p_throws, venue, season,
        inning, outs, runner_1b, runner_2b, runner_3b, n_thru_order,
    )

    if not result.get("ok"):
        return result

    # Get pregame baseline for comparison
    pregame = predict_matchup(
        batter_id, pitcher_id, stand, p_throws, venue, season,
        inning=1, outs=0, runner_1b=0, runner_2b=0, runner_3b=0, n_thru_order=1,
    )
    result["pregame_probs"] = pregame.get("probs", {})

    # Apply Bayesian adjustments
    adjustments = {}
    adj_factors = {cls: 1.0 for cls in CLASSES}

    # Velocity adjustment
    _load()
    if pitcher_velo_tonight is not None and _pitcher_arsenal is not None and not _pitcher_arsenal.empty:
        pa = _pitcher_arsenal
        mask = (pa["pitcher"] == pitcher_id) & (pa["stand"] == "ALL")
        season_data = pa[mask & (pa["season"] == season)]
        if season_data.empty:
            season_data = pa[mask].sort_values("season", ascending=False)

        if not season_data.empty:
            n = season_data["n"].values.astype(float)
            total = n.sum()
            if total > 0:
                velos = pd.to_numeric(season_data["avg_velo"], errors="coerce")
                valid = velos.notna()
                if valid.any():
                    expected_velo = float(np.average(velos[valid], weights=n[valid]))
                    velo_delta = pitcher_velo_tonight - expected_velo

                    if abs(velo_delta) > 0.3:  # Only adjust if meaningful difference
                        # Each 1 mph above expected -> K prob +2.5%, HR prob -1.5%
                        adj_factors["K"] *= (1.0 + velo_delta * 0.025)
                        adj_factors["HR"] *= (1.0 - velo_delta * 0.015)
                        adj_factors["BB"] *= (1.0 - velo_delta * 0.01)
                        adjustments["velo"] = {
                            "tonight": round(pitcher_velo_tonight, 1),
                            "expected": round(expected_velo, 1),
                            "delta": round(velo_delta, 1),
                        }

    # Pitch count / fatigue adjustment
    if pitcher_pitch_count is not None and pitcher_pitch_count > 75:
        fatigue_factor = min((pitcher_pitch_count - 75) / 25.0, 1.0)
        adj_factors["K"] *= (1.0 - fatigue_factor * 0.08)
        adj_factors["BB"] *= (1.0 + fatigue_factor * 0.06)
        adj_factors["HR"] *= (1.0 + fatigue_factor * 0.04)
        adj_factors["1B"] *= (1.0 + fatigue_factor * 0.02)
        adjustments["fatigue"] = {
            "pitch_count": pitcher_pitch_count,
            "factor": round(fatigue_factor, 2),
        }

    # Apply adjustments and renormalize
    if adjustments:
        probs = result["probs"]
        adjusted = {cls: probs[cls] * adj_factors.get(cls, 1.0) for cls in CLASSES}
        total = sum(adjusted.values())
        if total > 0:
            adjusted = {cls: round(v / total, 4) for cls, v in adjusted.items()}
        result["probs"] = adjusted

        # Recompute summary with adjusted probs
        hit_pct = adjusted["1B"] + adjusted["2B"] + adjusted["3B"] + adjusted["HR"]
        on_base_pct = hit_pct + adjusted["BB"] + adjusted["HBP"] + adjusted["IBB"]
        ab_pct = 1.0 - adjusted["BB"] - adjusted["HBP"] - adjusted["IBB"]
        xba = hit_pct / ab_pct if ab_pct > 0 else 0
        tb = adjusted["1B"] + adjusted["2B"] * 2 + adjusted["3B"] * 3 + adjusted["HR"] * 4
        xslg = tb / ab_pct if ab_pct > 0 else 0

        result["summary"] = {
            "k_pct": round(adjusted["K"], 3),
            "bb_pct": round(adjusted["BB"] + adjusted["IBB"], 3),
            "hit_pct": round(hit_pct, 3),
            "hr_pct": round(adjusted["HR"], 3),
            "obp": round(on_base_pct, 3),
            "xba": round(xba, 3),
            "xslg": round(xslg, 3),
        }

    result["adjustments"] = adjustments
    return result
