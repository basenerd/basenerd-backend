"""
Umpire strike zone service.

Loads per-umpire strike zone models and provides:
- P(called strike) predictions for a given umpire + pitch location
- Zone heatmap grids (per-umpire vs league average)
- Umpire profile data (tendencies from umpire_metrics.parquet)
- Umpire list with key metrics
- Per-game umpire report (all called pitches, correct/incorrect, ABS challenges)

Each umpire has their own individually trained model.
Umpires below the training threshold fall back to the league-average model.
"""

import os
import json
import logging
import math

import joblib
import numpy as np
import pandas as pd
import requests

log = logging.getLogger(__name__)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MODEL_DIR = os.path.join(_ROOT, "models", "umpire_zone_models")
_REGISTRY_PATH = os.path.join(_MODEL_DIR, "registry.json")
_DATA_DIR = os.path.join(_ROOT, "data")

# Lazy-loaded globals
_loaded = False
_registry = None
_league_model = None
_umpire_models = {}  # cache: umpire_id -> model
_umpire_metrics = None
_game_outcomes = None

# Heatmap grid bounds
GRID_X_MIN, GRID_X_MAX = -1.5, 1.5
GRID_Z_MIN, GRID_Z_MAX = 1.0, 4.2
GRID_SIZE = 50

# Average strike zone for heatmap generation
AVG_SZ_TOP = 3.4
AVG_SZ_BOT = 1.6
ZONE_X_HALF = 0.83


def _safe_float(val):
    """Convert to float, return None if NaN/None/invalid."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if math.isfinite(f) else None
    except (ValueError, TypeError):
        return None


def _load():
    """Lazy-load registry, league-average model, and data files."""
    global _loaded, _registry, _league_model, _umpire_metrics, _game_outcomes

    if _loaded:
        return
    _loaded = True

    try:
        with open(_REGISTRY_PATH) as f:
            _registry = json.load(f)
        log.info("Umpire zone registry loaded: %d umpires",
                 len(_registry.get("umpires", {})))
    except Exception as e:
        log.warning("Could not load umpire zone registry: %s", e)
        _registry = {"umpires": {}, "features": [], "cat_features": [],
                      "cat_categories": {}}

    try:
        _league_model = joblib.load(os.path.join(_MODEL_DIR, "_league_avg.joblib"))
        log.info("League-average zone model loaded")
    except Exception as e:
        log.warning("Could not load league-average zone model: %s", e)

    # Load umpire metrics (for profile data)
    metrics_path = os.path.join(_DATA_DIR, "umpire_metrics.parquet")
    try:
        _umpire_metrics = pd.read_parquet(metrics_path)
        log.info("Umpire metrics: %d rows", len(_umpire_metrics))
    except Exception as e:
        log.warning("Could not load umpire metrics: %s", e)
        _umpire_metrics = pd.DataFrame()

    # Load game outcomes (for umpire-game mapping and name lookup)
    outcomes_path = os.path.join(_DATA_DIR, "game_outcomes.parquet")
    try:
        _game_outcomes = pd.read_parquet(outcomes_path)
        log.info("Game outcomes: %d rows", len(_game_outcomes))
    except Exception as e:
        log.warning("Could not load game outcomes: %s", e)
        _game_outcomes = pd.DataFrame()


def _get_model(umpire_id):
    """Get the model for a specific umpire, or league-avg fallback."""
    _load()

    ump_key = str(umpire_id)

    # Check cache
    if umpire_id in _umpire_models:
        return _umpire_models[umpire_id]

    # Try to load per-umpire model
    if ump_key in _registry.get("umpires", {}):
        model_path = os.path.join(_MODEL_DIR, f"{umpire_id}.joblib")
        try:
            model = joblib.load(model_path)
            _umpire_models[umpire_id] = model
            return model
        except Exception as e:
            log.warning("Could not load model for umpire %s: %s", umpire_id, e)

    # Fallback to league average
    return _league_model


def _build_features(plate_x, plate_z, sz_top, sz_bot, pitch_type, stand,
                    balls, strikes):
    """Build a single-row feature DataFrame for prediction."""
    _load()

    sz_range = sz_top - sz_bot
    plate_z_norm = (plate_z - sz_bot) / sz_range if sz_range > 0 else 0.5

    row = {
        "plate_x": plate_x,
        "plate_z_norm": plate_z_norm,
        "dist_from_edge_x": abs(plate_x) - ZONE_X_HALF,
        "dist_from_edge_z_top": plate_z - sz_top,
        "dist_from_edge_z_bot": sz_bot - plate_z,
        "pitch_type": pitch_type,
        "stand": stand,
        "balls": balls,
        "strikes": strikes,
    }

    df = pd.DataFrame([row])

    # Apply categorical encoding consistent with training
    cat_categories = _registry.get("cat_categories", {})
    for col in _registry.get("cat_features", []):
        if col in cat_categories:
            df[col] = pd.Categorical(df[col], categories=cat_categories[col])

    features = _registry.get("features", list(row.keys()))
    return df[features]


def predict_called_strike(hp_umpire_id, plate_x, plate_z, sz_top, sz_bot,
                          pitch_type, stand, balls, strikes):
    """Predict P(called strike) for a single pitch using the umpire's model.

    Returns dict with "ok", "p_called_strike", "model_type".
    """
    _load()

    model = _get_model(hp_umpire_id)
    if model is None:
        return {"ok": False, "error": "No model available"}

    try:
        X = _build_features(plate_x, plate_z, sz_top, sz_bot,
                            pitch_type, stand, balls, strikes)
        prob = float(model.predict_proba(X)[:, 1][0])

        is_individual = str(hp_umpire_id) in _registry.get("umpires", {})
        return {
            "ok": True,
            "p_called_strike": round(prob, 4),
            "model_type": "individual" if is_individual else "league_avg",
        }
    except Exception as e:
        log.error("Prediction error for umpire %s: %s", hp_umpire_id, e)
        return {"ok": False, "error": str(e)}


def umpire_zone_heatmap(hp_umpire_id, stand="R", pitch_type=None,
                        balls=None, strikes=None):
    """Generate a heatmap grid of P(called strike) for an umpire.

    Returns the umpire's grid and the league-average grid for comparison.
    Grid spans plate_x × plate_z with GRID_SIZE resolution.
    """
    _load()

    ump_model = _get_model(hp_umpire_id)
    if ump_model is None:
        return {"ok": False, "error": "No model available"}

    # Build meshgrid
    xs = np.linspace(GRID_X_MIN, GRID_X_MAX, GRID_SIZE)
    zs = np.linspace(GRID_Z_MIN, GRID_Z_MAX, GRID_SIZE)
    xx, zz = np.meshgrid(xs, zs)
    n_points = xx.size

    # Default values
    pt = pitch_type or "FF"
    b = balls if balls is not None else 0
    s = strikes if strikes is not None else 0

    # Build feature grid
    sz_range = AVG_SZ_TOP - AVG_SZ_BOT
    grid_data = {
        "plate_x": xx.ravel(),
        "plate_z_norm": (zz.ravel() - AVG_SZ_BOT) / sz_range,
        "dist_from_edge_x": np.abs(xx.ravel()) - ZONE_X_HALF,
        "dist_from_edge_z_top": zz.ravel() - AVG_SZ_TOP,
        "dist_from_edge_z_bot": AVG_SZ_BOT - zz.ravel(),
        "pitch_type": [pt] * n_points,
        "stand": [stand] * n_points,
        "balls": [b] * n_points,
        "strikes": [s] * n_points,
    }
    grid_df = pd.DataFrame(grid_data)

    # Apply categorical encoding
    cat_categories = _registry.get("cat_categories", {})
    for col in _registry.get("cat_features", []):
        if col in cat_categories:
            grid_df[col] = pd.Categorical(
                grid_df[col], categories=cat_categories[col]
            )

    features = _registry.get("features", list(grid_data.keys()))
    X_grid = grid_df[features]

    try:
        # Umpire-specific predictions
        ump_probs = ump_model.predict_proba(X_grid)[:, 1].reshape(GRID_SIZE, GRID_SIZE)

        # League-average predictions
        league_probs = None
        if _league_model is not None:
            league_probs = _league_model.predict_proba(X_grid)[:, 1].reshape(
                GRID_SIZE, GRID_SIZE
            )

        result = {
            "ok": True,
            "grid_x": xs.tolist(),
            "grid_z": zs.tolist(),
            "p_strike": [[round(float(v), 4) for v in row] for row in ump_probs],
            "stand": stand,
            "pitch_type": pt,
            "balls": b,
            "strikes": s,
            "sz_top": AVG_SZ_TOP,
            "sz_bot": AVG_SZ_BOT,
            "model_type": ("individual"
                           if str(hp_umpire_id) in _registry.get("umpires", {})
                           else "league_avg"),
        }

        if league_probs is not None:
            result["p_strike_league_avg"] = [
                [round(float(v), 4) for v in row] for row in league_probs
            ]
            diff = ump_probs - league_probs
            result["p_strike_diff"] = [
                [round(float(v), 4) for v in row] for row in diff
            ]

        return result

    except Exception as e:
        log.error("Heatmap error for umpire %s: %s", hp_umpire_id, e)
        return {"ok": False, "error": str(e)}


def umpire_profile(hp_umpire_id, season=None):
    """Return umpire profile data including tendencies and model info.

    Returns dict with "ok", tendencies, career stats, and model eval metrics.
    """
    _load()

    if _umpire_metrics is None or _umpire_metrics.empty:
        return {"ok": False, "error": "Umpire metrics not available"}

    um = _umpire_metrics
    ump_data = um[um["hp_umpire_id"] == hp_umpire_id]

    if ump_data.empty:
        return {"ok": False, "error": f"No data for umpire {hp_umpire_id}"}

    # Get name from game_outcomes if available
    name = None
    if _game_outcomes is not None and not _game_outcomes.empty:
        if "hp_umpire_name" in _game_outcomes.columns:
            name_rows = _game_outcomes[
                _game_outcomes["hp_umpire_id"] == hp_umpire_id
            ]["hp_umpire_name"].dropna()
            if not name_rows.empty:
                name = str(name_rows.iloc[0])

    # Season-specific or most recent
    if season:
        row = ump_data[ump_data["season"] == season]
    else:
        row = ump_data.sort_values("season", ascending=False)

    if row.empty:
        row = ump_data.sort_values("season", ascending=False)

    latest = row.iloc[0]

    # Model info
    ump_key = str(hp_umpire_id)
    has_model = ump_key in _registry.get("umpires", {})
    model_info = _registry["umpires"].get(ump_key, {}) if has_model else {}

    result = {
        "ok": True,
        "umpire_id": int(hp_umpire_id),
        "name": name,
        "seasons": sorted(int(s) for s in ump_data["season"].unique()),
        "season": int(latest["season"]),
        "tendencies": {
            "overall_cs_rate": _safe_float(latest.get("overall_cs_rate")),
            "ooz_cs_rate": _safe_float(latest.get("ooz_cs_rate")),
            "iz_ball_rate": _safe_float(latest.get("iz_ball_rate")),
            "zone_size_factor": _safe_float(latest.get("zone_size_factor")),
            "shadow_high_cs_rate": _safe_float(latest.get("shadow_high_cs_rate")),
            "shadow_low_cs_rate": _safe_float(latest.get("shadow_low_cs_rate")),
            "games": int(latest.get("games", 0)),
            "total_called": int(latest.get("total_called", 0)),
            "run_env_factor": _safe_float(latest.get("run_env_factor")),
        },
        "has_individual_model": has_model,
        "model_eval": {
            "brier_score": model_info.get("brier_score"),
            "auc": model_info.get("auc"),
            "total_pitches": model_info.get("total_pitches"),
        } if has_model else None,
    }

    return result


def umpire_list(season=None):
    """Return list of all umpires with key tendency metrics.

    Returns dict with "ok" and "umpires" array.
    """
    _load()

    if _umpire_metrics is None or _umpire_metrics.empty:
        return {"ok": False, "error": "Umpire metrics not available"}

    um = _umpire_metrics.copy()

    if season:
        um = um[um["season"] == season]

    if um.empty:
        return {"ok": True, "umpires": [], "season": season}

    # Get name lookup
    name_map = {}
    if (_game_outcomes is not None and not _game_outcomes.empty
            and "hp_umpire_name" in _game_outcomes.columns):
        names = _game_outcomes[["hp_umpire_id", "hp_umpire_name"]].dropna()
        names = names.drop_duplicates("hp_umpire_id")
        name_map = dict(zip(names["hp_umpire_id"], names["hp_umpire_name"]))

    # Compute league averages for this season subset for percentile context
    lg_ooz_cs = um["ooz_cs_rate"].dropna()
    lg_iz_ball = um["iz_ball_rate"].dropna()

    umpires = []
    for _, row in um.iterrows():
        ump_id = int(row["hp_umpire_id"])
        has_model = str(ump_id) in _registry.get("umpires", {})

        ooz = _safe_float(row.get("ooz_cs_rate"))
        iz = _safe_float(row.get("iz_ball_rate"))

        # Favor index: positive = pitcher-friendly (expands zone),
        # negative = hitter-friendly (contracts zone)
        favor_index = None
        favor_label = None
        if ooz is not None and iz is not None:
            # ooz_cs_rate = incorrectly expanding zone (helps pitchers)
            # iz_ball_rate = incorrectly contracting zone (helps hitters)
            # Normalize to roughly -1..+1 scale
            favor_index = round(ooz - iz, 4)
            if favor_index > 0.01:
                favor_label = "Pitcher-friendly"
            elif favor_index < -0.01:
                favor_label = "Hitter-friendly"
            else:
                favor_label = "Neutral"

        # Accuracy: use precomputed if available, otherwise estimate
        accuracy = _safe_float(row.get("accuracy"))
        if accuracy is None and ooz is not None and iz is not None:
            cs_rate = _safe_float(row.get("overall_cs_rate"))
            if cs_rate is not None:
                ooz_frac = 1 - cs_rate
                iz_frac = cs_rate
                error_rate = ooz * ooz_frac + iz * iz_frac
                accuracy = round(1.0 - error_rate, 4)

        abs_challenges = int(row.get("abs_challenges", 0))
        abs_overturned = int(row.get("abs_overturned", 0))
        abs_overturn_pct = (abs_overturned / abs_challenges
                           if abs_challenges > 0 else None)

        umpires.append({
            "umpire_id": ump_id,
            "name": name_map.get(ump_id),
            "season": int(row["season"]),
            "games": int(row.get("games", 0)),
            "total_called": int(row.get("total_called", 0)),
            "overall_cs_rate": _safe_float(row.get("overall_cs_rate")),
            "ooz_cs_rate": ooz,
            "iz_ball_rate": iz,
            "zone_size_factor": _safe_float(row.get("zone_size_factor")),
            "shadow_high_cs_rate": _safe_float(row.get("shadow_high_cs_rate")),
            "shadow_low_cs_rate": _safe_float(row.get("shadow_low_cs_rate")),
            "run_env_factor": _safe_float(row.get("run_env_factor")),
            "accuracy": accuracy,
            "favor_index": favor_index,
            "favor_label": favor_label,
            "abs_challenges": abs_challenges,
            "abs_overturned": abs_overturned,
            "abs_overturn_pct": _safe_float(abs_overturn_pct),
            "has_individual_model": has_model,
        })

    # Sort by games worked descending
    umpires.sort(key=lambda u: u.get("games", 0), reverse=True)

    # Available seasons
    all_seasons = sorted(_umpire_metrics["season"].unique().tolist())

    return {
        "ok": True,
        "umpires": umpires,
        "season": season,
        "seasons": all_seasons,
        "total": len(umpires),
    }


def umpire_bio(hp_umpire_id):
    """Fetch umpire bio from MLB API."""
    try:
        resp = requests.get(
            f"https://statsapi.mlb.com/api/v1/people/{hp_umpire_id}",
            timeout=10,
        )
        if resp.status_code != 200:
            return {"ok": False, "error": "MLB API error"}
        people = resp.json().get("people", [])
        if not people:
            return {"ok": False, "error": "Not found"}
        p = people[0]
        return {
            "ok": True,
            "id": p.get("id"),
            "fullName": p.get("fullName"),
            "birthDate": p.get("birthDate"),
            "currentAge": p.get("currentAge"),
            "birthCity": p.get("birthCity"),
            "birthStateProvince": p.get("birthStateProvince"),
            "birthCountry": p.get("birthCountry"),
            "height": p.get("height"),
            "weight": p.get("weight"),
            "active": p.get("active"),
        }
    except Exception as e:
        log.warning("Could not fetch umpire bio %s: %s", hp_umpire_id, e)
        return {"ok": False, "error": str(e)}


def umpire_gamelog(hp_umpire_id, season=None):
    """Return game log for an umpire from game_outcomes."""
    _load()

    if _game_outcomes is None or _game_outcomes.empty:
        return {"ok": False, "error": "Game outcomes not available"}

    go = _game_outcomes
    go_ump = go[go["hp_umpire_id"] == hp_umpire_id]

    if season:
        go_ump = go_ump[go_ump["season"] == season]

    if go_ump.empty:
        return {"ok": True, "games": [], "total": 0}

    games = []
    for _, row in go_ump.sort_values("game_date", ascending=False).iterrows():
        games.append({
            "game_pk": int(row["game_pk"]),
            "game_date": str(row.get("game_date", "")),
            "season": int(row.get("season", 0)),
            "home_team": str(row.get("home_team", "")),
            "away_team": str(row.get("away_team", "")),
            "home_score": int(row.get("home_score", 0)),
            "away_score": int(row.get("away_score", 0)),
            "total_runs": int(row.get("total_runs", 0)),
        })

    return {"ok": True, "games": games, "total": len(games)}


# ── Per-game umpire report ──────────────────────────────────────────

# Zone boundary: half plate width in feet (17 inches / 2 / 12)
_PLATE_HALF = 17.0 / 24.0  # 0.7083 ft


def umpire_game_report(feed):
    """Build an umpire report for a completed game from the MLB live feed.

    Extracts every called pitch (called strike or ball), classifies each
    as correct or incorrect based on the true zone, and pulls ABS challenges.

    Returns dict with:
      - umpire info (id, name)
      - all called pitches with correct/incorrect flags
      - summary stats (accuracy, missed calls, etc.)
      - ABS challenges
    """
    if not feed:
        return {"ok": False, "error": "No feed data"}

    game_data = feed.get("gameData") or {}
    live_data = feed.get("liveData") or {}
    status = (game_data.get("status") or {}).get("abstractGameState", "")
    if status != "Final":
        return {"ok": False, "error": "Game not final"}

    # --- Umpire info ---
    boxscore = live_data.get("boxscore") or {}
    officials = boxscore.get("officials") or []
    hp_umpire_id = None
    hp_umpire_name = None
    for official in officials:
        if official.get("officialType") == "Home Plate":
            ump = official.get("official") or {}
            hp_umpire_id = ump.get("id")
            hp_umpire_name = ump.get("fullName")
            break

    if not hp_umpire_id:
        return {"ok": False, "error": "No home plate umpire found"}

    # --- Walk all plays and pitches ---
    all_plays = (live_data.get("plays") or {}).get("allPlays") or []
    called_pitches = []
    challenges = []
    pitch_num = 0

    for play in all_plays:
        about = play.get("about") or {}
        inning = about.get("inning", 0)
        half = (about.get("halfInning") or "").lower()
        half_label = "Top" if half.startswith("top") else "Bot"
        matchup = play.get("matchup") or {}
        batter_name = (matchup.get("batter") or {}).get("fullName", "")
        batter_id = (matchup.get("batter") or {}).get("id")
        pitcher_name = (matchup.get("pitcher") or {}).get("fullName", "")
        pitcher_id = (matchup.get("pitcher") or {}).get("id")
        bat_side = ((matchup.get("batSide") or {}).get("code") or "R").upper()

        play_events = play.get("playEvents") or []
        for ev in play_events:
            if not ev.get("isPitch"):
                continue

            details = ev.get("details") or {}
            call_obj = details.get("call") or {}
            call_code = (call_obj.get("code") or "").upper()
            call_desc = call_obj.get("description") or ""
            desc = details.get("description") or ""

            pitch_data = ev.get("pitchData") or {}
            coords = pitch_data.get("coordinates") or {}
            px = coords.get("pX")
            pz = coords.get("pZ")
            sz_top = pitch_data.get("strikeZoneTop")
            sz_bot = pitch_data.get("strikeZoneBottom")

            pitch_type_obj = details.get("type") or {}
            pitch_type_code = pitch_type_obj.get("code") or ""
            pitch_type_desc = pitch_type_obj.get("description") or ""

            count = ev.get("count") or {}
            balls = count.get("balls", 0)
            strikes = count.get("strikes", 0)

            # Check for ABS challenge on this pitch
            review = ev.get("reviewDetails")
            if isinstance(review, dict) and review.get("reviewType"):
                review_type = review.get("reviewType") or ""
                overturned = bool(review.get("isOverturned"))
                challenge_team = review.get("challengeTeamId")
                is_abs = review_type == "MJ"
                result_text = "Overturned" if overturned else "Confirmed"
                challenges.append({
                    "type": "abs_challenge" if is_abs else "review",
                    "label": "ABS Challenge" if is_abs else f"Review ({review_type})",
                    "call": call_desc,
                    "result": result_text,
                    "overturned": overturned,
                    "challengeTeamId": challenge_team,
                    "inning": inning,
                    "half": half_label,
                    "batter": batter_name,
                    "pitcher": pitcher_name,
                    "px": _safe_float(px),
                    "pz": _safe_float(pz),
                    "sz_top": _safe_float(sz_top),
                    "sz_bot": _safe_float(sz_bot),
                    "pitch_num": pitch_num + 1,
                })

            # Only process called strikes and balls
            is_called_strike = call_code in ("C",)
            is_ball = call_code in ("B", "*B")
            if not is_called_strike and not is_ball:
                pitch_num += 1
                continue

            pitch_num += 1

            px_f = _safe_float(px)
            pz_f = _safe_float(pz)
            sz_top_f = _safe_float(sz_top)
            sz_bot_f = _safe_float(sz_bot)

            if px_f is None or pz_f is None or sz_top_f is None or sz_bot_f is None:
                continue

            # True zone check
            in_zone = (abs(px_f) <= _PLATE_HALF
                       and pz_f >= sz_bot_f
                       and pz_f <= sz_top_f)

            if is_called_strike:
                correct = in_zone
            else:
                correct = not in_zone

            called_pitches.append({
                "px": round(px_f, 4),
                "pz": round(pz_f, 4),
                "sz_top": round(sz_top_f, 4),
                "sz_bot": round(sz_bot_f, 4),
                "call": "called_strike" if is_called_strike else "ball",
                "correct": correct,
                "in_zone": in_zone,
                "inning": inning,
                "half": half_label,
                "batter": batter_name,
                "batter_id": batter_id,
                "pitcher": pitcher_name,
                "pitcher_id": pitcher_id,
                "stand": bat_side,
                "pitch_type": pitch_type_code,
                "pitch_type_desc": pitch_type_desc,
                "balls": balls,
                "strikes": strikes,
                "n": pitch_num,
            })

    # --- Summary stats ---
    total = len(called_pitches)
    correct_count = sum(1 for p in called_pitches if p["correct"])
    incorrect = [p for p in called_pitches if not p["correct"]]
    incorrect_strikes = [p for p in incorrect if p["call"] == "called_strike"]
    incorrect_balls = [p for p in incorrect if p["call"] == "ball"]

    called_strikes_total = sum(1 for p in called_pitches if p["call"] == "called_strike")
    called_balls_total = total - called_strikes_total

    summary = {
        "total_called": total,
        "correct": correct_count,
        "incorrect": len(incorrect),
        "accuracy": round(correct_count / total, 4) if total > 0 else None,
        "called_strikes": called_strikes_total,
        "called_balls": called_balls_total,
        "incorrect_strikes": len(incorrect_strikes),
        "incorrect_balls": len(incorrect_balls),
        "challenges_total": len(challenges),
        "challenges_overturned": sum(1 for c in challenges if c["overturned"]),
    }

    # Favor breakdown (incorrect calls by team impact)
    # An incorrect called strike hurts the batting team; incorrect ball hurts pitching team
    favor_home = 0
    favor_away = 0
    for p in incorrect:
        is_top = p["half"] == "Top"  # top inning = away batting
        if p["call"] == "called_strike":
            # Incorrect strike hurts batter
            if is_top:
                favor_home += 1  # helps home (pitching)
            else:
                favor_away += 1  # helps away (pitching)
        else:
            # Incorrect ball hurts pitcher
            if is_top:
                favor_away += 1  # helps away (batting)
            else:
                favor_home += 1  # helps home (batting)

    summary["favor_home"] = favor_home
    summary["favor_away"] = favor_away

    # Hitter/Pitcher favor index
    # Incorrect called strikes expand the zone → favor pitchers (+)
    # Incorrect balls contract the zone → favor hitters (-)
    # Range: -1.0 (all favor hitters) to +1.0 (all favor pitchers), 0 = neutral
    n_incorrect = len(incorrect)
    if n_incorrect > 0:
        favor_index = (len(incorrect_strikes) - len(incorrect_balls)) / n_incorrect
        summary["favor_index"] = round(favor_index, 3)
        if favor_index > 0.15:
            summary["favor_label"] = "Pitcher-friendly"
        elif favor_index < -0.15:
            summary["favor_label"] = "Hitter-friendly"
        else:
            summary["favor_label"] = "Neutral"
    else:
        summary["favor_index"] = 0.0
        summary["favor_label"] = "Perfect"

    # Mark which pitches had ABS challenges (by pitch number)
    challenge_pitch_nums = set()
    for c in challenges:
        if c.get("pitch_num"):
            challenge_pitch_nums.add(c["pitch_num"])
    for p in called_pitches:
        p["challenged"] = p["n"] in challenge_pitch_nums

    # Team names
    teams = game_data.get("teams") or {}
    home_team = (teams.get("home") or {}).get("abbreviation") or \
                (teams.get("home") or {}).get("teamName", "Home")
    away_team = (teams.get("away") or {}).get("abbreviation") or \
                (teams.get("away") or {}).get("teamName", "Away")

    return {
        "ok": True,
        "umpire_id": hp_umpire_id,
        "umpire_name": hp_umpire_name,
        "home_team": home_team,
        "away_team": away_team,
        "pitches": called_pitches,
        "summary": summary,
        "challenges": challenges,
    }
