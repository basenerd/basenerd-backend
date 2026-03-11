"""
Pregame lineup predictions service.

For a given game, fetches lineups from the MLB API and runs matchup
predictions for every batter vs the opposing starting pitcher.
"""

import logging
from services.mlb_api import get_game_feed, get_player_headshot_url
from services.matchup_predict import predict_matchup

log = logging.getLogger(__name__)

# Pitch hand codes from MLB API
_HAND_CODE = {"L": "L", "R": "R", "S": "S"}

# --- Matchup grading ---
_GRADE_SCALE = ["F", "D-", "D", "D+", "C-", "C", "C+", "B-", "B", "B+", "A-", "A", "A+"]


def _matchup_grade(summary):
    """Grade the batter's favorability in this matchup (A+ = great for batter)."""
    if not summary:
        return "C"
    # Composite score: weight OBP/xSLG heavily, penalize K rate
    score = (
        summary.get("obp", 0.300) * 2.0
        + summary.get("xslg", 0.380) * 1.5
        + summary.get("hit_pct", 0.230) * 1.0
        - summary.get("k_pct", 0.225) * 1.5
    )
    # League avg composite ≈ 0.600 + 0.570 + 0.230 - 0.338 = 1.062
    # Range roughly 0.65 (terrible) to 1.50 (elite)
    normalized = (score - 0.65) / (1.50 - 0.65)
    idx = int(round(max(0, min(12, normalized * 12))))
    return _GRADE_SCALE[idx]


def _batter_expected_statline(probs, summary, est_pa):
    """Compute expected statline for a batter given their PA count."""
    if not probs or not summary:
        return None
    est_ab = est_pa * (1 - probs.get("BB", 0) - probs.get("HBP", 0) - probs.get("IBB", 0))
    hit_pct = summary.get("hit_pct", 0)
    hr_pct = probs.get("HR", 0)
    bb_pct = probs.get("BB", 0) + probs.get("IBB", 0)
    k_pct = probs.get("K", 0)
    obp = summary.get("obp", 0)
    xslg = summary.get("xslg", 0)

    return {
        "pa": round(est_pa, 1),
        "ab": round(est_ab, 1),
        "h": round(est_pa * hit_pct, 2),
        "hr": round(est_pa * hr_pct, 2),
        "rbi": round(est_pa * (
            hr_pct * 1.5
            + probs.get("2B", 0) * 0.5
            + probs.get("3B", 0) * 0.8
            + probs.get("1B", 0) * 0.15
        ), 2),
        "bb": round(est_pa * bb_pct, 2),
        "k": round(est_pa * k_pct, 2),
        # Rates for display (more differentiated than tiny expected counts)
        "avg": round(hit_pct / (1 - bb_pct - probs.get("HBP", 0)) if (1 - bb_pct - probs.get("HBP", 0)) > 0 else 0, 3),
        "obp": round(obp, 3),
        "xslg": round(xslg, 3),
        "k_rate": round(k_pct, 3),
        "bb_rate": round(bb_pct, 3),
    }


def _outs_to_ip(outs):
    """Convert total outs to baseball IP notation (e.g. 17 outs → '5.2')."""
    full = int(outs) // 3
    remainder = int(outs) % 3
    return f"{full}.{remainder}"


def _pitcher_expected_statline(batters, is_spring):
    """Compute expected pitcher statline by aggregating batter predictions."""
    valid = [b for b in batters if b.get("probs")]
    if not valid:
        return None

    n = len(valid)
    avg_out = sum(b["probs"].get("OUT", 0) + b["probs"].get("K", 0) for b in valid) / n
    avg_k = sum(b["probs"].get("K", 0) for b in valid) / n
    avg_bb = sum(b["probs"].get("BB", 0) + b["probs"].get("IBB", 0) + b["probs"].get("HBP", 0) for b in valid) / n
    avg_hit = sum(b["summary"].get("hit_pct", 0) for b in valid) / n
    avg_hr = sum(b["probs"].get("HR", 0) for b in valid) / n

    # Estimated batters faced:
    # Regular season starter avg ~24 BF, ~5.1 IP (2024 MLB avg)
    # Spring training starter ~12 BF, ~2.2 IP (short outings)
    est_bf = 12 if is_spring else 24
    est_outs = est_bf * avg_out
    ip_str = _outs_to_ip(round(est_outs))

    # ER estimate: calibrated so league-avg starter ≈ 4.00-4.30 ERA
    # HR worth ~1.4 runs, other hits ~0.25 run contribution, walks ~0.12
    er_per_bf = avg_hr * 1.4 + (avg_hit - avg_hr) * 0.25 + avg_bb * 0.12
    est_er = est_bf * er_per_bf

    return {
        "ip": ip_str,
        "k": round(est_bf * avg_k, 1),
        "bb": round(est_bf * avg_bb, 1),
        "h": round(est_bf * avg_hit, 1),
        "er": round(est_er, 1),
    }


def _extract_lineups_from_feed(feed):
    """
    Extract lineups, probables, and venue from a game feed.
    Works for both Preview and Live game states.
    """
    if not feed or feed.get("scheduleOnly"):
        return None

    game_data = feed.get("gameData") or {}
    live_data = feed.get("liveData") or {}
    boxscore = live_data.get("boxscore") or {}

    # Venue
    teams_data = game_data.get("teams") or {}
    home_team = teams_data.get("home") or {}
    away_team = teams_data.get("away") or {}
    venue_abbrev = (home_team.get("abbreviation") or "").upper() or None
    home_name = home_team.get("teamName") or home_team.get("name") or ""
    away_name = away_team.get("teamName") or away_team.get("name") or ""
    home_abbrev = (home_team.get("abbreviation") or "").upper()
    away_abbrev = (away_team.get("abbreviation") or "").upper()
    home_team_id = home_team.get("id")
    away_team_id = away_team.get("id")

    venue_info = game_data.get("venue") or {}
    venue_name = venue_info.get("name") or ""
    venue_id = venue_info.get("id")
    venue_loc = venue_info.get("location") or {}
    venue_city = venue_loc.get("city") or ""
    venue_state = venue_loc.get("stateAbbrev") or venue_loc.get("state") or ""
    venue_location = f"{venue_city}, {venue_state}" if venue_city and venue_state else (venue_city or venue_state)

    # Game date/time
    game_datetime = game_data.get("datetime") or {}
    game_date = game_datetime.get("officialDate") or ""
    game_time = game_datetime.get("time") or ""
    am_pm = game_datetime.get("ampm") or ""
    game_dt_iso = game_datetime.get("dateTime") or ""

    # Game type: R=regular, S=spring, P/F/D/L/W=postseason
    game_info = game_data.get("game") or {}
    game_type = (game_info.get("type") or "R").upper()

    # Player data (has batSide, pitchHand for all players)
    all_players = game_data.get("players") or {}

    def _get_player_hand(player_id, hand_type="batSide"):
        """Get bat side or pitch hand from gameData.players."""
        key = f"ID{player_id}"
        p = all_players.get(key) or {}
        hand_obj = p.get(hand_type) or {}
        return hand_obj.get("code") or "R"

    def _get_player_name(player_id):
        key = f"ID{player_id}"
        p = all_players.get(key) or {}
        return p.get("fullName") or p.get("lastFirstName") or str(player_id)

    def _get_player_position(player_id, team_box):
        """Get position from boxscore player data."""
        players = team_box.get("players") or {}
        key = f"ID{player_id}"
        p = players.get(key) or {}
        pos = p.get("position") or {}
        return pos.get("abbreviation") or "?"

    # Probable pitchers
    probables = game_data.get("probablePitchers") or {}

    def _get_probable(side):
        pp = probables.get(side) or {}
        pid = pp.get("id")
        if not pid:
            return None
        return {
            "id": int(pid),
            "name": _get_player_name(pid),
            "throws": _get_player_hand(pid, "pitchHand"),
            "headshot": None,
        }

    home_pitcher = _get_probable("home")
    away_pitcher = _get_probable("away")

    # Try to get headshots
    for p in [home_pitcher, away_pitcher]:
        if p:
            try:
                p["headshot"] = get_player_headshot_url(p["id"], size=120)
            except Exception:
                pass

    # Lineups from boxscore
    box_teams = boxscore.get("teams") or {}

    def _get_lineup(side):
        team_box = box_teams.get(side) or {}
        batting_order = team_box.get("battingOrder") or []
        if not batting_order:
            return []

        lineup = []
        for spot, pid in enumerate(batting_order, 1):
            pid = int(pid)
            lineup.append({
                "id": pid,
                "name": _get_player_name(pid),
                "stand": _get_player_hand(pid, "batSide"),
                "pos": _get_player_position(pid, team_box),
                "spot": spot,
                "headshot": None,
            })
            try:
                lineup[-1]["headshot"] = get_player_headshot_url(pid, size=60)
            except Exception:
                pass

        return lineup

    away_lineup = _get_lineup("away")
    home_lineup = _get_lineup("home")

    return {
        "away_lineup": away_lineup,
        "home_lineup": home_lineup,
        "away_pitcher": away_pitcher,
        "home_pitcher": home_pitcher,
        "venue": venue_abbrev,
        "venue_name": venue_name,
        "venue_id": venue_id,
        "venue_location": venue_location,
        "home_name": home_name,
        "away_name": away_name,
        "home_abbrev": home_abbrev,
        "away_abbrev": away_abbrev,
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "game_date": game_date,
        "game_time": f"{game_time} {am_pm}".strip(),
        "game_dt_iso": game_dt_iso,
        "game_type": game_type,
        "lineups_posted": bool(away_lineup and home_lineup),
    }


def get_pregame_predictions(game_pk, season=2026):
    """
    For a given game, predict PA outcomes for every batter vs the opposing starter.

    Returns structured dict with both lineups, predictions, and team totals.
    """
    feed = get_game_feed(game_pk)
    lineup_data = _extract_lineups_from_feed(feed)

    if not lineup_data:
        return {"ok": False, "reason": "no_feed"}

    is_spring = lineup_data["game_type"] == "S"

    result = {
        "ok": True,
        "game_pk": game_pk,
        "venue": lineup_data["venue"],
        "venue_name": lineup_data["venue_name"],
        "venue_location": lineup_data.get("venue_location", ""),
        "home_name": lineup_data["home_name"],
        "away_name": lineup_data["away_name"],
        "home_abbrev": lineup_data["home_abbrev"],
        "away_abbrev": lineup_data["away_abbrev"],
        "game_date": lineup_data["game_date"],
        "game_time": lineup_data["game_time"],
        "lineups_posted": lineup_data["lineups_posted"],
        "is_spring": is_spring,
    }

    # Weather
    try:
        from services.weather import fetch_game_weather
        venue_id = lineup_data.get("venue_id")
        game_dt = lineup_data.get("game_dt_iso")
        if venue_id:
            result["weather"] = fetch_game_weather(venue_id, game_dt)
        else:
            result["weather"] = None
    except Exception as e:
        log.warning("Weather fetch failed: %s", e)
        result["weather"] = None

    # Weather HR/XBH factors for adjusting predictions
    wx_hr_factor = 1.0
    wx_xbh_factor = 1.0
    if result.get("weather") and result["weather"].get("impact"):
        wx_hr_factor = result["weather"]["impact"].get("hr_factor", 1.0)
        wx_xbh_factor = result["weather"]["impact"].get("xbh_factor", 1.0)

    def _predict_lineup(lineup, pitcher, side_label):
        """Run predictions for a lineup vs a pitcher."""
        if not lineup or not pitcher:
            return {
                "pitcher": pitcher,
                "batters": [],
                "totals": {},
                "pitcher_statline": None,
            }

        batters = []
        total_exp_k = 0
        total_exp_hr = 0
        total_exp_h = 0
        total_exp_bb = 0

        for batter in lineup:
            # Estimate inning based on lineup spot
            est_inning = (batter["spot"] - 1) // 3 + 1

            pred = predict_matchup(
                batter_id=batter["id"],
                pitcher_id=pitcher["id"],
                stand=batter["stand"],
                p_throws=pitcher["throws"],
                venue=lineup_data["venue"],
                season=season,
                inning=est_inning,
                outs=0,
                runner_1b=0,
                runner_2b=0,
                runner_3b=0,
                n_thru_order=1,
            )

            if pred.get("ok"):
                probs = dict(pred["probs"])  # copy so we can adjust
                summary = dict(pred["summary"])

                # Apply weather adjustments to HR and XBH
                if wx_hr_factor != 1.0:
                    old_hr = probs.get("HR", 0)
                    new_hr = old_hr * wx_hr_factor
                    delta = new_hr - old_hr
                    probs["HR"] = new_hr
                    # Redistribute: reduce OUT prob by the delta
                    probs["OUT"] = max(0, probs.get("OUT", 0) - delta)
                    summary["hr_pct"] = new_hr

                if wx_xbh_factor != 1.0:
                    for key in ("2B", "3B"):
                        old = probs.get(key, 0)
                        new_val = old * wx_xbh_factor
                        delta = new_val - old
                        probs[key] = new_val
                        probs["OUT"] = max(0, probs.get("OUT", 0) - delta)

                    # Recalculate hit_pct and obp
                    summary["hit_pct"] = probs.get("1B", 0) + probs.get("2B", 0) + probs.get("3B", 0) + probs.get("HR", 0)
                    summary["obp"] = summary["hit_pct"] + probs.get("BB", 0) + probs.get("HBP", 0) + probs.get("IBB", 0)

                # Matchup grade
                grade = _matchup_grade(summary)

                # Expected PA: top of lineup gets more, bottom less
                # Regular season ~4.0 avg, spring training ~2.4 avg
                base_pa = 4.2 if batter["spot"] <= 5 else 3.8
                est_pa = base_pa * (0.6 if is_spring else 1.0)
                statline = _batter_expected_statline(probs, summary, est_pa)

                # Determine hot/cold from recent form
                try:
                    from services.recent_form import get_batter_recent_form
                    rf = get_batter_recent_form(batter["id"])
                    r14_xwoba = rf.get("bat_r14_xwoba", 0.315)
                    # Compare to league avg
                    form_delta = r14_xwoba - 0.315
                    if form_delta > 0.030:
                        form_label = "hot"
                    elif form_delta < -0.030:
                        form_label = "cold"
                    else:
                        form_label = "neutral"
                    form_xwoba = round(r14_xwoba, 3)
                except Exception:
                    form_label = "neutral"
                    form_xwoba = None

                batter_result = {
                    "id": batter["id"],
                    "name": batter["name"],
                    "spot": batter["spot"],
                    "pos": batter["pos"],
                    "stand": batter["stand"],
                    "headshot": batter.get("headshot"),
                    "probs": probs,
                    "summary": summary,
                    "arsenal": pred.get("arsenal", []),
                    "grade": grade,
                    "statline": statline,
                    "form": {
                        "label": form_label,
                        "xwoba_14d": form_xwoba,
                    },
                }
                batters.append(batter_result)

                if statline:
                    total_exp_k += statline.get("k", 0)
                    total_exp_hr += statline.get("hr", 0)
                    total_exp_h += statline.get("h", 0)
                    total_exp_bb += statline.get("bb", 0)
            else:
                batters.append({
                    "id": batter["id"],
                    "name": batter["name"],
                    "spot": batter["spot"],
                    "pos": batter["pos"],
                    "stand": batter["stand"],
                    "headshot": batter.get("headshot"),
                    "probs": {},
                    "summary": {},
                    "arsenal": [],
                    "grade": "C",
                    "statline": None,
                    "form": {"label": "neutral", "xwoba_14d": None},
                })

        n = len(batters) or 1
        totals = {
            "exp_k": round(total_exp_k, 1),
            "exp_hr": round(total_exp_hr, 2),
            "exp_hits": round(total_exp_h, 1),
            "exp_bb": round(total_exp_bb, 1),
        }

        # Pitcher expected statline
        pitcher_statline = _pitcher_expected_statline(batters, is_spring)

        return {
            "pitcher": pitcher,
            "batters": batters,
            "totals": totals,
            "pitcher_statline": pitcher_statline,
        }

    # Away batters vs Home pitcher, Home batters vs Away pitcher
    result["away"] = _predict_lineup(
        lineup_data["away_lineup"],
        lineup_data["home_pitcher"],
        "away",
    )
    result["home"] = _predict_lineup(
        lineup_data["home_lineup"],
        lineup_data["away_pitcher"],
        "home",
    )

    # Bullpen availability
    try:
        from services.bullpen_availability import get_bullpen_availability
        game_date = lineup_data.get("game_date") or ""
        home_tid = lineup_data.get("home_team_id")
        away_tid = lineup_data.get("away_team_id")
        if home_tid:
            result["home_bullpen"] = get_bullpen_availability(
                home_tid, game_date, lineup_data.get("home_abbrev", "")
            )
        if away_tid:
            result["away_bullpen"] = get_bullpen_availability(
                away_tid, game_date, lineup_data.get("away_abbrev", "")
            )
    except Exception as e:
        log.warning("Bullpen availability failed: %s", e)
        result["home_bullpen"] = []
        result["away_bullpen"] = []

    # Monte Carlo game simulation
    try:
        sim = _simulate_game(result)
        result["simulation"] = sim
    except Exception as e:
        log.warning("Simulation failed: %s", e)
        result["simulation"] = None

    return result


def _simulate_game(pregame_data, sims=2000):
    """
    Run Monte Carlo simulation using per-batter outcome probabilities.

    Simulates full 9-inning games, cycling through each lineup.
    Returns win probabilities and expected score.
    """
    import random

    away_batters = pregame_data.get("away", {}).get("batters", [])
    home_batters = pregame_data.get("home", {}).get("batters", [])

    if not away_batters or not home_batters:
        return None

    # Build probability arrays for each batter
    outcomes = ["1B", "2B", "3B", "HR", "BB", "HBP", "K", "OUT"]

    def _build_probs(batters):
        """Build normalized probability arrays for lineup."""
        lineup = []
        for b in batters:
            probs = b.get("probs", {})
            if not probs:
                # Fallback: league-average hitter
                probs = {"1B": 0.15, "2B": 0.045, "3B": 0.004, "HR": 0.033,
                         "BB": 0.08, "HBP": 0.01, "K": 0.22, "OUT": 0.458}
            p = [probs.get(o, 0) for o in outcomes]
            total = sum(p)
            if total > 0:
                p = [x / total for x in p]
            else:
                p = [0.15, 0.045, 0.004, 0.033, 0.08, 0.01, 0.22, 0.458]
            lineup.append(p)
        return lineup

    away_probs = _build_probs(away_batters)
    home_probs = _build_probs(home_batters)

    away_wins = 0
    home_wins = 0
    away_total = 0.0
    home_total = 0.0

    for _ in range(sims):
        away_runs, home_runs = _sim_one_game(away_probs, home_probs, outcomes)
        away_total += away_runs
        home_total += home_runs
        if away_runs > home_runs:
            away_wins += 1
        elif home_runs > away_runs:
            home_wins += 1
        else:
            # Tie: simulate extra innings (simplified coin flip weighted by quality)
            if random.random() < 0.5:
                away_wins += 1
            else:
                home_wins += 1

    return {
        "sims": sims,
        "awayWinPct": round(away_wins / sims, 4),
        "homeWinPct": round(home_wins / sims, 4),
        "expectedScore": {
            "away": round(away_total / sims, 1),
            "home": round(home_total / sims, 1),
        },
    }


def _sim_one_game(away_probs, home_probs, outcomes):
    """Simulate a single 9-inning game. Returns (away_runs, home_runs)."""
    import random

    def _sim_half(lineup_probs, batter_idx):
        """Simulate one half-inning. Returns (runs_scored, new_batter_idx)."""
        outs = 0
        runners = 0  # bitmask: 1=1B, 2=2B, 4=3B
        runs = 0
        idx = batter_idx

        while outs < 3:
            probs = lineup_probs[idx % len(lineup_probs)]
            idx += 1

            # Sample outcome
            r = random.random()
            cumulative = 0
            outcome = "OUT"
            for i, p in enumerate(probs):
                cumulative += p
                if r < cumulative:
                    outcome = outcomes[i]
                    break

            if outcome == "K" or outcome == "OUT":
                outs += 1
            elif outcome == "BB" or outcome == "HBP":
                # Force walk advancement
                if runners & 1:
                    if runners & 2:
                        if runners & 4:
                            runs += 1  # bases loaded, score from 3rd
                        runners |= 4
                    runners |= 2
                runners |= 1
            elif outcome == "1B":
                # Runner on 3B scores, 2B scores (80%), 1B to 2B
                if runners & 4:
                    runs += 1
                    runners &= ~4
                if runners & 2:
                    if random.random() < 0.80:
                        runs += 1
                        runners &= ~2
                    else:
                        runners |= 4
                        runners &= ~2
                if runners & 1:
                    runners |= 2
                runners |= 1
            elif outcome == "2B":
                # All runners score except runner on 1B goes to 3B (60% scores)
                if runners & 4:
                    runs += 1
                    runners &= ~4
                if runners & 2:
                    runs += 1
                    runners &= ~2
                if runners & 1:
                    if random.random() < 0.60:
                        runs += 1
                    else:
                        runners |= 4
                    runners &= ~1
                runners |= 2
            elif outcome == "3B":
                # All runners score
                runs += bin(runners).count('1')
                runners = 4
            elif outcome == "HR":
                runs += bin(runners).count('1') + 1
                runners = 0

        return runs, idx

    away_runs = 0
    home_runs = 0
    away_idx = 0
    home_idx = 0

    for inning in range(1, 10):
        r, away_idx = _sim_half(away_probs, away_idx)
        away_runs += r

        # Bottom of 9th: skip if home team already ahead
        if inning == 9 and home_runs > away_runs:
            break
        r, home_idx = _sim_half(home_probs, home_idx)
        home_runs += r

        # Walk-off
        if inning >= 9 and home_runs > away_runs:
            break

    return away_runs, home_runs
