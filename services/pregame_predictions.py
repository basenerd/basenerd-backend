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

    venue_info = game_data.get("venue") or {}
    venue_name = venue_info.get("name") or ""

    # Game date/time
    game_datetime = game_data.get("datetime") or {}
    game_date = game_datetime.get("officialDate") or ""
    game_time = game_datetime.get("time") or ""
    am_pm = game_datetime.get("ampm") or ""

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
        "home_name": home_name,
        "away_name": away_name,
        "home_abbrev": home_abbrev,
        "away_abbrev": away_abbrev,
        "game_date": game_date,
        "game_time": f"{game_time} {am_pm}".strip(),
        "lineups_posted": bool(away_lineup and home_lineup),
    }


def get_pregame_predictions(game_pk, season=2025):
    """
    For a given game, predict PA outcomes for every batter vs the opposing starter.

    Returns structured dict with both lineups, predictions, and team totals.
    """
    feed = get_game_feed(game_pk)
    lineup_data = _extract_lineups_from_feed(feed)

    if not lineup_data:
        return {"ok": False, "reason": "no_feed"}

    result = {
        "ok": True,
        "game_pk": game_pk,
        "venue": lineup_data["venue"],
        "venue_name": lineup_data["venue_name"],
        "home_name": lineup_data["home_name"],
        "away_name": lineup_data["away_name"],
        "home_abbrev": lineup_data["home_abbrev"],
        "away_abbrev": lineup_data["away_abbrev"],
        "game_date": lineup_data["game_date"],
        "game_time": lineup_data["game_time"],
        "lineups_posted": lineup_data["lineups_posted"],
    }

    def _predict_lineup(lineup, pitcher, side_label):
        """Run predictions for a lineup vs a pitcher."""
        if not lineup or not pitcher:
            return {
                "pitcher": pitcher,
                "batters": [],
                "totals": {},
            }

        batters = []
        total_k = 0
        total_hr = 0
        total_hit = 0
        total_bb = 0

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
                probs = pred["probs"]
                summary = pred["summary"]

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
                    rf = {}

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
                    "form": {
                        "label": form_label,
                        "xwoba_14d": form_xwoba,
                    },
                }
                batters.append(batter_result)

                total_k += probs.get("K", 0)
                total_hr += probs.get("HR", 0)
                total_hit += summary.get("hit_pct", 0)
                total_bb += probs.get("BB", 0) + probs.get("IBB", 0)
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
                    "form": {"label": "neutral", "xwoba_14d": None},
                })

        n = len(batters) or 1
        totals = {
            "exp_k": round(total_k, 1),
            "exp_hr": round(total_hr, 2),
            "exp_hits": round(total_hit * n, 1),  # approximate
            "exp_bb": round(total_bb, 1),
            "avg_k_pct": round(total_k / n, 3),
            "avg_hr_pct": round(total_hr / n, 3),
        }

        return {
            "pitcher": pitcher,
            "batters": batters,
            "totals": totals,
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

    return result
