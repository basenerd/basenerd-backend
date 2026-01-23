# services/postseason.py
import requests

STATSAPI_BASE = "https://statsapi.mlb.com/api/v1"

def get_postseason_series(season: int) -> dict:
    """
    Fetch postseason schedule grouped by series for a given season.
    Endpoint: /schedule/postseason/series
    """
    url = f"{STATSAPI_BASE}/schedule/postseason/series"
    params = {"season": str(season), "sportId": "1"}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def build_playoff_bracket(series_json: dict) -> dict:
    """
    Normalize StatsAPI series response into an easy-to-render structure.

    Returns:
      {
        "AL": { "<Round Name>": [ {series...}, ... ], ... },
        "NL": { "<Round Name>": [ ... ], ... },
        "WS": { "<Round Name>": [ ... ], ... },
      }
    """
    out = {"AL": {}, "NL": {}, "WS": {}}
    series_list = series_json.get("series", []) or []

    for s in series_list:
        round_name = (
            (s.get("round") or {}).get("name")
            or s.get("roundName")
            or s.get("seriesDescription")
            or s.get("name")
            or "Postseason"
        )

        lg = (
            (s.get("league") or {}).get("abbreviation")
            or (s.get("league") or {}).get("name")
            or s.get("leagueName")
            or ""
        )
        lg = "AL" if ("American" in lg or lg == "AL") else "NL" if ("National" in lg or lg == "NL") else "WS"

        matchup = {
            "seriesNumber": s.get("seriesNumber"),
            "bestOf": s.get("gamesInSeries") or s.get("bestOf") or None,
            "status": (s.get("status") or {}).get("detailedState")
                      or (s.get("status") or {}).get("abstractGameState")
                      or None,
            "teams": [],
            "link": s.get("link"),
        }

        teams_blob = s.get("teams") or s.get("matchupTeams") or {}
        candidates = []
        if isinstance(teams_blob, dict):
            for k in ("home", "away", "team1", "team2"):
                if teams_blob.get(k):
                    candidates.append(teams_blob.get(k))

        for t in candidates:
            team_obj = t.get("team") or t.get("club") or {}
            team_id = team_obj.get("id")
            abbrev = (team_obj.get("abbreviation") or team_obj.get("abbrev") or "").upper()

            wins = t.get("wins")
            if wins is None:
                wins = t.get("seriesWins") or t.get("score")

            matchup["teams"].append({
                "team_id": team_id,
                "abbrev": abbrev,
                "logo_url": f"https://www.mlbstatic.com/team-logos/{team_id}.svg" if team_id else None,
                "wins": wins,
                "is_winner": t.get("isWinner"),
            })

        out[lg].setdefault(round_name, []).append(matchup)

    return out
