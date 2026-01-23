# services/standings_db.py
import os
import psycopg


def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def fetch_standings_ranked(season: int) -> list[dict]:
    sql = """
    SELECT
      s.season,
      ssf.league,
      ssf.division,
      s.team_id,
      s.team_abbrev,
      s.team_name,
      s.w,
      s.l,
      s.pct,
      s.gb,
      s.wc_gb,
      s.rs,
      s.ra,
      (s.rs - s.ra) AS run_differential,
      s.streak,

      -- Official MLB rank fields (tiebreak-safe)
      s.division_rank,
      s.wild_card_rank,

      s.last_updated

    FROM standings s
    JOIN standings_season_final ssf
      ON ssf.team_id = s.team_id
     AND ssf.season = s.season

    WHERE s.season = %s

    ORDER BY ssf.league, ssf.division, s.division_rank NULLS LAST, s.pct DESC;
    """

    with psycopg.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (season,))
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def build_divs(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    by_league_div: dict[tuple[str, str], list[dict]] = {}

    for r in rows:
        league = (r.get("league") or "").strip()
        division = (r.get("division") or "").strip()
        by_league_div.setdefault((league, division), []).append(r)

    al_divs: list[dict] = []
    nl_divs: list[dict] = []

    for (league, division), teams in by_league_div.items():
        # Stable sort: division_rank first, then pct desc
        teams_sorted = sorted(
            teams,
            key=lambda x: (
                x.get("division_rank") if x.get("division_rank") is not None else 999,
                -(x.get("pct") or 0),
            ),
        )

        mapped: list[dict] = []
        for t in teams_sorted:
            team_id = int(t["team_id"])

            division_rank = t.get("division_rank")
            wild_card_rank = t.get("wild_card_rank")

            is_winner = (division_rank == 1)
            is_wc = (wild_card_rank in (1, 2, 3)) and not is_winner

            mapped.append(
                {
                    "team_id": team_id,
                    "abbrev": t.get("team_abbrev") or "",
                    "w": t.get("w"),
                    "l": t.get("l"),

                    # Keep pct numeric for template formatting/gradient; template can format to .432
                    "pct": t.get("pct"),

                    "gb": t.get("gb") or "—",
                    "wc_gb": t.get("wc_gb") or "—",
                    "streak": t.get("streak") or "—",
                    "run_diff": t.get("run_differential"),
                    "logo_url": f"https://www.mlbstatic.com/team-logos/{team_id}.svg",

                    # ranks + derived flags
                    "division_rank": division_rank,
                    "wild_card_rank": wild_card_rank,
                    "division_leader": 1 if is_winner else 0,
                    "wild_card": 1 if is_wc else 0,
                }
            )

        div_obj = {"name": division, "teams": mapped}

        if league == "American League":
            al_divs.append(div_obj)
        elif league == "National League":
            nl_divs.append(div_obj)

    # Keep division order stable
    def div_sort_key(d: dict) -> int:
        order = {
            "American League East": 1,
            "American League Central": 2,
            "American League West": 3,
            "National League East": 1,
            "National League Central": 2,
            "National League West": 3,
        }
        return order.get(d.get("name", ""), 99)

    al_divs.sort(key=div_sort_key)
    nl_divs.sort(key=div_sort_key)

    return al_divs, nl_divs
