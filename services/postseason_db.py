# services/postseason_db.py
import os
from collections import defaultdict

# Support psycopg3 or psycopg2
try:
    import psycopg  # type: ignore
    _PSYCOPG3 = True
except Exception:
    psycopg = None
    _PSYCOPG3 = False

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except Exception:
    psycopg2 = None


def _get_conn():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")

    if _PSYCOPG3:
        return psycopg.connect(db_url)
    if psycopg2:
        return psycopg2.connect(db_url)

    raise RuntimeError("Neither psycopg (v3) nor psycopg2 is installed")


def fetch_postseason_series_rows(season: int) -> list[dict]:
    """
    Pull series + teams (with seeds) from your views:
      - vw_postseason_series
      - vw_postseason_series_team_enriched
    """
    sql = """
    SELECT
      s.series_id,
      s.season,
      s.league,
      s.game_type,
      s.round_name,
      s.sort_order,
      s.best_of,
      s.status,

      t.team_id,
      t.abbrev,
      t.seed,
      t.wins,
      t.is_winner

    FROM vw_postseason_series s
    JOIN vw_postseason_series_team_enriched t
      ON t.series_id = s.series_id
    WHERE s.season = %s
    ORDER BY
      s.sort_order,
      s.league,
      s.series_id,
      t.seed NULLS LAST,
      t.wins DESC;
    """

    conn = _get_conn()
    try:
        if _PSYCOPG3:
            with conn.cursor() as cur:
                cur.execute(sql, (season,))
                cols = [d.name for d in cur.description]
                out = []
                for r in cur.fetchall():
                    out.append({cols[i]: r[i] for i in range(len(cols))})
                return out
        else:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:  # type: ignore
                cur.execute(sql, (season,))
                return list(cur.fetchall())
    finally:
        conn.close()


def build_playoff_picture(rows: list[dict]) -> dict:
    """
    Returns structure for bracket:
    {
      "AL": {"F":[series...], "D":[series...], "L":[series...]},
      "NL": {"F":[...], "D":[...], "L":[...]},
      "WS": {"W":[...]}
    }
    where each series is:
      {
        "series_id": ...,
        "best_of": ...,
        "status": ...,
        "teams": [ {seed, team_id, abbrev, logo_url, wins, is_winner}, ... ]  # 2 items
      }
    """
    # group by series_id
    by_series = {}
    for r in rows:
        sid = r["series_id"]
        if sid not in by_series:
            by_series[sid] = {
                "series_id": sid,
                "season": r.get("season"),
                "league": r.get("league"),
                "game_type": r.get("game_type"),
                "round_name": r.get("round_name"),
                "sort_order": r.get("sort_order"),
                "best_of": r.get("best_of"),
                "status": r.get("status"),
                "teams": [],
            }

        team_id = r.get("team_id")
        by_series[sid]["teams"].append({
            "seed": r.get("seed"),
            "team_id": team_id,
            "abbrev": (r.get("abbrev") or "").upper(),
            "logo_url": f"https://www.mlbstatic.com/team-logos/{int(team_id)}.svg" if team_id else None,
            "wins": r.get("wins") or 0,
            "is_winner": bool(r.get("is_winner")),
        })

    # sort teams inside each series by seed (fallback), then abbrev
    for s in by_series.values():
        s["teams"].sort(key=lambda t: (t["seed"] if t["seed"] is not None else 99, t["abbrev"]))

    picture = {"AL": {"F": [], "D": [], "L": []},
               "NL": {"F": [], "D": [], "L": []},
               "WS": {"W": []}}

    # distribute
    for s in by_series.values():
        lg = s.get("league") or "UNK"
        gt = s.get("game_type")
        if lg == "WS":
            if gt == "W":
                picture["WS"]["W"].append(s)
        elif lg in ("AL", "NL"):
            if gt in ("F", "D", "L"):
                picture[lg][gt].append(s)

    # stable ordering within rounds
    def _series_sort_key(s):
        # mostly series_id (which includes team ids), but keep deterministic
        return (s.get("sort_order") or 99, s.get("series_id") or "")

    for lg in ("AL", "NL"):
        for gt in ("F", "D", "L"):
            picture[lg][gt].sort(key=_series_sort_key)
    picture["WS"]["W"].sort(key=_series_sort_key)

    return picture
