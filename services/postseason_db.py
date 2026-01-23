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
    Returns a bracket-ready structure with explicit slots:

    playoff_picture = {
      "AL": {"wc_top": s, "wc_bot": s, "ds_top": s, "ds_bot": s, "cs": s},
      "NL": {"wc_top": s, "wc_bot": s, "ds_top": s, "ds_bot": s, "cs": s},
      "WS": {"ws": s, "champion": team_dict_or_none}
    }

    Each series s:
      {
        "series_id", "league", "game_type", "round_name", "best_of", "status",
        "teams": [ {seed, team_id, abbrev, logo_url, wins, is_winner}, ... ],
        "winner": {...} or None,
        "winner_text": "TOR wins, 4-3" / "TOR leads, 2-1" / None
      }
    """
    # ---- group rows into series objects ----
    by_series: dict[str, dict] = {}
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
                "winner": None,
                "winner_text": None,
            }

        team_id = r.get("team_id")
        by_series[sid]["teams"].append({
            "seed": r.get("seed"),
            "team_id": team_id,
            "abbrev": (r.get("abbrev") or "").upper(),
            "logo_url": f"https://www.mlbstatic.com/team-logos/{int(team_id)}.svg" if team_id else None,
            "wins": int(r.get("wins") or 0),
            "is_winner": bool(r.get("is_winner")),
        })

    # sort teams inside each series by seed then abbrev
    for s in by_series.values():
        s["teams"].sort(key=lambda t: (t["seed"] if t["seed"] is not None else 99, t["abbrev"]))

        # derive winner + winner_text
        teams = s["teams"]
        if len(teams) >= 2:
            # winner: prefer is_winner flag; otherwise current wins leader
            winner = next((t for t in teams if t["is_winner"]), None)
            if not winner:
                winner = sorted(teams, key=lambda t: (t["wins"], -(t["seed"] or 99)), reverse=True)[0]

            loser = [t for t in teams if t is not winner][0]

            s["winner"] = winner

            best_of = s.get("best_of")
            status = (s.get("status") or "").lower()
            is_final = (status == "final")
            if best_of:
                needed = (best_of // 2) + 1
                is_final = is_final or (winner["wins"] >= needed)

            if is_final:
                s["winner_text"] = f"{winner['abbrev']} wins, {winner['wins']}-{loser['wins']}"
            else:
                # only show if there are games played
                if winner["wins"] > 0 or loser["wins"] > 0:
                    s["winner_text"] = f"{winner['abbrev']} leads, {winner['wins']}-{loser['wins']}"
                else:
                    s["winner_text"] = None

    # ---- helper to pick series by seeds ----
    def _seed_set(series):
        return set([t["seed"] for t in series["teams"] if t.get("seed") is not None])

    def _find_series(series_list, predicate):
        for s in series_list:
            if predicate(s):
                return s
        return None

    # ---- bucket by league + round ----
    leagues = {"AL": {"F": [], "D": [], "L": []},
               "NL": {"F": [], "D": [], "L": []}}
    ws_series = None

    for s in by_series.values():
        lg = s.get("league")
        gt = s.get("game_type")
        if lg == "WS" and gt == "W":
            ws_series = s
        elif lg in ("AL", "NL") and gt in ("F", "D", "L"):
            leagues[lg][gt].append(s)

    # ---- assign bracket slots (modern MLB format) ----
    out = {
        "AL": {"wc_top": None, "wc_bot": None, "ds_top": None, "ds_bot": None, "cs": None},
        "NL": {"wc_top": None, "wc_bot": None, "ds_top": None, "ds_bot": None, "cs": None},
        "WS": {"ws": ws_series, "champion": None},
    }

    for lg in ("AL", "NL"):
        wc = leagues[lg]["F"]
        ds = leagues[lg]["D"]
        cs = leagues[lg]["L"]

        # WC: (3 vs 6) and (4 vs 5)
        out[lg]["wc_top"] = _find_series(wc, lambda s: _seed_set(s) == {3, 6}) or (wc[0] if len(wc) > 0 else None)
        out[lg]["wc_bot"] = _find_series(wc, lambda s: _seed_set(s) == {4, 5}) or (wc[1] if len(wc) > 1 else None)

        # DS: seed 1 series + seed 2 series
        out[lg]["ds_top"] = _find_series(ds, lambda s: 1 in _seed_set(s)) or (ds[0] if len(ds) > 0 else None)
        out[lg]["ds_bot"] = _find_series(ds, lambda s: 2 in _seed_set(s)) or (ds[1] if len(ds) > 1 else None)

        # CS: only one
        out[lg]["cs"] = cs[0] if len(cs) > 0 else None

    # Champion (WS winner)
    if ws_series and ws_series.get("winner"):
        out["WS"]["champion"] = ws_series["winner"]

    return out

