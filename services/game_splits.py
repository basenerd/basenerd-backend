# services/game_splits.py
"""
Interactive Splits explorer for the game page.

For the two teams in a game, computes SEASON batting or pitching stat lines from the
raw `statcast_pitches` table, letting the user mix-and-match filters:
  - side:      batting | pitching
  - group:     player  | team
  - team:      both | away | home
  - hand:      L | R | all          (batting -> pitcher hand `p_throws`; pitching -> batter hand `stand`)
  - venue:     home | away | all
  - daynight:  day | night | all
  - range:     season | 7d | 14d | 30d | g7 | g15 | g30   (date window or last-N games)
  - count:     any | first | ahead | behind | even | 2k | 3b
  - bases:     any | risp | empty | on | loaded

MLB StatsAPI only serves individual pre-computed splits, never arbitrary intersections,
so everything is computed here from pitch-level data. Day/night and home/away are derived
from a cached schedule map (game_pk -> dayNight, home/away team) via `game_pk = ANY(...)`,
avoiding any dependency on the (unconfirmed) `inning_topbot`/`home_team` Statcast columns.
"""
from __future__ import annotations

import os
import math
import time
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import psycopg

from services.mlb_api import (
    get_game_feed,
    get_40man_roster_grouped,
    get_team_schedule,
)

# --- Statcast description sets (mirror services/pitching_report.py) ---
SWING_DESC = (
    "swinging_strike", "swinging_strike_blocked", "foul", "foul_tip", "foul_bunt",
    "hit_into_play", "hit_into_play_no_out", "hit_into_play_score",
)
WHIFF_DESC = ("swinging_strike", "swinging_strike_blocked")

# --- wOBA linear weights (approx. modern-era constants) ---
W_uBB, W_HBP, W_1B, W_2B, W_3B, W_HR = 0.69, 0.72, 0.88, 1.24, 1.56, 2.00

_CACHE: Dict[str, Tuple[float, Any]] = {}
_CACHE_TTL = 300  # seconds


def _db_url() -> str:
    url = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_URL_PG") or ""
    if not url:
        raise RuntimeError("Missing DATABASE_URL env var")
    return url


def _norm(v: Optional[str]) -> Optional[str]:
    """Normalize an incoming filter value; treat blank/all/any as unset (None)."""
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("", "all", "any", "none"):
        return None
    return s


# ----------------------------------------------------------------------------
# Roster + schedule resolution
# ----------------------------------------------------------------------------
def _resolve_game(game_pk: int) -> Optional[dict]:
    feed = get_game_feed(game_pk)
    if not feed or feed.get("scheduleOnly"):
        gd = (feed or {}).get("gameData") or {}
        if not gd:
            return None
    gd = (feed or {}).get("gameData") or {}
    teams = gd.get("teams") or {}
    away = teams.get("away") or {}
    home = teams.get("home") or {}
    if not away.get("id") or not home.get("id"):
        return None
    season = None
    try:
        season = int((gd.get("game") or {}).get("season"))
    except Exception:
        pass
    if not season:
        d = (gd.get("datetime") or {}).get("originalDate") or (gd.get("datetime") or {}).get("officialDate")
        if d:
            try:
                season = int(str(d)[:4])
            except Exception:
                season = None
    if not season:
        season = date.today().year
    return {
        "season": season,
        "away": {"id": int(away["id"]), "abbrev": away.get("abbreviation") or "AWY", "name": away.get("name")},
        "home": {"id": int(home["id"]), "abbrev": home.get("abbreviation") or "HOM", "name": home.get("name")},
    }


def _roster_players(team_id: int, abbrev: str) -> List[dict]:
    """Flatten the 40-man roster into [{id, name, bats, throws, team}]."""
    out: List[dict] = []
    try:
        grouped, other = get_40man_roster_grouped(team_id)
    except Exception as e:
        print(f"[game_splits] roster fetch failed team={team_id}: {e}")
        return out
    buckets = list(grouped.values()) + [other]
    for bucket in buckets:
        for r in bucket:
            pid = r.get("id")
            if not pid:
                continue
            bt = r.get("bt") or ""  # e.g. "L/R" = bats/throws
            bats = throws = None
            if "/" in bt:
                bats, throws = (bt.split("/", 1) + [None])[:2]
            out.append({
                "id": int(pid),
                "name": r.get("name") or str(pid),
                "bats": (bats or "").upper() or None,
                "throws": (throws or "").upper() or None,
                "team": abbrev,
            })
    return out


def _schedule_pk_sets(team_id: int, season: int) -> dict:
    """
    From the cached team schedule, derive regular-season game_pk sets used for the
    home/away and day/night filters, plus an ordered (recent-first) list for last-N-games.
    """
    home_pks: List[int] = []
    away_pks: List[int] = []
    day_pks: List[int] = []
    night_pks: List[int] = []
    dated: List[Tuple[str, int]] = []  # (game_date, game_pk)
    try:
        data = get_team_schedule(team_id, season) or {}
    except Exception as e:
        print(f"[game_splits] schedule fetch failed team={team_id}: {e}")
        data = {}
    today = date.today().isoformat()
    for d in (data.get("dates") or []):
        for g in (d.get("games") or []):
            if (g.get("gameType") or "").upper() != "R":
                continue
            try:
                pk = int(g.get("gamePk"))
            except Exception:
                continue
            gdate = (g.get("gameDate") or d.get("date") or "")[:10]
            gteams = g.get("teams") or {}
            hid = (((gteams.get("home") or {}).get("team")) or {}).get("id")
            aid = (((gteams.get("away") or {}).get("team")) or {}).get("id")
            if hid == team_id:
                home_pks.append(pk)
            elif aid == team_id:
                away_pks.append(pk)
            dn = (g.get("dayNight") or "").lower()
            if dn == "day":
                day_pks.append(pk)
            elif dn == "night":
                night_pks.append(pk)
            # only completed/started games count toward "last N games"
            if gdate and gdate <= today:
                dated.append((gdate, pk))
    dated.sort(key=lambda t: t[0], reverse=True)
    ordered_recent = [pk for _, pk in dated]
    return {
        "home": home_pks, "away": away_pks,
        "day": day_pks, "night": night_pks,
        "recent": ordered_recent,
    }


# ----------------------------------------------------------------------------
# Stat math
# ----------------------------------------------------------------------------
def _rates(c: dict) -> dict:
    """Turn raw event counts into a stat line (works for batting or pitching-against)."""
    pa = c["pa"]
    ubb, ibb, hbp = c["ubb"], c["ibb"], c["hbp"]
    sf, sh, ci = c["sf"], c["sh"], c["ci"]
    h, d2, d3, hr = c["h"], c["d2"], c["d3"], c["hr"]
    bb = ubb + ibb
    ab = pa - bb - hbp - sf - sh - ci
    b1 = h - d2 - d3 - hr

    def _r(n, d, nd=3):
        return round(n / d, nd) if d else None

    avg = _r(h, ab)
    obp_den = ab + bb + hbp + sf
    obp = _r(h + bb + hbp, obp_den)
    slg = _r(b1 + 2 * d2 + 3 * d3 + 4 * hr, ab)
    ops = round((obp or 0) + (slg or 0), 3) if (obp is not None and slg is not None) else None
    woba_den = ab + ubb + sf + hbp
    woba_num = W_uBB * ubb + W_HBP * hbp + W_1B * b1 + W_2B * d2 + W_3B * d3 + W_HR * hr
    woba = _r(woba_num, woba_den)
    swings, whiffs = c["swings"], c["whiffs"]
    return {
        "pa": pa, "ab": ab, "h": h, "2b": d2, "3b": d3, "hr": hr,
        "bb": bb, "so": c["k"],
        "avg": avg, "obp": obp, "slg": slg, "ops": ops,
        "woba": woba,
        "xwoba": round(c["xwoba"], 3) if c["xwoba"] is not None else None,
        "k_pct": _r(c["k"], pa, 3), "bb_pct": _r(bb, pa, 3),
        "whiff_pct": _r(whiffs, swings, 3),
    }


def _empty_counts() -> dict:
    keys = ["pa", "h", "d2", "d3", "hr", "ubb", "ibb", "hbp", "sf", "sh", "ci", "k", "swings", "whiffs"]
    c = {k: 0 for k in keys}
    c["xwoba"] = None
    return c


def _accumulate(dst: dict, row: dict) -> None:
    for k in ("pa", "h", "d2", "d3", "hr", "ubb", "ibb", "hbp", "sf", "sh", "ci", "k", "swings", "whiffs"):
        dst[k] += int(row.get(k) or 0)
    # xwoba is a PA-weighted average carried as (sum, denom) during accumulation
    xw = row.get("xwoba")
    xd = int(row.get("xwoba_den") or 0)
    if xw is not None and xd:
        dst.setdefault("_xw_sum", 0.0)
        dst.setdefault("_xw_den", 0)
        dst["_xw_sum"] += float(xw) * xd
        dst["_xw_den"] += xd


def _finalize_xwoba(dst: dict) -> None:
    den = dst.get("_xw_den") or 0
    dst["xwoba"] = (dst["_xw_sum"] / den) if den else None


# ----------------------------------------------------------------------------
# Query builder
# ----------------------------------------------------------------------------
def _build_query(side: str, group: str, filters: dict, pids: List[int],
                 season: int, pk_sets_by_team: dict, team_of: Dict[int, int]) -> Tuple[str, dict, bool]:
    """
    Returns (sql, params, need_window). id_col is batter/pitcher; hand_col flips by side.
    Home/away and day/night filters union the relevant pk-sets across the teams in scope.
    """
    id_col = "batter" if side == "batting" else "pitcher"
    hand_col = "p_throws" if side == "batting" else "stand"

    conds = [f"{id_col} = ANY(%(pids)s)", "game_type = 'R'", "game_year = %(year)s"]
    params: Dict[str, Any] = {"pids": pids, "year": season}

    hand = _norm(filters.get("hand"))
    if hand in ("l", "r"):
        conds.append(f"UPPER({hand_col}) = %(hand)s")
        params["hand"] = hand.upper()

    # Collect which teams are in scope so we can union their schedule pk-sets.
    teams_in_scope = sorted(set(team_of.values()))

    def _union_pks(key: str) -> List[int]:
        out: List[int] = []
        for tid in teams_in_scope:
            out.extend(pk_sets_by_team.get(tid, {}).get(key, []))
        return out

    venue = _norm(filters.get("venue"))
    if venue in ("home", "away"):
        params["venue_pks"] = _union_pks(venue)
        conds.append("game_pk = ANY(%(venue_pks)s)")

    daynight = _norm(filters.get("daynight"))
    if daynight in ("day", "night"):
        params["dn_pks"] = _union_pks(daynight)
        conds.append("game_pk = ANY(%(dn_pks)s)")

    # Date window vs last-N games.
    # last-N-games uses each in-scope team's most recent N regular-season game_pks
    # (from the schedule map). A player only has rows in their own team's games, so this
    # correctly means "the team's last N games" for both the player and team views, and
    # stays index-friendly (no per-row window function).
    rng = _norm(filters.get("range"))
    need_window = False
    if rng and rng.endswith("d") and rng[:-1].isdigit():
        cutoff = (date.today() - timedelta(days=int(rng[:-1]))).isoformat()
        conds.append("game_date >= %(cutoff)s")
        params["cutoff"] = cutoff
    elif rng and rng.startswith("g") and rng[1:].isdigit():
        n = int(rng[1:])
        lastn_pks: List[int] = []
        for tid in teams_in_scope:
            lastn_pks.extend(pk_sets_by_team.get(tid, {}).get("recent", [])[:n])
        params["lastn_pks"] = lastn_pks
        conds.append("game_pk = ANY(%(lastn_pks)s)")

    # Count state
    count = _norm(filters.get("count"))
    if count == "first":
        conds.append("balls = 0 AND strikes = 0")
    elif count == "2k":
        conds.append("strikes = 2")
    elif count == "3b":
        conds.append("balls = 3")
    elif count == "even":
        conds.append("balls = strikes")
    elif count in ("ahead", "behind"):
        # From the subject's perspective: batting subject ahead = more balls than strikes.
        if (side == "batting") == (count == "ahead"):
            conds.append("balls > strikes")
        else:
            conds.append("strikes > balls")

    # Base state (on_1b/2b/3b hold runner ids or NULL)
    def _occ(col: str) -> str:
        return f"({col} IS NOT NULL AND {col}::text NOT IN ('', 'nan', 'NaN'))"

    bases = _norm(filters.get("bases"))
    if bases == "risp":
        conds.append(f"({_occ('on_2b')} OR {_occ('on_3b')})")
    elif bases == "empty":
        conds.append(f"(NOT {_occ('on_1b')} AND NOT {_occ('on_2b')} AND NOT {_occ('on_3b')})")
    elif bases == "on":
        conds.append(f"({_occ('on_1b')} OR {_occ('on_2b')} OR {_occ('on_3b')})")
    elif bases == "loaded":
        conds.append(f"({_occ('on_1b')} AND {_occ('on_2b')} AND {_occ('on_3b')})")

    where = " AND ".join(conds)

    swing_in = ", ".join("'%s'" % s for s in SWING_DESC)
    whiff_in = ", ".join("'%s'" % s for s in WHIFF_DESC)
    ev_term = "events IS NOT NULL AND events NOT IN ('', 'nan', 'NaN')"
    xw_ok = "estimated_woba_using_speedangle IS NOT NULL AND estimated_woba_using_speedangle::text NOT IN ('', 'nan', 'NaN')"

    agg = f"""
        {id_col} AS pid,
        COUNT(*) FILTER (WHERE {ev_term}) AS pa,
        COUNT(*) FILTER (WHERE events IN ('single','double','triple','home_run')) AS h,
        COUNT(*) FILTER (WHERE events = 'double') AS d2,
        COUNT(*) FILTER (WHERE events = 'triple') AS d3,
        COUNT(*) FILTER (WHERE events = 'home_run') AS hr,
        COUNT(*) FILTER (WHERE events = 'walk') AS ubb,
        COUNT(*) FILTER (WHERE events = 'intent_walk') AS ibb,
        COUNT(*) FILTER (WHERE events = 'hit_by_pitch') AS hbp,
        COUNT(*) FILTER (WHERE events IN ('sac_fly','sac_fly_double_play')) AS sf,
        COUNT(*) FILTER (WHERE events IN ('sac_bunt','sac_bunt_double_play')) AS sh,
        COUNT(*) FILTER (WHERE events = 'catcher_interf') AS ci,
        COUNT(*) FILTER (WHERE events IN ('strikeout','strikeout_double_play')) AS k,
        COUNT(*) FILTER (WHERE description IN ({swing_in})) AS swings,
        COUNT(*) FILTER (WHERE description IN ({whiff_in})) AS whiffs,
        AVG(estimated_woba_using_speedangle::float) FILTER (WHERE {xw_ok}) AS xwoba,
        COUNT(*) FILTER (WHERE {xw_ok}) AS xwoba_den
    """

    sql = f"SELECT {agg} FROM statcast_pitches WHERE {where} GROUP BY {id_col}"
    return sql, params, need_window


# ----------------------------------------------------------------------------
# Public entry point
# ----------------------------------------------------------------------------
def game_splits(game_pk: int, side: str = "batting", group: str = "player",
                filters: Optional[dict] = None) -> dict:
    filters = filters or {}
    side = "pitching" if str(side).lower().startswith("pitch") else "batting"
    group = "team" if str(group).lower().startswith("team") else "player"

    cache_key = f"{game_pk}|{side}|{group}|" + "|".join(
        f"{k}={_norm(filters.get(k))}" for k in
        ("team", "hand", "venue", "daynight", "range", "count", "bases")
    )
    now = time.time()
    hit = _CACHE.get(cache_key)
    if hit and now - hit[0] < _CACHE_TTL:
        return hit[1]

    info = _resolve_game(game_pk)
    if not info:
        return {"ok": False, "error": "Could not load game", "side": side, "group": group, "rows": []}

    season = info["season"]
    team_sel = _norm(filters.get("team")) or "both"
    which = []
    if team_sel in ("both", "away"):
        which.append(info["away"])
    if team_sel in ("both", "home"):
        which.append(info["home"])
    if team_sel == "home" and not which:
        which = [info["home"]]

    # Rosters -> players; schedule -> pk sets.
    players: List[dict] = []
    pk_sets_by_team: Dict[int, dict] = {}
    team_abbrev: Dict[int, str] = {}
    for t in which:
        players.extend(_roster_players(t["id"], t["abbrev"]))
        pk_sets_by_team[t["id"]] = _schedule_pk_sets(t["id"], season)
        team_abbrev[t["id"]] = t["abbrev"]

    # Map player id -> team id (for grouping + scope), and gather the id list.
    abbrev_to_tid = {t["abbrev"]: t["id"] for t in which}
    team_of: Dict[int, int] = {}
    meta_by_pid: Dict[int, dict] = {}
    for p in players:
        tid = abbrev_to_tid.get(p["team"])
        if tid is None:
            continue
        team_of[p["id"]] = tid
        meta_by_pid[p["id"]] = p
    pids = sorted(team_of.keys())
    if not pids:
        result = {"ok": True, "side": side, "group": group, "rows": [],
                  "meta": {"note": "No players found for this game."}}
        _CACHE[cache_key] = (now, result)
        return result

    sql, params, _ = _build_query(side, group, filters, pids, season, pk_sets_by_team, team_of)

    rows_raw: List[dict] = []
    try:
        with psycopg.connect(_db_url(), connect_timeout=8) as conn:
            conn.execute("SET statement_timeout = '15000'")
            with conn.cursor() as cur:
                cur.execute(sql, params)
                cols = [d[0] for d in cur.description]
                for r in cur.fetchall():
                    rows_raw.append(dict(zip(cols, r)))
    except Exception as e:
        print(f"[game_splits] query error: {e}")
        import traceback; traceback.print_exc()
        return {"ok": False, "error": "Database error", "side": side, "group": group, "rows": []}

    # Assemble output rows.
    out_rows: List[dict] = []
    if group == "player":
        for r in rows_raw:
            pid = int(r["pid"])
            meta = meta_by_pid.get(pid)
            if not meta or not (r.get("pa") or 0):
                continue
            counts = {k: (r.get(k) or 0) for k in
                      ("pa", "h", "d2", "d3", "hr", "ubb", "ibb", "hbp", "sf", "sh", "ci", "k", "swings", "whiffs")}
            counts["xwoba"] = float(r["xwoba"]) if r.get("xwoba") is not None else None
            line = _rates(counts)
            line.update({"playerId": pid, "name": meta["name"], "team": meta["team"]})
            out_rows.append(line)
        out_rows.sort(key=lambda x: (x.get("pa") or 0), reverse=True)
    else:  # team
        acc: Dict[int, dict] = {}
        for r in rows_raw:
            pid = int(r["pid"])
            tid = team_of.get(pid)
            if tid is None:
                continue
            acc.setdefault(tid, _empty_counts())
            _accumulate(acc[tid], r)
        for tid, c in acc.items():
            _finalize_xwoba(c)
            if not c["pa"]:
                continue
            line = _rates(c)
            line.update({"playerId": None, "name": team_abbrev.get(tid, str(tid)), "team": team_abbrev.get(tid, "")})
            out_rows.append(line)
        out_rows.sort(key=lambda x: x.get("name") or "")

    result = {
        "ok": True, "side": side, "group": group,
        "season": season,
        "teams": {"away": info["away"]["abbrev"], "home": info["home"]["abbrev"]},
        "rows": out_rows,
        "meta": {"players": len(pids), "filters": {k: _norm(filters.get(k)) for k in
                 ("team", "hand", "venue", "daynight", "range", "count", "bases")}},
    }
    _CACHE[cache_key] = (now, result)
    return result
