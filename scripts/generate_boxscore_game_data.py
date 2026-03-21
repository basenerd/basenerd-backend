#!/usr/bin/env python3
"""
Generate player pool for the Career Boxscore Guessing Game.

Finds MLB hitters from 2000 onwards with 2000+ career plate appearances
and saves their year-by-year stats to data/boxscore_game_pool.json.

Usage:
    python scripts/generate_boxscore_game_data.py

Takes ~3-5 minutes due to MLB API rate limiting.
"""

import json
import os
import sys
import time

import requests

BASE = "https://statsapi.mlb.com/api/v1"
MIN_PA = 2000
MIN_SEASONS = 5
START_YEAR = 2000
END_YEAR = 2026
BATCH_SIZE = 50


def fetch_season_player_ids(season: int) -> list:
    """Get all MLB player IDs for a season."""
    url = f"{BASE}/sports/1/players"
    r = requests.get(url, params={"season": season}, timeout=30)
    r.raise_for_status()
    return [p["id"] for p in r.json().get("people", []) if p.get("id")]


def fetch_players_career_pa(pids: list) -> dict:
    """Batch-fetch career hitting PAs for a list of player IDs."""
    url = f"{BASE}/people"
    params = {
        "personIds": ",".join(str(p) for p in pids),
        "hydrate": "stats(group=[hitting],type=[career])",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()

    results = {}
    for p in r.json().get("people", []) or []:
        pid = p.get("id")
        pa = 0
        for grp in p.get("stats", []) or []:
            gname = ((grp.get("group") or {}).get("displayName") or "").lower()
            tname = ((grp.get("type") or {}).get("displayName") or "").lower()
            if "hitting" in gname and "career" in tname:
                for s in grp.get("splits", []) or []:
                    pa = max(pa, int((s.get("stat") or {}).get("plateAppearances", 0)))
        results[pid] = pa
    return results


def fetch_player_full(pid: int) -> dict:
    """Fetch player info + yearByYear + career hitting stats."""
    url = f"{BASE}/people/{pid}"
    params = {"hydrate": "stats(group=[hitting],type=[yearByYear,career])"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    people = r.json().get("people", []) or []
    return people[0] if people else None


def build_entry(player: dict):
    """Build a pool entry from a full player dict."""
    name = player.get("fullName", "")
    pos = (player.get("primaryPosition") or {}).get("abbreviation", "")
    bats = (player.get("batSide") or {}).get("code", "")
    birth_str = (player.get("birthDate") or "")[:4]
    birth_year = int(birth_str) if birth_str else None

    years = []
    career = {}

    for grp in player.get("stats", []) or []:
        gname = ((grp.get("group") or {}).get("displayName") or "").lower()
        tname = ((grp.get("type") or {}).get("displayName") or "").lower()
        if "hitting" not in gname:
            continue

        if "yearbyyear" in tname.replace(" ", "").lower():
            by_year = {}
            for s in grp.get("splits", []) or []:
                sport = (s.get("sport") or {}).get("name", "")
                if sport and "Major League Baseball" not in sport:
                    continue
                season = s.get("season", "")
                team_abbr = (s.get("team") or {}).get("abbreviation", "")
                by_year.setdefault(season, []).append({
                    "team": team_abbr,
                    "stat": s.get("stat", {}),
                })

            for season in sorted(by_year.keys()):
                rows = by_year[season]
                team_rows = [r for r in rows if r["team"]]
                no_team = [r for r in rows if not r["team"]]

                if no_team:
                    stat = no_team[0]["stat"]
                    team_str = "/".join(r["team"] for r in team_rows) or "TOT"
                elif len(team_rows) == 1:
                    stat = team_rows[0]["stat"]
                    team_str = team_rows[0]["team"]
                else:
                    # Multiple teams, no total row — use first (rare)
                    stat = team_rows[0]["stat"]
                    team_str = "/".join(r["team"] for r in team_rows)

                age = ""
                if birth_year and season:
                    try:
                        age = int(season) - birth_year
                    except ValueError:
                        pass

                years.append({
                    "year": season,
                    "team": team_str,
                    "age": age,
                    "stat": stat,
                })

        elif "career" in tname:
            for s in grp.get("splits", []) or []:
                career = s.get("stat", {})

    if not years:
        return None

    return {
        "id": player["id"],
        "name": name,
        "pos": pos,
        "bats": bats,
        "years": years,
        "career": career,
    }


def main():
    print("=== Boxscore Game Data Generator ===\n")

    # Step 1: Scan seasons for player IDs
    print(f"Step 1: Scanning seasons {START_YEAR}-{END_YEAR} for player IDs...")
    season_counts = {}
    for season in range(START_YEAR, END_YEAR + 1):
        sys.stdout.write(f"\r  Season {season}...")
        sys.stdout.flush()
        try:
            ids = fetch_season_player_ids(season)
            for pid in ids:
                season_counts[pid] = season_counts.get(pid, 0) + 1
        except Exception as e:
            sys.stdout.write(f" error: {e}")
        time.sleep(0.3)
    print(f"\r  Found {len(season_counts)} unique players across all seasons    ")

    # Step 2: Filter to players with enough seasons
    candidates = sorted(pid for pid, count in season_counts.items() if count >= MIN_SEASONS)
    print(f"\nStep 2: {len(candidates)} players with {MIN_SEASONS}+ seasons")

    # Step 3: Batch check career PAs
    print(f"\nStep 3: Checking career plate appearances (batch size={BATCH_SIZE})...")
    qualifying = []
    total_batches = (len(candidates) - 1) // BATCH_SIZE + 1
    for i in range(0, len(candidates), BATCH_SIZE):
        batch = candidates[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        sys.stdout.write(f"\r  Batch {batch_num}/{total_batches}...")
        sys.stdout.flush()
        try:
            pa_map = fetch_players_career_pa(batch)
            for pid, pa in pa_map.items():
                if pa >= MIN_PA:
                    qualifying.append(pid)
        except Exception as e:
            sys.stdout.write(f" error: {e}")
        time.sleep(0.3)
    print(f"\r  {len(qualifying)} players with {MIN_PA}+ career PAs          ")

    # Step 4: Fetch full year-by-year data
    print(f"\nStep 4: Fetching year-by-year stats for {len(qualifying)} players...")
    pool = []
    for i, pid in enumerate(qualifying):
        sys.stdout.write(f"\r  {i + 1}/{len(qualifying)}...")
        sys.stdout.flush()
        try:
            player = fetch_player_full(pid)
            if not player:
                continue
            entry = build_entry(player)
            if entry and entry["years"]:
                pool.append(entry)
        except Exception:
            pass
        time.sleep(0.2)
    print(f"\r  Built {len(pool)} complete player entries          ")

    # Save
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "boxscore_game_pool.json")
    with open(out_path, "w") as f:
        json.dump(pool, f)
    size_mb = os.path.getsize(out_path) / (1024 * 1024)
    print(f"\n=== Done! Saved {len(pool)} players to {out_path} ({size_mb:.1f} MB) ===")


if __name__ == "__main__":
    main()
