"""Single source of truth for "what season is it" and "are we in season".

Two separate questions that the codebase had been answering ad hoc:

  current_season()  the calendar-year season — what the schedule, live games
                    and game-day jobs mean by "this season".
  stats_season()    the most recent season with meaningful stats to show. In
                    January there is no current-year data yet, so leaderboards
                    and player pages should still default to last year.
  in_season()       whether game-day jobs have anything to do. Used to stop
                    the 15- and 20-minute crons from burning free-tier hours
                    all winter.

Set BASENERD_SEASON to pin the season (useful for backfills), or
BASENERD_FORCE_IN_SEASON=1 to make in_season() always true (useful when
testing game-day jobs out of season).
"""

from __future__ import annotations

import os
from datetime import date

# Conservative game-day window. Spring training reports start appearing in
# mid-February and the World Series ends in early November; padding both ends
# costs a couple of idle weeks and avoids ever missing a real game day.
SEASON_START = (2, 1)    # Feb 1
SEASON_END = (11, 15)    # Nov 15

# Before this month, the current calendar year has no stats worth showing.
_STATS_CUTOVER_MONTH = 3


def _today(today: date | None = None) -> date:
    return today or date.today()


def current_season(today: date | None = None) -> int:
    """The calendar-year season. Schedule- and game-oriented."""
    override = os.environ.get("BASENERD_SEASON")
    if override:
        try:
            return int(override)
        except ValueError:
            pass
    return _today(today).year


def stats_season(today: date | None = None) -> int:
    """Latest season with meaningful stats.

    Returns last year until March, so a January visitor sees a populated
    leaderboard instead of an empty one.
    """
    override = os.environ.get("BASENERD_SEASON")
    if override:
        try:
            return int(override)
        except ValueError:
            pass
    d = _today(today)
    return d.year if d.month >= _STATS_CUTOVER_MONTH else d.year - 1


def season_list(count: int = 26, today: date | None = None) -> list[int]:
    """Descending seasons for a dropdown, newest first."""
    latest = stats_season(today)
    return list(range(latest, latest - count, -1))


def in_season(today: date | None = None) -> bool:
    """Whether game-day jobs should run at all."""
    if os.environ.get("BASENERD_FORCE_IN_SEASON") == "1":
        return True
    d = _today(today)
    return SEASON_START <= (d.month, d.day) <= SEASON_END


def require_in_season(job_name: str, today: date | None = None) -> bool:
    """Log-and-report helper for a cron entry point.

    Returns True when the job should proceed. Call it first thing in main()
    and exit 0 when it returns False — a skipped run is a success, not a
    failure, so it should not page anyone.
    """
    if in_season(today):
        return True
    d = _today(today)
    print(f"[{job_name}] offseason ({d.isoformat()}); "
          f"game-day window is {SEASON_START[0]:02d}-{SEASON_START[1]:02d} "
          f"to {SEASON_END[0]:02d}-{SEASON_END[1]:02d}. Skipping.")
    return False
