"""TEMP: seed prediction_log with real predictions for recent completed matchups
to validate the grading pipeline end-to-end."""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from collections import defaultdict
import psycopg
import services.matchup_predict as m
from services.prediction_log import log_pregame

m._load()
cur = psycopg.connect(os.environ["DATABASE_URL"]).cursor()
cur.execute("""
    SELECT game_pk, pitcher, batter, MAX(stand), MAX(p_throws)
    FROM statcast_pitches
    WHERE game_year=2026 AND game_type='R'
      AND game_date >= current_date - 12 AND events IS NOT NULL AND events <> 'nan'
    GROUP BY game_pk, pitcher, batter LIMIT 1500
""")
rows = cur.fetchall()
groups = defaultdict(list)
for gp, pit, bat, stand, pthrows in rows:
    r = m.predict_matchup(batter_id=bat, pitcher_id=pit, season=2026, venue=None,
                          stand=stand or "R", p_throws=pthrows or "R")
    if r.get("ok"):
        groups[(gp, pit)].append({"id": bat, "probs": r["probs"]})
for (gp, pit), batters in groups.items():
    log_pregame(gp, 2026, pit, batters)
print(f"seeded {len(rows)} matchups across {len(groups)} game/pitcher groups")
