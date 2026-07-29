#!/usr/bin/env python3
"""Evaluate whether recent form is properly weighted in live matchup projections.

Part A: sensitivity — hold a matchup fixed, vary ONLY batter recent-form input,
        measure how much the projection moves.
Part B: real hot/cold hitters — run the live model with their actual recent form.
"""
import os, sys, json, urllib.request
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import psycopg
import services.matchup_predict as mp
import services.recent_form as rf

SEASON = 2026
NEUTRAL_PITCHER = 0  # non-existent id -> league-average pitcher fallback
ASOF = "2026-07-09"

mp._load()
_orig_bat = rf.get_batter_recent_form
_orig_pit = rf.get_pitcher_recent_form
conn = psycopg.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()


def names(ids):
    try:
        url = f"https://statsapi.mlb.com/api/v1/people?personIds={','.join(map(str,ids))}"
        with urllib.request.urlopen(url, timeout=20) as r:
            d = json.load(r)
        return {p["id"]: p["fullName"] for p in d.get("people", [])}
    except Exception:
        return {}


def summ(batter, pitcher=NEUTRAL_PITCHER):
    r = mp.predict_matchup(batter_id=batter, pitcher_id=pitcher, season=SEASON,
                           venue=None, stand="R", p_throws="R")
    return r["summary"]


# Pick a real, high-PA batter as the fixed base for the sensitivity test
cur.execute("""SELECT batter, COUNT(*) FROM statcast_pitches
    WHERE game_year=2026 AND game_type='R' AND events IS NOT NULL AND events!=''
    GROUP BY batter ORDER BY COUNT(*) DESC LIMIT 1""")
base_batter = cur.fetchone()[0]
bname = names([base_batter]).get(base_batter, base_batter)

NEUTRAL_PIT = {"p_r14_k_pct": .223, "p_r14_bb_pct": .083, "p_r14_xwoba": .315,
               "p_r14_whiff_rate": .245, "p_r14_chase_rate": .295, "_pa": 100}


def bat_r14(xwoba, kpct):
    return {"bat_r14_k_pct": kpct, "bat_r14_bb_pct": .083, "bat_r14_xwoba": xwoba,
            "bat_r14_barrel_rate": .07, "bat_r14_whiff_rate": .245,
            "bat_r14_chase_rate": .295, "_pa": 100}  # _pa=100 -> no shrinkage


def patched(xwoba, kpct):
    rf.get_batter_recent_form = lambda bid, **k: bat_r14(xwoba, kpct)
    rf.get_pitcher_recent_form = lambda pid, **k: dict(NEUTRAL_PIT)
    return summ(base_batter)


print(f"\n=== PART A: SENSITIVITY (base batter {bname}, neutral pitcher) ===")
print("Vary ONLY batter 14-day xwOBA (recent K% held at .22, no shrinkage):")
print(f"{'r14_xwOBA':>10} | {'proj hit%':>9} {'proj K%':>8} {'proj HR%':>8} {'proj OBP':>8} {'xSLG':>6}")
for x in [.250, .290, .315, .350, .400]:
    s = patched(x, .22)
    print(f"{x:>10.3f} | {s['hit_pct']*100:>8.1f} {s['k_pct']*100:>7.1f} "
          f"{s['hr_pct']*100:>7.1f} {s['obp']:>8.3f} {s.get('xslg',0):>6.3f}")

print("\nVary ONLY batter 14-day K% (recent xwOBA held .315, no shrinkage):")
print(f"{'r14_K%':>10} | {'proj K%':>8} {'proj hit%':>9} {'proj OBP':>8}")
for kk in [.12, .18, .22, .28, .36]:
    s = patched(.315, kk)
    print(f"{kk*100:>9.0f}% | {s['k_pct']*100:>7.1f} {s['hit_pct']*100:>8.1f} {s['obp']:>8.3f}")

# restore real recent-form functions for Part B
rf.get_batter_recent_form = _orig_bat
rf.get_pitcher_recent_form = _orig_pit

print("\n\n=== PART B: REAL HOT / COLD HITTERS (actual recent form, neutral pitcher) ===")
q = """
WITH season AS (
  SELECT batter,
         AVG(CASE WHEN estimated_woba_using_speedangle BETWEEN -1 AND 5 THEN estimated_woba_using_speedangle END) sxw,
         SUM(CASE WHEN events IS NOT NULL AND events!='' THEN 1 ELSE 0 END) spa
  FROM statcast_pitches WHERE game_year=2026 AND game_type='R' GROUP BY batter),
recent AS (
  SELECT batter,
         AVG(CASE WHEN estimated_woba_using_speedangle BETWEEN -1 AND 5 THEN estimated_woba_using_speedangle END) rxw,
         SUM(CASE WHEN events IS NOT NULL AND events!='' THEN 1 ELSE 0 END) rpa
  FROM statcast_pitches
  WHERE game_year=2026 AND game_type='R' AND game_date >= %s::date - INTERVAL '14 days'
  GROUP BY batter)
SELECT s.batter, s.sxw, s.spa, r.rxw, r.rpa, (r.rxw - s.sxw) diff
FROM season s JOIN recent r USING(batter)
WHERE s.spa>=250 AND r.rpa>=35
ORDER BY diff {order} LIMIT 4
"""
rows = []
for label, order in [("HOT", "DESC"), ("COLD", "ASC")]:
    cur.execute(q.format(order=order), (ASOF,))
    for batter, sxw, spa, rxw, rpa, diff in cur.fetchall():
        rows.append((label, batter, float(sxw), int(spa), float(rxw), int(rpa)))
nm = names([r[1] for r in rows])
print(f"{'':4} {'player':22} {'seasXWOBA':>9} {'rec14XWOBA':>10} {'recPA':>6} | {'projHIT%':>8} {'projK%':>7} {'projOBP':>8}")
for label, batter, sxw, spa, rxw, rpa in rows:
    s = summ(batter)
    print(f"{label:4} {str(nm.get(batter,batter)):22} {sxw:>9.3f} {rxw:>10.3f} {rpa:>6} | "
          f"{s['hit_pct']*100:>7.1f} {s['k_pct']*100:>6.1f} {s['obp']:>8.3f}")
