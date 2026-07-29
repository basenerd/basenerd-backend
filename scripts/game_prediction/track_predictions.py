#!/usr/bin/env python3
"""Grade logged live predictions against actual outcomes (autonomous monitor).

Joins prediction_log (pregame batter-vs-pitcher predicted PA distributions) to
the ACTUAL plate-appearance outcomes in statcast_pitches, computes real
production accuracy (log loss, calibration), logs a row to model_accuracy_log,
detects drift, does bounded isotonic recalibration, and emails a report.

Render-native: needs only the database (no big training parquet). Accumulates
signal over time as games complete.

Run: python scripts/game_prediction/track_predictions.py [--days 14] [--email]
"""
import os, sys, json, argparse
from datetime import date

import numpy as np
import joblib
from sklearn.metrics import log_loss
from sklearn.isotonic import IsotonicRegression

# model_monitor performs os.chdir(repo root) on import; import it first so all
# relative paths below resolve from the project root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
import model_monitor as mm  # noqa: E402  (ensure_table, email_report, reliability, CALIB, thresholds)
from eval_matchup_model import reliability, brier_multiclass  # noqa: E402
from db_utils import get_conn  # noqa: E402

CLASSES = ["1B", "2B", "3B", "BB", "HBP", "HR", "IBB", "K", "OUT"]
IDX = {c: i for i, c in enumerate(CLASSES)}

# statcast events -> model class (matches build_matchup_training_v2 OUTCOME_MAP)
OUTCOME_MAP = {
    "single": "1B", "double": "2B", "triple": "3B", "home_run": "HR",
    "walk": "BB", "hit_by_pitch": "HBP",
    "intent_walk": "IBB", "intentional_walk": "IBB",
    "strikeout": "K", "strikeout_double_play": "K",
}


def _to_class(ev):
    return OUTCOME_MAP.get(ev, "OUT")  # any other terminal event = OUT


def fetch_graded(cur, days):
    """Return (P matrix, y idx array, n_games) for completed, graded PAs."""
    cur.execute("""
        SELECT pl.probs, sp.events
        FROM prediction_log pl
        JOIN statcast_pitches sp
          ON sp.game_pk = pl.game_pk AND sp.batter = pl.batter AND sp.pitcher = pl.pitcher
         AND sp.events IS NOT NULL AND sp.events <> '' AND sp.events <> 'nan'
        WHERE pl.logged_at >= now() - (%s || ' days')::interval
    """, (str(days),))
    rows = cur.fetchall()
    P, y = [], []
    for probs, ev in rows:
        if isinstance(probs, str):
            probs = json.loads(probs)
        vec = [float(probs.get(c, 0.0)) for c in CLASSES]
        s = sum(vec)
        if s <= 0:
            continue
        vec = [v / s for v in vec]              # renormalize (weather-adjusted probs)
        P.append(vec)
        y.append(IDX[_to_class(ev)])
    return np.array(P), np.array(y)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=21)
    ap.add_argument("--email", action="store_true")
    ap.add_argument("--min-samples", type=int, default=200)
    a = ap.parse_args()

    labels = list(range(len(CLASSES)))
    with get_conn() as conn:
        with conn.cursor() as cur:
            mm.ensure_table(cur)
            P, y = fetch_graded(cur, a.days)
        conn.commit()

    if len(y) < a.min_samples:
        print(f"Only {len(y)} graded PAs (< {a.min_samples}); need more completed games. Exiting.")
        return

    ll = log_loss(y, P, labels=labels)
    base_rate = np.bincount(y, minlength=len(CLASSES)) / len(y)
    ll_base = log_loss(y, np.tile(base_rate, (len(y), 1)), labels=labels)
    br = brier_multiclass(y, P, len(CLASSES))
    acc = float((P.argmax(1) == y).mean())

    per_class, tot = {}, 0.0
    for i, c in enumerate(CLASSES):
        act = float((y == i).mean()); prd = float(P[:, i].mean())
        off = abs(prd - act) / act * 100 if act > 0 else 0
        ece, _ = reliability((y == i).astype(float), P[:, i])
        per_class[c] = {"actual": round(act, 4), "pred": round(prd, 4),
                        "pct_off": round(off, 1), "ece": round(ece, 4)}
        tot += off
    mean_off = tot / len(CLASSES)
    hit_idx = [IDX[c] for c in ["1B", "2B", "3B", "HR"]]
    ece_hit, _ = reliability(np.isin(y, hit_idx).astype(float), P[:, hit_idx].sum(1))

    # Drift vs prior live-grading run
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT log_loss, mean_cal_pct_off FROM model_accuracy_log
                           WHERE notes LIKE 'live-grading%%' ORDER BY id DESC LIMIT 1""")
            pr = cur.fetchone()
        conn.commit()
    drift = (pr is not None) and (ll > float(pr[0]) + mm.LOGLOSS_DRIFT or mean_off > float(pr[1]) + mm.CAL_DRIFT_PCT)

    # Bounded recalibration on real outcomes
    recalibrated, note = False, "not attempted"
    worst = max(v["ece"] for v in per_class.values())
    if drift and worst <= mm.RECAL_MAX_ECE:
        cals, before, after = {}, 0.0, 0.0
        for i, c in enumerate(CLASSES):
            yb = (y == i).astype(float)
            iso = IsotonicRegression(out_of_bounds="clip", y_min=0, y_max=1).fit(P[:, i], yb)
            e0, _ = reliability(yb, P[:, i]); e1, _ = reliability(yb, iso.predict(P[:, i]))
            before += e0; after += e1; cals[c] = iso
        if after < before - 1e-4:
            joblib.dump(cals, mm.CALIB); recalibrated = True
            note = f"recalibrated on live outcomes: ECE {before/len(CLASSES):.4f}->{after/len(CLASSES):.4f}"
        else:
            note = "recalibration not adopted (no improvement)"
    elif drift:
        note = f"drift but ECE {worst:.3f} too high to auto-recalibrate (review)"

    mv = json.load(open("models/matchup_model_v2_meta.json")).get("model_version", "v2")
    season = date.today().year
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO model_accuracy_log
                (run_date, model_version, eval_season, n_pas, log_loss, baseline_log_loss,
                 brier, accuracy, mean_cal_pct_off, ece_hit, per_class, drift_flag, recalibrated, notes)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (date.today(), mv, season, len(y), ll, ll_base, br, acc, mean_off, ece_hit,
                 json.dumps(per_class), drift, recalibrated, f"live-grading ({a.days}d)"))
        conn.commit()

    lines = [f"# Live Prediction Accuracy — {date.today()}",
             f"**Model:** {mv}  |  graded **{len(y):,}** real PAs from completed games (last {a.days}d)",
             f"- Log loss: **{ll:.4f}** (base {ll_base:.4f})  |  Brier {br:.4f}  |  acc {acc:.3f}",
             f"- Mean per-class calibration error: **{mean_off:.1f}%**  |  hit ECE {ece_hit:.4f}",
             f"- Drift: {'** YES **' if drift else 'no'}   |   {note}",
             "", "| class | actual | pred | %off | ece |", "|---|---|---|---|---|"]
    for c, v in per_class.items():
        lines.append(f"| {c} | {v['actual']:.4f} | {v['pred']:.4f} | {v['pct_off']:.1f}% | {v['ece']:.4f} |")
    report = "\n".join(lines)
    os.makedirs("reports/model_eval", exist_ok=True)
    open(f"reports/model_eval/live_accuracy_{date.today()}.md", "w", encoding="utf-8").write(report)
    print(report)
    if a.email or drift:
        mm.email_report(f"[Basenerd] Live model accuracy {date.today()}" + (" — DRIFT" if drift else ""), report)


if __name__ == "__main__":
    main()
