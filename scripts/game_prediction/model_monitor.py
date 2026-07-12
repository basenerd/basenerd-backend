#!/usr/bin/env python3
"""Model accuracy monitoring + bounded auto-recalibration + reporting.

Weekly upkeep job for the matchup (PA-outcome) model:
  1. Evaluate the production model on the most recent completed season slice.
  2. Log metrics to the model_accuracy_log table (trend over time).
  3. Detect drift vs the model's ship-time reference and the prior run.
  4. If calibration drifts, refit per-class isotonic calibrators on recent data
     and adopt them ONLY if they improve held-out calibration (bounded, safe).
     Full retrains are never automatic — they are flagged for review.
  5. Generate a markdown report and email it.

Run: python scripts/game_prediction/model_monitor.py [--email] [--no-recalibrate]
"""
import os, sys, json, argparse, smtplib
from datetime import date, timezone, datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import numpy as np, pandas as pd, joblib
from sklearn.metrics import log_loss
from sklearn.isotonic import IsotonicRegression

os.chdir(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join("scripts", "game_prediction"))
from eval_matchup_model import prep, brier_multiclass, reliability, FILL_DEFAULTS  # noqa
from db_utils import get_conn  # noqa

DATA = "data/matchup_train_v2.parquet"
MODEL = "models/matchup_model_v2.joblib"
META = "models/matchup_model_v2_meta.json"
CALIB = "models/matchup_model_v2_calibrators.joblib"
REPORT_DIR = "reports/model_eval"
RECIPIENT = os.environ.get("MODEL_REPORT_TO", os.environ.get("GMAIL_USER", ""))

# Drift thresholds (alert if exceeded)
LOGLOSS_DRIFT = 0.05      # test log loss worse than reference by this
CAL_DRIFT_PCT = 4.0       # mean per-class |%off| worse than reference by this
RECAL_MAX_ECE = 0.06      # don't auto-recalibrate if a class is wildly off (data problem)


def ensure_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS model_accuracy_log (
            id SERIAL PRIMARY KEY,
            run_ts TIMESTAMPTZ DEFAULT now(),
            run_date DATE,
            model_version TEXT,
            eval_season INTEGER,
            n_pas INTEGER,
            log_loss DOUBLE PRECISION,
            baseline_log_loss DOUBLE PRECISION,
            brier DOUBLE PRECISION,
            accuracy DOUBLE PRECISION,
            mean_cal_pct_off DOUBLE PRECISION,
            ece_hit DOUBLE PRECISION,
            per_class JSONB,
            drift_flag BOOLEAN,
            recalibrated BOOLEAN,
            notes TEXT
        )
    """)


def evaluate(model, meta, df):
    numeric = meta["numeric_features"]; categorical = meta["categorical_features"]
    classes = meta["classes"]; feats = numeric + categorical
    labels = list(range(len(classes)))
    cls_to_idx = {c: i for i, c in enumerate(classes)}
    d = prep(df, numeric, categorical)
    d["y"] = d["outcome"].map(cls_to_idx)
    d = d[d["y"].notna()].copy(); d["y"] = d["y"].astype(int)
    X, y = d[feats], d["y"].values
    p = model.predict_proba(X)
    ll = log_loss(y, p, labels=labels)
    base_rate = np.bincount(y, minlength=len(classes)) / len(y)
    ll_base = log_loss(y, np.tile(base_rate, (len(y), 1)), labels=labels)
    br = brier_multiclass(y, p, len(classes))
    acc = float((p.argmax(1) == y).mean())
    per_class, tot = {}, 0.0
    for i, c in enumerate(classes):
        a = float((y == i).mean()); pr = float(p[:, i].mean())
        off = abs(pr - a) / a * 100 if a > 0 else 0
        ece, _ = reliability((y == i).astype(float), p[:, i])
        per_class[c] = {"actual": round(a, 4), "pred": round(pr, 4),
                        "pct_off": round(off, 1), "ece": round(ece, 4)}
        tot += off
    hit_idx = [cls_to_idx[c] for c in ["1B", "2B", "3B", "HR"]]
    ece_hit, _ = reliability(np.isin(y, hit_idx).astype(float), p[:, hit_idx].sum(1))
    return {"n": len(y), "log_loss": ll, "baseline_log_loss": ll_base, "brier": br,
            "accuracy": acc, "mean_cal_pct_off": tot / len(classes), "ece_hit": ece_hit,
            "per_class": per_class, "_X": X, "_y": y, "_p": p, "_classes": classes}


def try_recalibrate(res, meta):
    """Refit per-class isotonic calibrators; adopt only if they improve ECE."""
    classes = res["_classes"]; y = res["_y"]; p = res["_p"]
    worst = max(v["ece"] for v in res["per_class"].values())
    if worst > RECAL_MAX_ECE:
        return False, f"skipped recalibration (ECE {worst:.3f} > {RECAL_MAX_ECE}; likely data issue, not calibration)"
    cals, before, after = {}, 0.0, 0.0
    for i, c in enumerate(classes):
        yb = (y == i).astype(float)
        iso = IsotonicRegression(out_of_bounds="clip", y_min=0, y_max=1)
        iso.fit(p[:, i], yb)
        pc = iso.predict(p[:, i])
        e0, _ = reliability(yb, p[:, i]); e1, _ = reliability(yb, pc)
        before += e0; after += e1; cals[c] = iso
    if after < before - 1e-4:
        joblib.dump(cals, CALIB)
        return True, f"recalibrated: mean per-class ECE {before/len(classes):.4f} -> {after/len(classes):.4f} (calibrators saved)"
    return False, f"recalibration not adopted (no ECE improvement: {before/len(classes):.4f} -> {after/len(classes):.4f})"


def prev_run(cur):
    cur.execute("SELECT log_loss, mean_cal_pct_off FROM model_accuracy_log ORDER BY id DESC LIMIT 1")
    return cur.fetchone()


def build_report(res, meta, drift, recal_note, ref):
    lines = [f"# Matchup Model Accuracy Report — {date.today()}",
             f"**Model:** {meta.get('model_version')}  |  **Eval season:** {res['eval_season']}  |  **PAs:** {res['n']:,}",
             "",
             "## Headline metrics",
             f"- Log loss: **{res['log_loss']:.4f}** (baseline {res['baseline_log_loss']:.4f}, ship-time ref {ref:.4f})",
             f"- Brier: {res['brier']:.4f}  |  Argmax accuracy: {res['accuracy']:.3f}",
             f"- Mean per-class calibration error: **{res['mean_cal_pct_off']:.1f}%**  |  hit ECE: {res['ece_hit']:.4f}",
             f"- Drift: {'** YES **' if drift else 'no'}   |   Recalibration: {recal_note}",
             "",
             "## Per-class calibration (predicted vs actual rate)",
             "| class | actual | pred | %off | ece |", "|---|---|---|---|---|"]
    for c, v in res["per_class"].items():
        flag = " (!)" if v["pct_off"] > 15 else ""
        lines.append(f"| {c} | {v['actual']:.4f} | {v['pred']:.4f} | {v['pct_off']:.1f}%{flag} | {v['ece']:.4f} |")
    return "\n".join(lines)


def email_report(subject, body_md):
    user = os.environ.get("GMAIL_USER", ""); pw = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not user or not pw or not RECIPIENT:
        print("  [email skipped — GMAIL_USER/APP_PASSWORD/recipient not set]")
        return
    msg = MIMEMultipart(); msg["From"] = user; msg["To"] = RECIPIENT; msg["Subject"] = subject
    msg.attach(MIMEText(body_md, "plain"))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
        s.login(user, pw); s.sendmail(user, [RECIPIENT], msg.as_string())
    print(f"  [emailed report to {RECIPIENT}]")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", action="store_true")
    ap.add_argument("--no-recalibrate", action="store_true")
    a = ap.parse_args()

    meta = json.load(open(META)); model = joblib.load(MODEL)
    df = pd.read_parquet(DATA)
    eval_season = int(df["season"].max())
    res = evaluate(model, meta, df[df["season"] == eval_season])
    res["eval_season"] = eval_season
    ref = float(meta.get("test_log_loss", res["log_loss"]))

    with get_conn() as conn:
        with conn.cursor() as cur:
            ensure_table(cur); pr = prev_run(cur)
        conn.commit()

    drift = (res["log_loss"] > ref + LOGLOSS_DRIFT) or \
            (pr is not None and res["mean_cal_pct_off"] > float(pr[1]) + CAL_DRIFT_PCT)

    recalibrated, recal_note = False, "not attempted"
    if drift and not a.no_recalibrate:
        recalibrated, recal_note = try_recalibrate(res, meta)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO model_accuracy_log
                (run_date, model_version, eval_season, n_pas, log_loss, baseline_log_loss,
                 brier, accuracy, mean_cal_pct_off, ece_hit, per_class, drift_flag, recalibrated, notes)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (date.today(), meta.get("model_version"), eval_season, res["n"],
                 res["log_loss"], res["baseline_log_loss"], res["brier"], res["accuracy"],
                 res["mean_cal_pct_off"], res["ece_hit"], json.dumps(res["per_class"]),
                 drift, recalibrated, recal_note))
        conn.commit()

    report = build_report(res, meta, drift, recal_note, ref)
    os.makedirs(REPORT_DIR, exist_ok=True)
    path = os.path.join(REPORT_DIR, f"accuracy_{date.today()}.md")
    open(path, "w", encoding="utf-8").write(report)
    print(report)
    print(f"\n[logged to model_accuracy_log | report {path}]")
    if a.email or drift:
        email_report(f"[Basenerd] Matchup model report {date.today()}" + (" — DRIFT" if drift else ""), report)


if __name__ == "__main__":
    main()
