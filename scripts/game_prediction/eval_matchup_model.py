#!/usr/bin/env python3
"""Reusable accuracy evaluation for the matchup (PA-outcome) model.

Computes log loss, Brier, per-class calibration (calibration-in-the-large +
reliability/ECE), accuracy, generalization gap, feature importance, and
recent-form redundancy. Used for the Phase-1 baseline and the Phase-4 reports.

Usage:
  python scripts/game_prediction/eval_matchup_model.py \
      --model models/matchup_model_v2.joblib \
      --meta  models/matchup_model_v2_meta.json \
      --data  data/matchup_train_v2.parquet
"""
import os, sys, json, argparse
import numpy as np
import pandas as pd
import joblib
from sklearn.metrics import log_loss

# Fill defaults must match training (train_v2_curated.py)
FILL_DEFAULTS = {
    "bat_avg_ev": 88.5, "bat_avg_la": 12.0, "bat_barrel_rate": 0.068,
    "bat_xwoba": 0.315, "bat_hard_hit_rate": 0.35, "bat_sweet_spot_rate": 0.33,
    "bat_iso": 0.155, "bat_babip": 0.295, "bat_hr_per_fb": 0.12,
    "bat_plat_avg_ev": 88.5, "bat_plat_barrel_rate": 0.068, "bat_plat_xwoba": 0.315,
    "p_avg_stuff_plus": 100.0, "p_avg_control_plus": 100.0, "p_xwoba": 0.315,
    "p_pitch1_stuff": 100.0, "p_pitch2_stuff": 100.0, "p_pitch3_stuff": 100.0,
    "bvpt_xwoba_fastball": 0.315, "bvpt_xwoba_breaking": 0.315,
    "bvpt_xwoba_offspeed": 0.315, "bvpt_w_xwoba": 0.315,
    "matchup_k_advantage": 0, "interact_k_whiff": 0.055,
    "platoon_k_split": 0, "p_delta_whiff": 0, "p_delta_chase": 0, "p_delta_xwoba": 0,
    "wl_is_starter": 1, "bat_season_pa": 300,
    "score_diff": 0, "runners_on": 0, "base_out_state": 0,
}


def prep(df, numeric, categorical):
    df = df.copy()
    for c in numeric:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float32")
    for c in categorical:
        if c in df.columns:
            df[c] = df[c].astype("category").cat.codes.astype("float32")
    for c, d in FILL_DEFAULTS.items():
        if c in df.columns:
            df[c] = df[c].fillna(d)
    for c in numeric + categorical:
        if c in df.columns:
            df[c] = df[c].fillna(0.0)
    return df


def brier_multiclass(y_idx, probs, n_classes):
    oh = np.eye(n_classes)[y_idx]
    return float(np.mean(np.sum((probs - oh) ** 2, axis=1)))


def reliability(y_true_binary, p, bins=10):
    """Expected calibration error for one class (binary)."""
    edges = np.linspace(0, 1, bins + 1)
    ece, rows = 0.0, []
    n = len(p)
    for i in range(bins):
        lo, hi = edges[i], edges[i + 1]
        m = (p >= lo) & (p < hi) if i < bins - 1 else (p >= lo) & (p <= hi)
        if m.sum() == 0:
            continue
        conf = p[m].mean()
        acc = y_true_binary[m].mean()
        ece += (m.sum() / n) * abs(conf - acc)
        rows.append((lo, hi, int(m.sum()), conf, acc))
    return ece, rows


def evaluate(model_path, meta_path, data_path, report=None):
    meta = json.load(open(meta_path))
    numeric = meta["numeric_features"]
    categorical = meta["categorical_features"]
    classes = meta["classes"]
    feats = numeric + categorical
    model = joblib.load(model_path)

    df = pd.read_parquet(data_path)
    df = prep(df, numeric, categorical)
    cls_to_idx = {c: i for i, c in enumerate(classes)}
    df["y"] = df["outcome"].map(cls_to_idx)
    df = df[df["y"].notna()].copy()
    df["y"] = df["y"].astype(int)

    max_season = int(df["season"].max())
    tr = df[df["season"] <= max_season - 1]
    te = df[df["season"] == max_season]

    out = []

    def w(line=""):
        out.append(line)
        print(line)

    w("=" * 70)
    w(f"MATCHUP MODEL EVALUATION — {meta.get('model_version','?')}")
    w(f"model={os.path.basename(model_path)}  data={os.path.basename(data_path)}")
    w("=" * 70)
    w(f"train seasons <= {max_season-1}: {len(tr):,} PAs | test {max_season}: {len(te):,} PAs")

    Xtr, ytr = tr[feats], tr["y"].values
    Xte, yte = te[feats], te["y"].values
    ptr = model.predict_proba(Xtr)
    pte = model.predict_proba(Xte)

    ll_tr = log_loss(ytr, ptr, labels=list(range(len(classes))))
    ll_te = log_loss(yte, pte, labels=list(range(len(classes))))
    br_te = brier_multiclass(yte, pte, len(classes))
    acc_te = float((pte.argmax(1) == yte).mean())
    base_rate = np.bincount(ytr, minlength=len(classes)) / len(ytr)
    ll_base = log_loss(yte, np.tile(base_rate, (len(yte), 1)), labels=list(range(len(classes))))

    w("\n--- OVERALL ---")
    w(f"  log loss  train={ll_tr:.4f}  test={ll_te:.4f}  gap={ll_te-ll_tr:+.4f}  (overfit if large)")
    w(f"  baseline (class-rate) test log loss={ll_base:.4f}  -> model improvement={ll_base-ll_te:+.4f}")
    w(f"  test Brier={br_te:.4f}   argmax accuracy={acc_te:.4f}")

    w("\n--- CALIBRATION-IN-THE-LARGE (test season, realistic outcomes) ---")
    w(f"  {'class':5} {'actual':>8} {'pred':>8} {'diff':>8} {'%off':>7} {'ECE':>7}")
    cal = {}
    for i, c in enumerate(classes):
        actual = float((yte == i).mean())
        pred = float(pte[:, i].mean())
        pctoff = abs(pred - actual) / actual * 100 if actual > 0 else 0
        ece, _ = reliability((yte == i).astype(float), pte[:, i])
        cal[c] = {"actual": actual, "pred": pred, "ece": ece}
        flag = " ***" if pctoff > 15 else ""
        w(f"  {c:5} {actual:>8.4f} {pred:>8.4f} {pred-actual:>+8.4f} {pctoff:>6.1f}% {ece:>7.4f}{flag}")

    # Grouped "hit" reliability (any hit)
    hit_idx = [cls_to_idx[c] for c in ["1B", "2B", "3B", "HR"] if c in cls_to_idx]
    p_hit = pte[:, hit_idx].sum(1)
    y_hit = np.isin(yte, hit_idx).astype(float)
    ece_hit, rows_hit = reliability(y_hit, p_hit)
    w(f"\n  HIT (any) reliability curve (ECE={ece_hit:.4f}):")
    w(f"  {'bin':>12} {'n':>7} {'pred':>7} {'actual':>7}")
    for lo, hi, n, conf, acc in rows_hit:
        w(f"  {lo:.2f}-{hi:.2f} {n:>10} {conf:>7.3f} {acc:>7.3f}")

    w("\n--- RECENT-FORM REDUNDANCY (corr with season baseline) ---")
    pairs = [("bat_r14_xwoba", "bat_xwoba"), ("bat_r14_k_pct", "bat_k_pct"),
             ("bat_r14_whiff_rate", "bat_whiff_rate"), ("bat_r14_chase_rate", "bat_chase_rate")]
    for a, b in pairs:
        if a in df.columns and b in df.columns:
            c = df[[a, b]].corr().iloc[0, 1]
            w(f"  corr({a}, {b}) = {c:.3f}")

    w("\n--- FEATURE IMPORTANCE (gain) ---")
    imp = pd.DataFrame({"feature": feats, "gain": model.feature_importances_}).sort_values("gain", ascending=False).reset_index(drop=True)
    cum = imp["gain"].cumsum()
    n80 = int((cum <= 0.80).sum()) + 1
    dead = imp[imp["gain"] < 0.002]
    w(f"  top 12: " + ", ".join(f"{r.feature}({r.gain:.3f})" for r in imp.head(12).itertuples()))
    w(f"  {n80} of {len(feats)} features carry 80% of gain")
    w(f"  {len(dead)} features with gain<0.002 (near-dead): " + ", ".join(dead['feature'].head(20)))

    # Dead/constant feature check (0 variance in training)
    const = [f for f in numeric if f in tr.columns and tr[f].nunique() <= 1]
    w(f"  CONSTANT-in-training features (learned nothing): {const}")

    if report:
        os.makedirs(os.path.dirname(report), exist_ok=True)
        with open(report, "w", encoding="utf-8") as f:
            f.write("\n".join(out))
        print(f"\n[report written to {report}]")

    return {"ll_train": ll_tr, "ll_test": ll_te, "gap": ll_te - ll_tr,
            "brier": br_te, "acc": acc_te, "ll_baseline": ll_base,
            "calibration": cal, "ece_hit": ece_hit}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="models/matchup_model_v2.joblib")
    ap.add_argument("--meta", default="models/matchup_model_v2_meta.json")
    ap.add_argument("--data", default="data/matchup_train_v2.parquet")
    ap.add_argument("--report", default=None)
    a = ap.parse_args()
    evaluate(a.model, a.meta, a.data, a.report)
