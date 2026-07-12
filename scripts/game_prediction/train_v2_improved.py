#!/usr/bin/env python3
"""Train an improved matchup model (v2.3 candidate).

Improvements over v2.2_curated:
  - Recent-form DELTA features (bat_r14_k_delta, bat_r14_xwoba_delta, p_r14_k_delta)
    isolate the hot/cold signal that raw r14 levels couldn't (redundant w/ season).
  - Targeted engineered features for known calibration gaps (BB over, K under).
  - Stuff+/Control+ become live once trained on data with real pitch_model_scores.
  - TEMPORAL validation (train<=2024 / val 2025) for honest model selection,
    small regularization search, then final fit on all pre-test seasons.

Saves to a *candidate* path and prints a full baseline comparison + ship gate.
"""
import os, sys, json, time, itertools
import numpy as np, pandas as pd, joblib
from sklearn.metrics import log_loss
import xgboost as xgb

os.chdir(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join("scripts", "game_prediction"))
from eval_matchup_model import FILL_DEFAULTS, prep, brier_multiclass, reliability  # noqa

DATA = "data/matchup_train_v2.parquet"
BASELINE_MODEL = "models/matchup_model_v2.joblib"
BASELINE_META = "models/matchup_model_v2_meta.json"
CAND_MODEL = "models/matchup_model_v2_candidate.joblib"
CAND_META = "models/matchup_model_v2_candidate_meta.json"
REPORT = "reports/model_eval/retrain_v2.3_compare.txt"

# Base curated feature set (v2.2) ...
BASE_NUMERIC = [
    "bat_k_pct","bat_bb_pct","bat_whiff_rate","bat_chase_rate","bat_zone_swing_rate",
    "bat_zone_contact_rate","bat_avg_ev","bat_avg_la","bat_barrel_rate","bat_hard_hit_rate",
    "bat_sweet_spot_rate","bat_gb_rate","bat_fb_rate","bat_hr_per_fb","bat_iso","bat_babip","bat_xwoba",
    "bat_plat_k_pct","bat_plat_bb_pct","bat_plat_whiff_rate","bat_plat_chase_rate","bat_plat_avg_ev",
    "bat_plat_barrel_rate","bat_plat_xwoba",
    "bvpt_whiff_rate_fastball","bvpt_chase_rate_fastball","bvpt_zone_contact_rate_fastball",
    "bvpt_hard_hit_rate_fastball","bvpt_xwoba_fastball",
    "bvpt_whiff_rate_breaking","bvpt_chase_rate_breaking","bvpt_zone_contact_rate_breaking",
    "bvpt_hard_hit_rate_breaking","bvpt_xwoba_breaking",
    "bvpt_whiff_rate_offspeed","bvpt_chase_rate_offspeed","bvpt_zone_contact_rate_offspeed",
    "bvpt_hard_hit_rate_offspeed","bvpt_xwoba_offspeed",
    "bvpt_w_whiff_rate","bvpt_w_chase_rate","bvpt_w_zone_contact_rate","bvpt_w_hard_hit_rate","bvpt_w_xwoba",
    "p_avg_stuff_plus","p_avg_control_plus","p_avg_velo","p_whiff_rate","p_chase_rate","p_zone_rate","p_xwoba",
    "p_num_pitches","p_total_thrown","p_usage_fastball","p_usage_breaking","p_usage_offspeed",
    "p_pitch1_usage","p_pitch1_velo","p_pitch1_whiff","p_pitch1_stuff",
    "p_pitch2_usage","p_pitch2_velo","p_pitch2_whiff","p_pitch2_stuff",
    "p_pitch3_usage","p_pitch3_velo","p_pitch3_whiff","p_pitch3_stuff",
    "park_run_factor","park_hr_factor",
    "bat_r14_k_pct","bat_r14_bb_pct","bat_r14_xwoba","bat_r14_barrel_rate","bat_r14_whiff_rate","bat_r14_chase_rate",
    "p_r14_k_pct","p_r14_bb_pct","p_r14_xwoba","p_r14_whiff_rate","p_r14_chase_rate",
    "inning","outs_when_up","n_thruorder_pitcher","runner_on_1b","runner_on_2b","runner_on_3b",
    "score_diff","runners_on","base_out_state",
    "matchup_k_advantage","interact_k_whiff","platoon_k_split",
    "p_delta_whiff","p_delta_chase","p_delta_xwoba","wl_is_starter","bat_season_pa",
]
# NEW candidate features (must already be built in the parquet).
# BASE_ONLY=1 tests a clean retrain of the proven v2.2 feature set (activates the
# now-live Stuff+/Control+) without the low-value additions that hurt calibration.
if os.environ.get("BASE_ONLY"):
    ADDED = []
else:
    ADDED = [
        "bat_r14_k_delta", "bat_r14_xwoba_delta", "p_r14_k_delta",   # recent-form deltas
        "matchup_bb_advantage",                                       # target BB over-prediction
        "stuff_vs_contact",                                           # now meaningful w/ real Stuff+
        "is_third_time_thru", "tto_squared",                          # fatigue -> K
        "platoon_xwoba_split",
    ]
CATEGORICAL = ["stand", "p_throws"]

CLASSES = ["1B","2B","3B","BB","HBP","HR","IBB","K","OUT"]  # sorted (LabelEncoder order)


def load():
    df = pd.read_parquet(DATA)
    numeric = [f for f in BASE_NUMERIC + ADDED if f in df.columns]
    missing = [f for f in ADDED if f not in df.columns]
    if missing:
        print(f"  WARNING: candidate features missing from data (skipped): {missing}")
    df = prep(df, numeric, CATEGORICAL)
    cls_to_idx = {c: i for i, c in enumerate(CLASSES)}
    df["y"] = df["outcome"].map(cls_to_idx)
    df = df[df["y"].notna()].copy(); df["y"] = df["y"].astype(int)
    return df, numeric + CATEGORICAL


def per_class_cal(y, p):
    return {c: {"actual": float((y == i).mean()), "pred": float(p[:, i].mean())}
            for i, c in enumerate(CLASSES)}


def main():
    df, feats = load()
    max_season = int(df["season"].max())
    tr = df[df["season"] <= max_season - 2]   # 2021..(test-2)
    val = df[df["season"] == max_season - 1]   # test-1 (temporal val)
    trv = df[df["season"] <= max_season - 1]   # all pre-test (final train)
    te = df[df["season"] == max_season]        # test
    print(f"data: {len(df):,} PAs | temporal-train {len(tr):,} | val {len(val):,} | final-train {len(trv):,} | test {len(te):,}")
    print(f"features: {len(feats)}  (added present: {[f for f in ADDED if f in feats]})")

    Xtr, ytr = tr[feats], tr["y"].values
    Xval, yval = val[feats], val["y"].values
    labels = list(range(len(CLASSES)))

    # --- small regularization search on temporal val ---
    grid = list(itertools.product([5, 6], [15, 30], [0.8, 2.0]))  # depth, min_child_weight, reg_lambda
    best = None
    print("\nRegularization search (temporal val log loss):")
    for depth, mcw, lam in grid:
        m = xgb.XGBClassifier(objective="multi:softprob", num_class=len(CLASSES),
            n_estimators=600, max_depth=depth, learning_rate=0.05, subsample=0.8,
            colsample_bytree=0.7, min_child_weight=mcw, reg_alpha=0.05, reg_lambda=lam,
            gamma=0.05, eval_metric="mlogloss", early_stopping_rounds=25, n_jobs=4,
            random_state=42, tree_method="hist")
        m.fit(Xtr, ytr, eval_set=[(Xval, yval)], verbose=False)
        vll = log_loss(yval, m.predict_proba(Xval), labels=labels)
        print(f"  depth={depth} mcw={mcw} lambda={lam}: val_ll={vll:.4f} best_iter={m.best_iteration}")
        if best is None or vll < best["vll"]:
            best = {"depth": depth, "mcw": mcw, "lam": lam, "vll": vll, "iter": m.best_iteration}
    print(f"  -> best: {best}")

    # --- final fit on all pre-test seasons with best hyperparams ---
    n_final = int((best["iter"] + 1) * 1.1)
    final = xgb.XGBClassifier(objective="multi:softprob", num_class=len(CLASSES),
        n_estimators=n_final, max_depth=best["depth"], learning_rate=0.05, subsample=0.8,
        colsample_bytree=0.7, min_child_weight=best["mcw"], reg_alpha=0.05,
        reg_lambda=best["lam"], gamma=0.05, eval_metric="mlogloss", n_jobs=4,
        random_state=42, tree_method="hist")
    t0 = time.time()
    final.fit(trv[feats], trv["y"].values)
    print(f"\nfinal fit: {n_final} trees, {time.time()-t0:.0f}s")

    # --- evaluate candidate vs baseline on same test ---
    Xte, yte = te[feats], te["y"].values
    p_new = final.predict_proba(Xte)
    ll_new = log_loss(yte, p_new, labels=labels)
    br_new = brier_multiclass(yte, p_new, len(CLASSES))
    ll_new_train = log_loss(trv["y"].values, final.predict_proba(trv[feats]), labels=labels)

    base = joblib.load(BASELINE_MODEL)
    bmeta = json.load(open(BASELINE_META))
    bfeats = bmeta["numeric_features"] + bmeta["categorical_features"]
    p_old = base.predict_proba(te[bfeats])
    ll_old = log_loss(yte, p_old, labels=labels)
    br_old = brier_multiclass(yte, p_old, len(CLASSES))

    out = []
    def w(s=""):
        out.append(s); print(s)

    w("\n" + "=" * 68)
    w("CANDIDATE (v2.3) vs BASELINE (v2.2) — test season {}".format(max_season))
    w("=" * 68)
    w(f"  test log loss:  baseline={ll_old:.4f}  candidate={ll_new:.4f}  delta={ll_new-ll_old:+.4f}")
    w(f"  test Brier:     baseline={br_old:.4f}  candidate={br_new:.4f}  delta={br_new-br_old:+.4f}")
    w(f"  candidate gap (train->test): {ll_new-ll_new_train:+.4f}  (overfit check)")
    w("\n  per-class calibration |pred-actual| (%off) — lower is better:")
    w(f"  {'cls':4} {'actual':>7} {'base':>7} {'cand':>7} {'base%':>7} {'cand%':>7}")
    con = per_class_cal(yte, p_new); cob = per_class_cal(yte, p_old)
    tot_b = tot_c = 0.0
    for c in CLASSES:
        a = con[c]["actual"]
        pb, pc = cob[c]["pred"], con[c]["pred"]
        eb = abs(pb-a)/a*100 if a>0 else 0; ec = abs(pc-a)/a*100 if a>0 else 0
        tot_b += eb; tot_c += ec
        w(f"  {c:4} {a:>7.4f} {pb:>7.4f} {pc:>7.4f} {eb:>6.1f}% {ec:>6.1f}%")
    w(f"  mean %off: baseline={tot_b/len(CLASSES):.1f}%  candidate={tot_c/len(CLASSES):.1f}%")

    # feature importance of new features
    imp = pd.DataFrame({"f": feats, "g": final.feature_importances_}).sort_values("g", ascending=False).reset_index(drop=True)
    rank = {f: i for i, f in enumerate(imp["f"])}
    w("\n  new-feature importance (rank of {}):".format(len(feats)))
    for f in ADDED:
        if f in rank:
            w(f"    {f:24} gain={imp.loc[rank[f],'g']:.4f} rank={rank[f]+1}")
    w("  Stuff+/Control+ now:")
    for f in ["p_avg_stuff_plus","p_avg_control_plus"]:
        if f in rank:
            w(f"    {f:24} gain={imp.loc[rank[f],'g']:.4f} rank={rank[f]+1}")

    improved = (ll_new < ll_old - 0.0005) and (tot_c <= tot_b + 1.0) and ((ll_new - ll_new_train) < 0.15)
    w("\n  SHIP GATE: " + ("PASS — candidate improves test log loss without overfitting" if improved
                           else "REVIEW — candidate does not clearly beat baseline"))

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    open(REPORT, "w", encoding="utf-8").write("\n".join(out))
    joblib.dump(final, CAND_MODEL)
    meta = {"model_type": "xgboost_multiclass", "model_version": "v2.3_candidate",
            "target": "pa_outcome", "classes": CLASSES,
            "numeric_features": [f for f in feats if f not in CATEGORICAL],
            "categorical_features": [f for f in feats if f in CATEGORICAL],
            "train_seasons": list(range(2021, max_season)), "test_season": max_season,
            "test_log_loss": float(ll_new), "baseline_test_log_loss": float(ll_old),
            "best_hyperparams": {k: best[k] for k in ("depth","mcw","lam")},
            "n_estimators_used": n_final, "shipped_gate": bool(improved),
            "label_encoding": {c: i for i, c in enumerate(CLASSES)}}
    json.dump(meta, open(CAND_META, "w"), indent=2)
    w(f"\n[candidate saved: {CAND_MODEL} | report: {REPORT}]")


if __name__ == "__main__":
    main()
