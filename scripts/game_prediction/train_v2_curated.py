#!/usr/bin/env python3
"""
Train v2.1: curated feature set.
Keeps v1's proven core features + only new features with demonstrated predictive value.
Matches v1's hyperparameter style (which had good 2026 calibration).
"""
import os, sys, json, time, joblib, warnings
import pandas as pd, numpy as np
from sklearn.metrics import log_loss
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
warnings.filterwarnings("ignore")
import xgboost as xgb

os.chdir(os.path.join(os.path.dirname(__file__), "..", ".."))

NUMERIC_FEATURES = [
    # === V1 CORE ===
    "bat_k_pct", "bat_bb_pct", "bat_whiff_rate", "bat_chase_rate",
    "bat_zone_swing_rate", "bat_zone_contact_rate",
    "bat_avg_ev", "bat_avg_la", "bat_barrel_rate", "bat_hard_hit_rate",
    "bat_sweet_spot_rate", "bat_gb_rate", "bat_fb_rate",
    "bat_hr_per_fb", "bat_iso", "bat_babip", "bat_xwoba",
    "bat_plat_k_pct", "bat_plat_bb_pct", "bat_plat_whiff_rate",
    "bat_plat_chase_rate", "bat_plat_avg_ev", "bat_plat_barrel_rate",
    "bat_plat_xwoba",
    "bvpt_whiff_rate_fastball", "bvpt_chase_rate_fastball",
    "bvpt_zone_contact_rate_fastball", "bvpt_hard_hit_rate_fastball",
    "bvpt_xwoba_fastball",
    "bvpt_whiff_rate_breaking", "bvpt_chase_rate_breaking",
    "bvpt_zone_contact_rate_breaking", "bvpt_hard_hit_rate_breaking",
    "bvpt_xwoba_breaking",
    "bvpt_whiff_rate_offspeed", "bvpt_chase_rate_offspeed",
    "bvpt_zone_contact_rate_offspeed", "bvpt_hard_hit_rate_offspeed",
    "bvpt_xwoba_offspeed",
    "bvpt_w_whiff_rate", "bvpt_w_chase_rate",
    "bvpt_w_zone_contact_rate", "bvpt_w_hard_hit_rate", "bvpt_w_xwoba",
    "p_avg_stuff_plus", "p_avg_control_plus", "p_avg_velo",
    "p_whiff_rate", "p_chase_rate", "p_zone_rate", "p_xwoba",
    "p_num_pitches", "p_total_thrown",
    "p_usage_fastball", "p_usage_breaking", "p_usage_offspeed",
    "p_pitch1_usage", "p_pitch1_velo", "p_pitch1_whiff", "p_pitch1_stuff",
    "p_pitch2_usage", "p_pitch2_velo", "p_pitch2_whiff", "p_pitch2_stuff",
    "p_pitch3_usage", "p_pitch3_velo", "p_pitch3_whiff", "p_pitch3_stuff",
    "park_run_factor", "park_hr_factor",
    "bat_r14_k_pct", "bat_r14_bb_pct", "bat_r14_xwoba",
    "bat_r14_barrel_rate", "bat_r14_whiff_rate", "bat_r14_chase_rate",
    "p_r14_k_pct", "p_r14_bb_pct", "p_r14_xwoba",
    "p_r14_whiff_rate", "p_r14_chase_rate",
    "inning", "outs_when_up", "n_thruorder_pitcher",
    "runner_on_1b", "runner_on_2b", "runner_on_3b",
    # === NEW: proven value features (BvP removed — biases early season) ===
    # "bvp_pa", "bvp_k_pct", "bvp_bb_pct", "bvp_hit_pct", "bvp_hr_pct", "bvp_xwoba",
    "score_diff", "runners_on", "base_out_state",
    "matchup_k_advantage", "interact_k_whiff", "platoon_k_split",
    "p_delta_whiff", "p_delta_chase", "p_delta_xwoba",
    "wl_is_starter",
    "bat_season_pa",
]

CATEGORICAL_FEATURES = ["stand", "p_throws"]

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

print("Loading data...")
df = pd.read_parquet("data/matchup_train_v2.parquet")
print(f"  {len(df):,} PAs, seasons: {sorted(df['season'].unique())}")

all_features = NUMERIC_FEATURES + CATEGORICAL_FEATURES
available = [f for f in all_features if f in df.columns]
print(f"  Using {len(available)} / {len(all_features)} features")

for col in [f for f in NUMERIC_FEATURES if f in df.columns]:
    df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
for col in [f for f in CATEGORICAL_FEATURES if f in df.columns]:
    df[col] = df[col].astype("category").cat.codes.astype("float32")

for col, default in FILL_DEFAULTS.items():
    if col in df.columns:
        df[col] = df[col].fillna(default)
for col in available:
    if col in df.columns:
        df[col] = df[col].fillna(0.0)

le = LabelEncoder()
df["target"] = le.fit_transform(df["outcome"])
class_names = le.classes_.tolist()

max_season = int(df["season"].max())
X_full = df.loc[df["season"] <= (max_season - 1), available]
y_full = df.loc[df["season"] <= (max_season - 1), "target"]
X_test = df.loc[df["season"] == max_season, available]
y_test = df.loc[df["season"] == max_season, "target"]

X_train, X_val, y_train, y_val = train_test_split(X_full, y_full, test_size=0.1, random_state=42)
print(f"  Train: {len(X_train):,}  Val: {len(X_val):,}  Test: {len(X_test):,}")

# Hyperparameters closer to v1 (which calibrated well)
model = xgb.XGBClassifier(
    objective="multi:softprob",
    num_class=len(class_names),
    n_estimators=500,
    max_depth=5,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.7,
    min_child_weight=15,
    reg_alpha=0.05,
    reg_lambda=0.8,
    gamma=0.05,
    eval_metric="mlogloss",
    early_stopping_rounds=20,
    n_jobs=1,
    random_state=42,
    tree_method="hist",
)

print("\nTraining...")
t0 = time.time()
model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=50)
elapsed = time.time() - t0
print(f"  Time: {elapsed:.0f}s  Best iteration: {model.best_iteration}")

# Evaluate
print("\n" + "=" * 60)
print("EVALUATION")
print("=" * 60)
for yr in [2024, 2025, 2026]:
    subset = df[df["season"] == yr]
    X_s = subset[available]
    probs = model.predict_proba(X_s)
    print(f"\nSeason {yr} ({len(subset):,} PAs):")
    for i, cls in enumerate(class_names):
        actual = (subset["outcome"] == cls).mean()
        pred = probs[:, i].mean()
        pct = abs(pred - actual) / actual * 100 if actual > 0 else 0
        marker = " ***" if pct > 15 else ""
        print(f"  {cls}: actual={actual:.4f} pred={pred:.4f} diff={pred-actual:+.4f} ({pct:.1f}% off){marker}")

# Feature importance (gain)
print("\n" + "=" * 60)
print("TOP 30 FEATURES (gain)")
print("=" * 60)
imp = pd.DataFrame({
    "feature": available,
    "gain": model.feature_importances_,
}).sort_values("gain", ascending=False)
for _, row in imp.head(30).iterrows():
    print(f"  {row['gain']:.6f}  {row['feature']}")

# Save
joblib.dump(model, "models/matchup_model_v2.joblib")
meta = {
    "model_type": "xgboost_multiclass",
    "model_version": "v2.2_curated_no_bvp",
    "target": "pa_outcome",
    "classes": class_names,
    "numeric_features": [f for f in NUMERIC_FEATURES if f in available],
    "categorical_features": [f for f in CATEGORICAL_FEATURES if f in available],
    "train_seasons": list(range(2021, max_season)),
    "test_season": max_season,
    "test_log_loss": float(log_loss(y_test, model.predict_proba(X_test))),
    "n_train": int(len(X_train)),
    "n_test": int(len(X_test)),
    "n_estimators_used": int(model.best_iteration + 1) if model.best_iteration else 500,
    "label_encoding": {name: int(idx) for idx, name in enumerate(class_names)},
}
with open("models/matchup_model_v2_meta.json", "w") as f:
    json.dump(meta, f, indent=2)

print(f"\nModel saved. Test log loss: {meta['test_log_loss']:.4f}")
