#!/usr/bin/env python3
"""
Visualize relationships between pitch features and Stuff+ scores.

Generates a set of charts showing how individual features and feature
combinations relate to the v3 Stuff+ model output, broken down by pitch type.
"""
import os
import sys
import json
import math
import warnings

import numpy as np
import pandas as pd
import psycopg2
import joblib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.gridspec import GridSpec

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://basenerd_user:d5LmELIOiEszYPBSLSDT1oIi79gkgDV6@dpg-d5i0tku3jp1c73f1d3gg-a.oregon-postgres.render.com/basenerd",
)
MODEL_PATH = "models/stuff_model.pkl"
META_PATH = "models/stuff_model_meta.json"

OUTPUT_DIR = "visuals/stuff_plus"

# Pitch types to plot (most common)
PITCH_TYPES = {
    "FF": "4-Seam Fastball",
    "SI": "Sinker",
    "FC": "Cutter",
    "SL": "Slider",
    "ST": "Sweeper",
    "CU": "Curveball",
    "CH": "Changeup",
    "FS": "Splitter",
}

# Basenerd color palette
COLORS = {
    "FF": "#E63946",
    "SI": "#F4845F",
    "FC": "#457B9D",
    "SL": "#2A9D8F",
    "ST": "#264653",
    "CU": "#6A4C93",
    "CH": "#E9C46A",
    "FS": "#8D99AE",
}

Y_PLATE = 17.0 / 12.0


def get_conn():
    url = DATABASE_URL
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url)


def compute_approach_angles(df):
    y0 = df["release_pos_y"].values.astype(np.float64)
    vy0 = df["vy0"].values.astype(np.float64)
    ay = df["ay"].values.astype(np.float64)
    vx0 = df["vx0"].values.astype(np.float64)
    vz0 = df["vz0"].values.astype(np.float64)
    ax = df["ax"].values.astype(np.float64)
    az = df["az"].values.astype(np.float64)

    a = 0.5 * ay
    b = vy0
    c = y0 - Y_PLATE
    discriminant = np.maximum(b**2 - 4.0 * a * c, 0.0)
    sqrt_disc = np.sqrt(discriminant)
    with np.errstate(divide="ignore", invalid="ignore"):
        t1 = (-b + sqrt_disc) / (2.0 * a)
        t2 = (-b - sqrt_disc) / (2.0 * a)
    t = np.where((t2 > 0) & (t2 < t1), t2, t1)
    t = np.where(t > 0, t, np.nan)
    abs_vy = np.abs(vy0 + ay * t)
    with np.errstate(divide="ignore", invalid="ignore"):
        df["vert_approach_angle"] = np.degrees(np.arctan2(vz0 + az * t, abs_vy))
        df["horiz_approach_angle"] = np.degrees(np.arctan2(vx0 + ax * t, abs_vy))
    return df


def fetch_and_score():
    """Fetch 2025 data, compute approach angles, score stuff+."""
    conn = get_conn()
    print("Fetching 2025 regular-season pitches...")
    sql = """
        SELECT
            release_speed, release_spin_rate, release_extension,
            release_pos_x, release_pos_z, release_pos_y,
            pfx_x, pfx_z,
            vx0, vy0, vz0, ax, ay, az,
            sz_top, sz_bot,
            pitch_type, p_throws
        FROM statcast_pitches
        WHERE game_type = 'R'
          AND game_year = 2025
          AND release_speed IS NOT NULL
          AND pfx_x IS NOT NULL
          AND pfx_z IS NOT NULL
          AND pitch_type IS NOT NULL
          AND vy0 IS NOT NULL
          AND ay IS NOT NULL
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    print(f"  Loaded {len(df):,} pitches")

    # Compute approach angles
    df = compute_approach_angles(df)
    df = df.dropna(subset=["vert_approach_angle", "horiz_approach_angle"]).reset_index(drop=True)

    # Convert movement to inches for display
    df["ivb_in"] = df["pfx_z"] * 12.0
    df["hb_in"] = df["pfx_x"] * 12.0

    # Score stuff+
    print("Scoring stuff+...")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        pipe = joblib.load(MODEL_PATH)
    with open(META_PATH) as f:
        meta = json.load(f)

    num_feats = meta["num_features"]
    cat_feats = meta["cat_features"]
    goodness_std = float(meta.get("goodness_std", 0.01)) or 0.01
    center = float(meta.get("stuff_center", 100.0))
    scale = float(meta.get("stuff_scale", 15.0))

    X = df[num_feats + cat_feats].copy()
    for c in num_feats:
        X[c] = pd.to_numeric(X[c], errors="coerce")
    preds = pipe.predict(X)
    goodness = -pd.Series(preds, index=X.index)
    sp = center + scale * (goodness / goodness_std)
    df["stuff_plus"] = sp.clip(40, 160)

    # Filter to common pitch types
    df = df[df["pitch_type"].isin(PITCH_TYPES.keys())].reset_index(drop=True)
    print(f"  {len(df):,} pitches after filtering to common types")
    return df


# ---------------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------------

def _style_ax(ax, title, xlabel, ylabel):
    ax.set_title(title, fontsize=13, fontweight="bold", pad=8)
    ax.set_xlabel(xlabel, fontsize=10)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(labelsize=9)


def _sp_cmap():
    """Red (bad) → White (100) → Blue (good) colormap for Stuff+."""
    return LinearSegmentedColormap.from_list(
        "stuff_plus",
        [(0.0, "#d73027"), (0.375, "#fee08b"), (0.5, "#ffffbf"),
         (0.625, "#91bfdb"), (1.0, "#4575b4")],
    )


# ---------------------------------------------------------------------------
# Chart 1: Single-feature scatter strips (VAA, Velo, Spin, Extension, IVB)
# ---------------------------------------------------------------------------

def plot_feature_strips(df):
    """Small-multiple scatter: feature vs Stuff+ for each pitch type."""
    features = [
        ("vert_approach_angle", "VAA (°)"),
        ("release_speed", "Velo (mph)"),
        ("release_spin_rate", "Spin (rpm)"),
        ("release_extension", "Extension (ft)"),
        ("ivb_in", "IVB (in)"),
        ("hb_in", "HB (in)"),
    ]
    pitch_order = [pt for pt in PITCH_TYPES if pt in df["pitch_type"].unique()]
    n_feats = len(features)
    n_types = len(pitch_order)

    fig, axes = plt.subplots(n_feats, n_types, figsize=(3.2 * n_types, 3.0 * n_feats),
                              constrained_layout=True)

    for fi, (feat, feat_label) in enumerate(features):
        for ti, pt in enumerate(pitch_order):
            ax = axes[fi, ti]
            sub = df[df["pitch_type"] == pt].dropna(subset=[feat, "stuff_plus"])
            if len(sub) < 50:
                ax.set_visible(False)
                continue

            # Sample for performance
            if len(sub) > 5000:
                sub = sub.sample(5000, random_state=42)

            ax.scatter(sub[feat], sub["stuff_plus"],
                       alpha=0.08, s=4, c=COLORS.get(pt, "#888"), rasterized=True)

            # Binned mean line
            try:
                bins = pd.qcut(sub[feat], q=20, duplicates="drop")
                means = sub.groupby(bins)["stuff_plus"].mean()
                bin_centers = sub.groupby(bins)[feat].mean()
                ax.plot(bin_centers.values, means.values, color="black", linewidth=2, zorder=5)
            except Exception:
                pass

            ax.axhline(100, color="gray", linewidth=0.5, linestyle="--", alpha=0.6)

            if fi == 0:
                ax.set_title(PITCH_TYPES[pt], fontsize=11, fontweight="bold")
            if ti == 0:
                ax.set_ylabel("Stuff+", fontsize=9)
            if fi == n_feats - 1:
                ax.set_xlabel(feat_label, fontsize=9)

            ax.set_ylim(55, 145)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.tick_params(labelsize=7)

    fig.suptitle("Feature vs Stuff+ by Pitch Type (2025, binned mean in black)",
                 fontsize=16, fontweight="bold", y=1.01)

    path = os.path.join(OUTPUT_DIR, "feature_strips.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  Saved {path}")


# ---------------------------------------------------------------------------
# Chart 2: VAA + Velo interaction heatmap per pitch type
# ---------------------------------------------------------------------------

def plot_vaa_velo_heatmaps(df):
    """2D heatmap: VAA × Velo → mean Stuff+ for key pitch types."""
    pitch_order = ["FF", "SI", "SL", "CU", "CH", "ST"]
    pitch_order = [pt for pt in pitch_order if pt in df["pitch_type"].unique()]
    n = len(pitch_order)
    cols = min(n, 3)
    rows = math.ceil(n / cols)

    fig, axes = plt.subplots(rows, cols, figsize=(6.5 * cols, 5.5 * rows),
                              constrained_layout=True)
    if rows == 1 and cols == 1:
        axes = np.array([[axes]])
    elif rows == 1:
        axes = axes[np.newaxis, :]
    elif cols == 1:
        axes = axes[:, np.newaxis]

    cmap = _sp_cmap()

    for idx, pt in enumerate(pitch_order):
        r, c = divmod(idx, cols)
        ax = axes[r, c]
        sub = df[df["pitch_type"] == pt].dropna(subset=["vert_approach_angle", "release_speed", "stuff_plus"])
        if len(sub) < 200:
            ax.set_visible(False)
            continue

        # Bin into grid
        vaa_bins = pd.cut(sub["vert_approach_angle"], bins=25)
        velo_bins = pd.cut(sub["release_speed"], bins=25)
        grid = sub.groupby([velo_bins, vaa_bins])["stuff_plus"].mean().unstack()

        # Get bin centers for axis labels
        velo_centers = [b.mid for b in grid.index]
        vaa_centers = [b.mid for b in grid.columns]

        im = ax.pcolormesh(
            vaa_centers, velo_centers, grid.values,
            cmap=cmap, vmin=80, vmax=120, shading="auto"
        )
        ax.set_title(f"{PITCH_TYPES[pt]}", fontsize=13, fontweight="bold")
        ax.set_xlabel("VAA (°)", fontsize=10)
        ax.set_ylabel("Velo (mph)", fontsize=10)
        ax.tick_params(labelsize=8)

        fig.colorbar(im, ax=ax, label="Mean Stuff+", shrink=0.8)

    # Hide unused axes
    for idx in range(len(pitch_order), rows * cols):
        r, c = divmod(idx, cols)
        axes[r, c].set_visible(False)

    fig.suptitle("VAA × Velo → Mean Stuff+ (2025)", fontsize=16, fontweight="bold", y=1.01)

    path = os.path.join(OUTPUT_DIR, "vaa_velo_heatmaps.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  Saved {path}")


# ---------------------------------------------------------------------------
# Chart 3: Movement profile (IVB × HB) colored by Stuff+
# ---------------------------------------------------------------------------

def plot_movement_stuff(df):
    """Movement scatter (IVB × HB) colored by Stuff+ for each pitch type."""
    pitch_order = [pt for pt in PITCH_TYPES if pt in df["pitch_type"].unique()]
    n = len(pitch_order)
    cols = min(n, 4)
    rows = math.ceil(n / cols)

    fig, axes = plt.subplots(rows, cols, figsize=(5 * cols, 4.5 * rows),
                              constrained_layout=True)
    axes = np.atleast_2d(axes)

    cmap = _sp_cmap()

    for idx, pt in enumerate(pitch_order):
        r, c = divmod(idx, cols)
        ax = axes[r, c]
        sub = df[df["pitch_type"] == pt].dropna(subset=["hb_in", "ivb_in", "stuff_plus"])
        if len(sub) < 50:
            ax.set_visible(False)
            continue

        if len(sub) > 8000:
            sub = sub.sample(8000, random_state=42)

        sc = ax.scatter(sub["hb_in"], sub["ivb_in"], c=sub["stuff_plus"],
                        cmap=cmap, vmin=70, vmax=130, s=3, alpha=0.25, rasterized=True)
        ax.set_title(PITCH_TYPES[pt], fontsize=12, fontweight="bold")
        ax.set_xlabel("HB (in)", fontsize=9)
        ax.set_ylabel("IVB (in)", fontsize=9)
        ax.axhline(0, color="gray", linewidth=0.4, alpha=0.5)
        ax.axvline(0, color="gray", linewidth=0.4, alpha=0.5)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.tick_params(labelsize=8)

    for idx in range(len(pitch_order), rows * cols):
        r, c = divmod(idx, cols)
        axes[r, c].set_visible(False)

    fig.colorbar(sc, ax=axes, label="Stuff+", shrink=0.6, pad=0.02)
    fig.suptitle("Pitch Movement Colored by Stuff+ (2025)", fontsize=16, fontweight="bold", y=1.01)

    path = os.path.join(OUTPUT_DIR, "movement_stuff_scatter.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  Saved {path}")


# ---------------------------------------------------------------------------
# Chart 4: VAA + Spin interaction for fastballs
# ---------------------------------------------------------------------------

def plot_vaa_spin_fastballs(df):
    """2D heatmap: VAA × Spin Rate → mean Stuff+ for FF only."""
    sub = df[(df["pitch_type"] == "FF")].dropna(
        subset=["vert_approach_angle", "release_spin_rate", "stuff_plus"]
    )
    if len(sub) < 500:
        print("  Skipping VAA × Spin chart (not enough FF data)")
        return

    fig, ax = plt.subplots(figsize=(10, 7), constrained_layout=True)

    vaa_bins = pd.cut(sub["vert_approach_angle"], bins=30)
    spin_bins = pd.cut(sub["release_spin_rate"], bins=30)
    grid = sub.groupby([spin_bins, vaa_bins])["stuff_plus"].mean().unstack()

    vaa_centers = [b.mid for b in grid.columns]
    spin_centers = [b.mid for b in grid.index]

    cmap = _sp_cmap()
    im = ax.pcolormesh(vaa_centers, spin_centers, grid.values,
                        cmap=cmap, vmin=85, vmax=115, shading="auto")
    ax.set_xlabel("VAA (°)", fontsize=12)
    ax.set_ylabel("Spin Rate (rpm)", fontsize=12)
    ax.set_title("4-Seam Fastball: VAA × Spin Rate → Mean Stuff+ (2025)",
                 fontsize=14, fontweight="bold")
    fig.colorbar(im, ax=ax, label="Mean Stuff+", shrink=0.8)

    path = os.path.join(OUTPUT_DIR, "ff_vaa_spin_heatmap.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  Saved {path}")


# ---------------------------------------------------------------------------
# Chart 5: Distribution ridgeline — Stuff+ by pitch type
# ---------------------------------------------------------------------------

def plot_stuff_distributions(df):
    """Horizontal violin/ridge plot: Stuff+ distribution per pitch type."""
    pitch_order = [pt for pt in PITCH_TYPES if pt in df["pitch_type"].unique()]
    n = len(pitch_order)

    fig, ax = plt.subplots(figsize=(10, 1.2 * n + 1), constrained_layout=True)

    for i, pt in enumerate(pitch_order):
        sub = df[df["pitch_type"] == pt]["stuff_plus"].dropna()
        if len(sub) < 50:
            continue
        parts = ax.violinplot(sub.values, positions=[i], vert=False,
                              showmedians=True, showextrema=False, widths=0.8)
        for pc in parts["bodies"]:
            pc.set_facecolor(COLORS.get(pt, "#888"))
            pc.set_alpha(0.7)
        parts["cmedians"].set_color("black")

    ax.set_yticks(range(n))
    ax.set_yticklabels([PITCH_TYPES[pt] for pt in pitch_order], fontsize=10)
    ax.axvline(100, color="gray", linewidth=1, linestyle="--", alpha=0.6)
    ax.set_xlabel("Stuff+", fontsize=12)
    ax.set_xlim(40, 160)
    ax.set_title("Stuff+ Distribution by Pitch Type (2025)", fontsize=14, fontweight="bold")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    path = os.path.join(OUTPUT_DIR, "stuff_distributions.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  Saved {path}")


# ---------------------------------------------------------------------------
# Chart 6: Extension × VAA for fastballs
# ---------------------------------------------------------------------------

def plot_extension_vaa_fastballs(df):
    """2D heatmap: Extension × VAA → mean Stuff+ for FF."""
    sub = df[df["pitch_type"] == "FF"].dropna(
        subset=["release_extension", "vert_approach_angle", "stuff_plus"]
    )
    if len(sub) < 500:
        print("  Skipping Extension × VAA chart (not enough FF data)")
        return

    fig, ax = plt.subplots(figsize=(10, 7), constrained_layout=True)

    ext_bins = pd.cut(sub["release_extension"], bins=25)
    vaa_bins = pd.cut(sub["vert_approach_angle"], bins=25)
    grid = sub.groupby([ext_bins, vaa_bins])["stuff_plus"].mean().unstack()

    vaa_centers = [b.mid for b in grid.columns]
    ext_centers = [b.mid for b in grid.index]

    cmap = _sp_cmap()
    im = ax.pcolormesh(vaa_centers, ext_centers, grid.values,
                        cmap=cmap, vmin=85, vmax=115, shading="auto")
    ax.set_xlabel("VAA (°)", fontsize=12)
    ax.set_ylabel("Extension (ft)", fontsize=12)
    ax.set_title("4-Seam Fastball: Extension × VAA → Mean Stuff+ (2025)",
                 fontsize=14, fontweight="bold")
    fig.colorbar(im, ax=ax, label="Mean Stuff+", shrink=0.8)

    path = os.path.join(OUTPUT_DIR, "ff_extension_vaa_heatmap.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  Saved {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = fetch_and_score()

    print("\nGenerating visualizations...")
    plot_stuff_distributions(df)
    plot_feature_strips(df)
    plot_vaa_velo_heatmaps(df)
    plot_movement_stuff(df)
    plot_vaa_spin_fastballs(df)
    plot_extension_vaa_fastballs(df)

    print(f"\nAll charts saved to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
