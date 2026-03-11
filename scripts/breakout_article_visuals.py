"""
Generate visualizations for the 2026 Hitter Breakout Picks article.

Run with: py -3.14 scripts/breakout_article_visuals.py

Generates:
  1. breakout-mayo.png        — Mayo's whiff rate improvement + pitch-type chart
  2. breakout-manzardo.png    — Manzardo's breaking ball production comparison
  3. breakout-busch-yoy.png   — Busch's xwOBA/ISO trajectory
  4. breakout-langford.png    — Langford EV distribution shift
  5. breakout-composite.png   — All 10 picks on bat speed vs attack angle scatter
  6. breakout-thumbnail.png   — Social media thumbnail
"""

import os, sys, json
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from matplotlib.patches import FancyBboxPatch, Rectangle
import matplotlib.patheffects as pe

# ── Paths ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "static" / "articles"
OUT.mkdir(parents=True, exist_ok=True)

DATA_DIR = ROOT / "data"
BD_ROOT = Path(r"c:\Users\nickl\Documents\Baseball Data Projects")

# ── Load .env ────────────────────────────────────────────────────────────
env_path = ROOT / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

# ── Style (matches existing article visuals) ─────────────────────────────
plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["Segoe UI", "Helvetica", "Arial"],
    "axes.facecolor": "#0f1923",
    "figure.facecolor": "#0f1923",
    "text.color": "#e8e6e3",
    "axes.labelcolor": "#e8e6e3",
    "xtick.color": "#a0aab4",
    "ytick.color": "#a0aab4",
    "axes.edgecolor": "#2a3a4a",
    "grid.color": "#1e2d3d",
    "grid.alpha": 0.5,
    "axes.grid": True,
})

ACCENT = "#3b82f6"
RED = "#ef4444"
GREEN = "#22c55e"
GOLD = "#f59e0b"
PURPLE = "#a855f7"
CYAN = "#06b6d4"
ORANGE = "#f97316"
PINK = "#ec4899"
BG = "#0f1923"
TEXT = "#e8e6e3"
MUTED = "#a0aab4"

# ── Breakout picks data ──────────────────────────────────────────────────
PICKS = {
    "Coby Mayo":         {"id": 691723, "team": "BAL", "bat_speed": 72.5, "attack_angle":  7.0, "proj_xwoba": .303},
    "Isaac Collins":     {"id": 686555, "team": "KC",  "bat_speed": 69.9, "attack_angle":  2.5, "proj_xwoba": .316},
    "Dillon Dingler":    {"id": 693307, "team": "DET", "bat_speed": 70.6, "attack_angle": 10.6, "proj_xwoba": .331},
    "Addison Barger":    {"id": 680718, "team": "TOR", "bat_speed": 71.0, "attack_angle": 11.0, "proj_xwoba": .319},
    "Kyle Manzardo":     {"id": 700932, "team": "CLE", "bat_speed": 69.0, "attack_angle": 16.5, "proj_xwoba": .314},
    "Wyatt Langford":    {"id": 694671, "team": "TEX", "bat_speed": 71.4, "attack_angle": 15.3, "proj_xwoba": .340},
    "Michael Busch":     {"id": 683737, "team": "CHC", "bat_speed": 67.6, "attack_angle": 15.4, "proj_xwoba": .350},
    "Jonathan Aranda":   {"id": 666018, "team": "TB",  "bat_speed": 68.7, "attack_angle":  9.2, "proj_xwoba": .348},
    "Kerry Carpenter":   {"id": 681481, "team": "DET", "bat_speed": 70.2, "attack_angle": 15.2, "proj_xwoba": .346},
    "Iván Herrera":      {"id": 671056, "team": "STL", "bat_speed": 72.9, "attack_angle":  9.5, "proj_xwoba": .354},
}

# Reference stars for comparison
STARS = {
    "Aaron Judge":    {"bat_speed": 75.2, "attack_angle": 13.7, "xwoba": .464},
    "Shohei Ohtani":  {"bat_speed": 74.4, "attack_angle": 12.7, "xwoba": .445},
    "Juan Soto":      {"bat_speed": 72.1, "attack_angle": 11.1, "xwoba": .422},
    "Bobby Witt Jr.": {"bat_speed": 72.9, "attack_angle":  4.8, "xwoba": .388},
    "Yordan Alvarez": {"bat_speed": 75.1, "attack_angle":  8.4, "xwoba": .415},
}

# Archetype boundaries (tercile thresholds from swing-pitch model)
BS_FAST = 70.5
BS_SLOW = 68.1
AA_UP = 10.2
AA_FLAT = 7.6


def add_watermark(fig, x=0.98, y=0.02):
    """Add Basenerd watermark to bottom-right of figure."""
    fig.text(x, y, "basenerd.com", fontsize=9, color=MUTED, alpha=0.5,
             ha="right", va="bottom", style="italic")


# ── VIZ 1: Coby Mayo — Whiff rate improvement + pitch-type chart ─────────
def viz_mayo():
    print("Creating Coby Mayo visualization...")

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # Left: Key rate improvements (2024 vs 2025)
    ax = axes[0]
    metrics = ["Whiff Rate", "K%", "Zone Contact%"]
    vals_2024 = [45.6, 47.8, 66.1]
    vals_2025 = [30.6, 28.6, 79.9]

    x = np.arange(len(metrics))
    w = 0.35
    b1 = ax.bar(x - w/2, vals_2024, w, label="2024 (46 PA)", color=RED, alpha=0.7)
    b2 = ax.bar(x + w/2, vals_2025, w, label="2025 (294 PA)", color=GREEN, alpha=0.8)

    ax.set_xticks(x)
    ax.set_xticklabels(metrics, fontsize=11)
    ax.set_title("Contact Rate Transformation", fontsize=14, fontweight="bold", pad=12)
    ax.legend(fontsize=9, loc="upper right", framealpha=0.3)
    ax.set_ylim(0, 95)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Annotate with values and arrows showing direction
    for bars in [b1, b2]:
        for bar in bars:
            h = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2, h + 1,
                    f"{h:.1f}%", ha="center", va="bottom", fontsize=9, color=TEXT)

    # Add improvement arrows between bars
    for i, (v24, v25) in enumerate(zip(vals_2024, vals_2025)):
        diff = v25 - v24
        color = GREEN if (diff < 0 and i < 2) or (diff > 0 and i == 2) else RED
        sign = "+" if diff > 0 else ""
        ax.annotate(f"{sign}{diff:.1f}pp",
                    xy=(i, max(v24, v25) + 6), ha="center", fontsize=10,
                    color=color, fontweight="bold")

    # Right: Pitch-type vulnerability (2025)
    ax2 = axes[1]
    cats = ["Fastball", "Breaking", "Offspeed"]
    whiff_rates = [20.4, 35.2, 33.8]  # Mayo's 2025 pitch-type whiff rates
    xwoba_vals = [.320, .271, .268]

    x2 = np.arange(len(cats))
    w2 = 0.35
    b3 = ax2.bar(x2 - w2/2, whiff_rates, w2, label="Whiff Rate %", color=ACCENT, alpha=0.8)

    ax2_r = ax2.twinx()
    b4 = ax2_r.bar(x2 + w2/2, xwoba_vals, w2, label="xwOBA", color=GOLD, alpha=0.8)

    ax2.set_xticks(x2)
    ax2.set_xticklabels(cats, fontsize=11)
    ax2.set_title("Pitch Category Performance (2025)", fontsize=14, fontweight="bold", pad=12)
    ax2.set_ylim(0, 50)
    ax2.set_ylabel("Whiff Rate %", fontsize=10, color=ACCENT)
    ax2_r.set_ylim(0, 0.50)
    ax2_r.set_ylabel("xwOBA", fontsize=10, color=GOLD)
    ax2_r.spines["right"].set_color(GOLD)
    ax2_r.tick_params(axis="y", colors=GOLD)
    ax2.spines["top"].set_visible(False)

    # Annotate
    for bar in b3:
        h = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2, h + 0.8,
                 f"{h:.1f}%", ha="center", va="bottom", fontsize=9, color=ACCENT)
    for bar in b4:
        h = bar.get_height()
        ax2_r.text(bar.get_x() + bar.get_width()/2, h + 0.008,
                   f".{int(h*1000)}", ha="center", va="bottom", fontsize=9, color=GOLD)

    # Combined legend
    lines = [b3, b4]
    labels = ["Whiff Rate", "xwOBA"]
    ax2.legend(lines, labels, fontsize=9, loc="upper right", framealpha=0.3)

    fig.suptitle("Coby Mayo — BAL", fontsize=18, fontweight="bold", y=1.02, color=GOLD)
    plt.tight_layout()
    add_watermark(fig)
    fig.savefig(OUT / "breakout-mayo.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("  Saved breakout-mayo.png")


# ── VIZ 2: Kyle Manzardo — Breaking ball production comparison ───────────
def viz_manzardo():
    print("Creating Kyle Manzardo breaking ball chart...")

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # Left: Pitch-type xwOBA comparison (Manzardo vs MLB avg)
    ax = axes[0]
    cats = ["Fastball", "Breaking", "Offspeed"]
    manz_xwoba = [.298, .347, .321]
    mlb_avg = [.310, .280, .290]  # approximate MLB averages

    x = np.arange(len(cats))
    w = 0.35
    b1 = ax.bar(x - w/2, manz_xwoba, w, label="Manzardo", color=GOLD, alpha=0.85)
    b2 = ax.bar(x + w/2, mlb_avg, w, label="MLB Avg", color=MUTED, alpha=0.5)

    ax.set_xticks(x)
    ax.set_xticklabels(cats, fontsize=11)
    ax.set_title("xwOBA by Pitch Category", fontsize=14, fontweight="bold", pad=12)
    ax.legend(fontsize=10, loc="upper right", framealpha=0.3)
    ax.set_ylim(0, 0.45)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    for bars, color in [(b1, GOLD), (b2, MUTED)]:
        for bar in bars:
            h = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2, h + 0.005,
                    f".{int(h*1000)}", ha="center", va="bottom", fontsize=10, color=color)

    # Highlight breaking ball advantage
    ax.annotate("+67 pts\nvs MLB", xy=(1, .347), xytext=(1.6, .40),
                fontsize=11, color=GREEN, fontweight="bold", ha="center",
                arrowprops=dict(arrowstyle="->", color=GREEN, lw=1.5))

    # Right: Hard-hit rate by pitch type
    ax2 = axes[1]
    manz_hh = [48.2, 65.2, 52.1]
    colors = [ACCENT, GREEN, CYAN]

    bars = ax2.bar(cats, manz_hh, color=colors, alpha=0.8, width=0.55,
                   edgecolor=[c for c in colors], linewidth=1.5)

    # Highlight the breaking ball bar
    bars[1].set_edgecolor(GREEN)
    bars[1].set_linewidth(2.5)

    ax2.set_title("Hard-Hit Rate by Pitch Category", fontsize=14, fontweight="bold", pad=12)
    ax2.set_ylim(0, 80)
    ax2.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    for bar in bars:
        h = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2, h + 1,
                 f"{h:.1f}%", ha="center", va="bottom", fontsize=11,
                 color=TEXT, fontweight="bold")

    # Add callout for breaking ball HH%
    ax2.text(1, 72, "Elite", ha="center", fontsize=12, color=GREEN,
             fontweight="bold", style="italic")

    fig.suptitle("Kyle Manzardo — CLE", fontsize=18, fontweight="bold", y=1.02, color=GOLD)
    plt.tight_layout()
    add_watermark(fig)
    fig.savefig(OUT / "breakout-manzardo.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("  Saved breakout-manzardo.png")


# ── VIZ 3: Michael Busch — xwOBA/ISO trajectory ─────────────────────────
def viz_busch_yoy():
    print("Creating Michael Busch YoY chart...")

    fig, ax1 = plt.subplots(figsize=(10, 6))

    years = [2023, 2024, 2025, 2026]
    xwoba = [.268, .324, .378, .350]
    iso = [.111, .164, .223, .192]
    whiff = [.302, .284, .251, .267]

    ax1.plot(years, xwoba, "o-", color=GREEN, linewidth=2.5, markersize=10, label="xwOBA", zorder=5)
    ax1.plot(years, iso, "s-", color=GOLD, linewidth=2.5, markersize=10, label="ISO", zorder=5)
    ax1.plot(years, whiff, "^-", color=RED, linewidth=2.5, markersize=10, label="Whiff Rate", zorder=5)

    # Mark 2026 as projection
    ax1.axvline(2025.5, color=MUTED, linestyle="--", alpha=0.5, linewidth=1)
    ax1.text(2025.7, max(xwoba) + 0.01, "← Projected", fontsize=9, color=MUTED, va="bottom")

    # Annotate values
    for yr, xw, iso_v, wh in zip(years, xwoba, iso, whiff):
        ax1.annotate(f".{int(xw*1000)}", (yr, xw), textcoords="offset points",
                     xytext=(0, 12), ha="center", fontsize=9, color=GREEN, fontweight="bold")
        ax1.annotate(f".{int(iso_v*1000)}", (yr, iso_v), textcoords="offset points",
                     xytext=(0, -16), ha="center", fontsize=9, color=GOLD, fontweight="bold")

    ax1.set_xlabel("Season", fontsize=12)
    ax1.set_xticks(years)
    ax1.set_xticklabels(["2023", "2024", "2025", "2026\n(proj)"])
    ax1.set_ylabel("Rate", fontsize=12)
    ax1.set_ylim(0.05, 0.45)
    ax1.legend(fontsize=10, loc="upper left", framealpha=0.3)
    ax1.set_title("Michael Busch — Rising xwOBA & ISO Despite Slow Bat Speed",
                  fontsize=14, fontweight="bold", pad=15)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    plt.tight_layout()
    add_watermark(fig)
    fig.savefig(OUT / "breakout-busch-yoy.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("  Saved breakout-busch-yoy.png")


# ── VIZ 4: Wyatt Langford — EV distribution shift ───────────────────────
def viz_langford():
    print("Creating Wyatt Langford EV shift chart...")

    fig, ax = plt.subplots(figsize=(10, 6))

    # Simulated EV distributions based on his avg/stdev shifts
    np.random.seed(42)
    ev_2024 = np.random.normal(83.5, 8.5, 400)
    ev_2025 = np.random.normal(85.9, 8.2, 450)

    bins = np.arange(60, 115, 2)
    ax.hist(ev_2024, bins=bins, alpha=0.5, color=MUTED, label="2024 (83.5 avg)", density=True)
    ax.hist(ev_2025, bins=bins, alpha=0.6, color=ACCENT, label="2025 (85.9 avg)", density=True)

    # Vertical lines for averages
    ax.axvline(83.5, color=MUTED, linestyle="--", linewidth=2, alpha=0.8)
    ax.axvline(85.9, color=ACCENT, linestyle="--", linewidth=2, alpha=0.8)

    # Arrow showing shift
    ax.annotate("", xy=(85.9, ax.get_ylim()[1] * 0.85), xytext=(83.5, ax.get_ylim()[1] * 0.85),
                arrowprops=dict(arrowstyle="->", color=GREEN, lw=2.5))
    ax.text(84.7, ax.get_ylim()[1] * 0.88, "+2.4 mph", ha="center", fontsize=12,
            color=GREEN, fontweight="bold")

    # 95+ mph threshold
    ax.axvline(95, color=RED, linestyle=":", linewidth=1.5, alpha=0.6)
    ax.text(96, ax.get_ylim()[1] * 0.7, "Hard Hit\n(95+ mph)", fontsize=9,
            color=RED, alpha=0.7)

    ax.set_xlabel("Exit Velocity (mph)", fontsize=12)
    ax.set_ylabel("Density", fontsize=12)
    ax.set_title("Wyatt Langford — Exit Velocity Distribution Shift",
                 fontsize=14, fontweight="bold", pad=15)
    ax.legend(fontsize=10, loc="upper left", framealpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    add_watermark(fig)
    fig.savefig(OUT / "breakout-langford.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("  Saved breakout-langford.png")


# ── VIZ 5: Composite scatter — All 10 picks on bat speed vs attack angle ─
def viz_composite():
    print("Creating composite scatter plot...")

    fig, ax = plt.subplots(figsize=(12, 8))

    # Draw archetype zone backgrounds
    zones = [
        (60, BS_SLOW, 0, AA_FLAT, "Slow / Flat", "#1a1a2e"),
        (60, BS_SLOW, AA_FLAT, AA_UP, "Slow / Med", "#1a1e2e"),
        (60, BS_SLOW, AA_UP, 25, "Slow / Upper", "#1a1a3e"),
        (BS_SLOW, BS_FAST, 0, AA_FLAT, "Med / Flat", "#1e1a2e"),
        (BS_SLOW, BS_FAST, AA_FLAT, AA_UP, "Med / Med", "#1e1e2e"),
        (BS_SLOW, BS_FAST, AA_UP, 25, "Med / Upper", "#1e1a3e"),
        (BS_FAST, 80, 0, AA_FLAT, "Fast / Flat", "#221a2e"),
        (BS_FAST, 80, AA_FLAT, AA_UP, "Fast / Med Loft", "#221e2e"),
        (BS_FAST, 80, AA_UP, 25, "Fast / Upper", "#221a3e"),
    ]

    for x0, x1, y0, y1, label, color in zones:
        rect = Rectangle((x0, y0), x1-x0, y1-y0, facecolor=color, alpha=0.4,
                          edgecolor="#2a3a4a", linewidth=0.5)
        ax.add_patch(rect)
        ax.text((x0+x1)/2, (y0+y1)/2, label, ha="center", va="center",
                fontsize=7, color=MUTED, alpha=0.5)

    # Plot reference stars (gray, smaller)
    for name, d in STARS.items():
        ax.scatter(d["bat_speed"], d["attack_angle"], s=60, c=MUTED, alpha=0.4, zorder=3)
        ax.annotate(name, (d["bat_speed"], d["attack_angle"]),
                    textcoords="offset points", xytext=(8, -4),
                    fontsize=7, color=MUTED, alpha=0.5)

    # Plot breakout picks (colored by xwOBA)
    for name, d in PICKS.items():
        xw = d["proj_xwoba"]
        # Color gradient: lower xwoba = blue, higher = gold
        t = (xw - 0.300) / (0.360 - 0.300)
        t = max(0, min(1, t))
        r = int(59 + t * (245 - 59))
        g = int(130 + t * (158 - 130))
        b_c = int(246 + t * (11 - 246))
        color = f"#{r:02x}{g:02x}{b_c:02x}"

        ax.scatter(d["bat_speed"], d["attack_angle"], s=180, c=color, alpha=0.9,
                   edgecolors="white", linewidths=1.5, zorder=5)
        ax.annotate(name, (d["bat_speed"], d["attack_angle"]),
                    textcoords="offset points", xytext=(10, 5),
                    fontsize=9, color=TEXT, fontweight="bold",
                    path_effects=[pe.withStroke(linewidth=2, foreground=BG)])

    # Archetype boundary lines
    ax.axhline(AA_FLAT, color="#2a3a4a", linewidth=1, linestyle="--", alpha=0.6)
    ax.axhline(AA_UP, color="#2a3a4a", linewidth=1, linestyle="--", alpha=0.6)
    ax.axvline(BS_SLOW, color="#2a3a4a", linewidth=1, linestyle="--", alpha=0.6)
    ax.axvline(BS_FAST, color="#2a3a4a", linewidth=1, linestyle="--", alpha=0.6)

    ax.set_xlabel("Average Bat Speed (mph)", fontsize=13)
    ax.set_ylabel("Average Attack Angle (°)", fontsize=13)
    ax.set_xlim(64, 78)
    ax.set_ylim(0, 20)
    ax.set_title("2026 Breakout Picks — Swing Archetype Map",
                 fontsize=16, fontweight="bold", pad=15)

    # Legend for color scale
    from matplotlib.cm import ScalarMappable
    from matplotlib.colors import Normalize, LinearSegmentedColormap
    cmap = LinearSegmentedColormap.from_list("xw", [ACCENT, GOLD])
    norm = Normalize(vmin=0.300, vmax=0.360)
    sm = ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, shrink=0.6, pad=0.02)
    cbar.set_label("Projected xwOBA", fontsize=10)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    add_watermark(fig)
    fig.savefig(OUT / "breakout-composite.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("  Saved breakout-composite.png")


# ── VIZ 6: Thumbnail ────────────────────────────────────────────────────
def viz_thumbnail():
    print("Creating thumbnail...")

    fig, ax = plt.subplots(figsize=(12, 6.3))  # ~1200x630 for social
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    # Title
    ax.text(0.5, 0.65, "10 HITTER BREAKOUT", fontsize=42, fontweight="bold",
            ha="center", va="center", color=TEXT,
            path_effects=[pe.withStroke(linewidth=3, foreground=BG)])
    ax.text(0.5, 0.45, "PICKS FOR 2026", fontsize=42, fontweight="bold",
            ha="center", va="center", color=GOLD,
            path_effects=[pe.withStroke(linewidth=3, foreground=BG)])

    ax.text(0.5, 0.25, "Based on Swing Profiles, Plate Discipline & Contact Quality",
            fontsize=14, ha="center", va="center", color=MUTED)

    ax.text(0.5, 0.12, "basenerd.com", fontsize=16, ha="center", va="center",
            color=ACCENT, fontweight="bold")

    # Decorative line
    ax.plot([0.2, 0.8], [0.35, 0.35], color=ACCENT, linewidth=2, alpha=0.6)

    fig.savefig(OUT / "breakout-thumbnail.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("  Saved breakout-thumbnail.png")


# ── Main ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Generating breakout article visualizations...\n")
    viz_mayo()
    viz_manzardo()
    viz_busch_yoy()
    viz_langford()
    viz_composite()
    viz_thumbnail()
    print(f"\nAll visualizations saved to {OUT}")
