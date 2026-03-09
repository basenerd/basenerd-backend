"""Generate all visualizations for the Monte Carlo Expected Winner article.
Uses the same dark theme as the stolen base / swing-pitch articles."""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
from matplotlib.colors import LinearSegmentedColormap
import numpy as np

OUT_DIR = "c:/Users/nickl/Documents/basenerd-backend-1/static/articles"

# ============================================
# DARK THEME (matches other articles)
# ============================================
BG = "#1a1a2e"
ROW_ALT = "#1e2a3a"
TEXT = "#ccd6f6"
TEXT_BRIGHT = "white"
MUTED = "#8892b0"
ACCENT = "#64ffda"
GREEN = "#64ffda"
RED = "#ff6b6b"
AMBER = "#ffd93d"
BORDER = "#4a5568"
BLUE = "#3b82f6"
FONT = "monospace"


# ============================================
# 1. FLOW DIAGRAM — How each PA is simulated
# ============================================
def make_flow_diagram():
    fig, ax = plt.subplots(figsize=(10, 6))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 6)
    ax.axis("off")

    # Title
    ax.text(5, 5.65, "MONTE CARLO SIMULATION — PER PLATE APPEARANCE",
            fontsize=13, fontfamily=FONT, fontweight="bold", color=TEXT_BRIGHT,
            ha="center", va="center")

    def box(x, y, w, h, label, sublabel=None, color=ROW_ALT, border=BORDER, text_color=TEXT_BRIGHT):
        rect = mpatches.FancyBboxPatch((x - w/2, y - h/2), w, h,
                                        boxstyle="round,pad=0.12",
                                        facecolor=color, edgecolor=border, linewidth=1.8)
        ax.add_patch(rect)
        if sublabel:
            ax.text(x, y + 0.12, label, ha="center", va="center",
                    fontsize=10, fontfamily=FONT, fontweight="bold", color=text_color)
            ax.text(x, y - 0.18, sublabel, ha="center", va="center",
                    fontsize=8, fontfamily=FONT, color=MUTED)
        else:
            ax.text(x, y, label, ha="center", va="center",
                    fontsize=10, fontfamily=FONT, fontweight="bold", color=text_color)

    def arrow(x1, y1, x2, y2, color=MUTED, label=None, label_offset=(0, 0.15)):
        ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                    arrowprops=dict(arrowstyle="-|>", color=color, lw=2))
        if label:
            mx = (x1 + x2) / 2 + label_offset[0]
            my = (y1 + y2) / 2 + label_offset[1]
            ax.text(mx, my, label, fontsize=8, fontfamily=FONT, color=color,
                    ha="center", va="center", fontweight="bold")

    # Row 1: Plate Appearance
    box(5, 5.0, 2.8, 0.55, "Plate Appearance", "from MLB game feed", color="#1e2a3a", border=ACCENT)

    # Row 2: Classification
    arrow(5, 4.72, 5, 4.28, color=ACCENT)
    box(5, 4.0, 2.0, 0.45, "Classify Event", color="#1e2a3a")

    # Branch: Deterministic (left)
    arrow(4.0, 3.77, 2.2, 3.3, color=MUTED, label="deterministic", label_offset=(-0.3, 0.15))
    box(2.0, 2.9, 2.2, 0.6, "Locked-In Events", color="#1e2a3a", border=BORDER)
    ax.text(2.0, 2.6, "HR  •  K  •  BB  •  HBP", fontsize=8, fontfamily=FONT,
            color=MUTED, ha="center")
    ax.text(2.0, 2.35, "Same result every sim", fontsize=7, fontfamily=FONT,
            color="#5a6578", ha="center", style="italic")

    # Branch: Stochastic (right)
    arrow(6.0, 3.77, 7.8, 3.3, color=ACCENT, label="ball in play", label_offset=(0.3, 0.15))
    box(8.0, 2.9, 2.2, 0.6, "Ball In Play", "xBA / xSLG from Statcast", color="#1e2a3a", border=ACCENT)

    # Step 2: Convert to probabilities
    arrow(8.0, 2.58, 8.0, 2.08, color=ACCENT)
    box(8.0, 1.75, 2.5, 0.55, "Outcome Probabilities",
        "P(1B), P(2B), P(3B), P(HR), P(OUT)", color="#1e2a3a", border=ACCENT)

    # Step 3: Random sample
    arrow(8.0, 1.46, 8.0, 0.96, color=AMBER)
    box(8.0, 0.65, 2.0, 0.5, "Random Sample", "\"roll the dice\"", color="#1e2a3a", border=AMBER)

    # Both converge to: Advance Runners
    arrow(2.0, 2.28, 4.0, 1.18, color=MUTED)
    arrow(7.0, 0.65, 5.7, 0.65, color=AMBER)
    box(5.0, 0.65, 2.2, 0.5, "Advance Runners", "advancement matrix", color="#1e2a3a", border=GREEN)

    # Final: Update Score
    arrow(5.0, 0.38, 5.0, -0.05, color=GREEN)
    box(5.0, -0.35, 1.8, 0.45, "Update Score", color="#1e2a3a", border=GREEN)

    # Repeat arrow
    ax.annotate("", xy=(9.5, 5.0), xytext=(9.5, -0.35),
                arrowprops=dict(arrowstyle="-|>", color="#5a6578", lw=1.5,
                                connectionstyle="arc3,rad=-0.3", linestyle="--"))
    ax.text(9.85, 2.5, "next PA", fontsize=8, fontfamily=FONT, color="#5a6578",
            rotation=90, ha="center", va="center")

    fig.savefig(f"{OUT_DIR}/mc-flow-diagram.png", dpi=180, facecolor=BG,
                bbox_inches="tight", pad_inches=0.3)
    plt.close()
    print("Saved mc-flow-diagram.png")


# ============================================
# 2. ADVANCEMENT MATRIX EXAMPLE
# ============================================
def make_advancement_example():
    fig, ax = plt.subplots(figsize=(10, 5))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 5)
    ax.axis("off")

    ax.text(5, 4.7, "RUNNER ADVANCEMENT EXAMPLE",
            fontsize=14, fontfamily=FONT, fontweight="bold", color=TEXT_BRIGHT,
            ha="center")
    ax.text(5, 4.35, "Single to Center Field  •  Runner on 2nd  •  0 Outs",
            fontsize=10, fontfamily=FONT, color=MUTED, ha="center")

    # Draw diamond helper
    def diamond(ax, cx, cy, size, runners, label_below=None):
        positions = {
            "1B": (cx + size, cy),
            "2B": (cx, cy + size),
            "3B": (cx - size, cy),
        }
        for base, (bx, by) in positions.items():
            d = plt.Polygon([
                (bx, by + size * 0.4),
                (bx + size * 0.4, by),
                (bx, by - size * 0.4),
                (bx - size * 0.4, by),
            ], closed=True, facecolor="none", edgecolor=BORDER, linewidth=2)
            ax.add_patch(d)
        for base in runners:
            bx, by = positions[base]
            d = plt.Polygon([
                (bx, by + size * 0.4),
                (bx + size * 0.4, by),
                (bx, by - size * 0.4),
                (bx - size * 0.4, by),
            ], closed=True, facecolor=ACCENT, edgecolor=ACCENT, linewidth=2)
            ax.add_patch(d)
        ax.plot(cx, cy - size * 0.7, marker="^", color=BORDER, markersize=7)
        if label_below:
            ax.text(cx, cy - size * 1.2, label_below, ha="center", va="center",
                    fontsize=9, fontfamily=FONT, color=MUTED)

    # BEFORE state
    ax.text(1.8, 3.6, "Before", fontsize=12, fontfamily=FONT, fontweight="bold",
            color=TEXT_BRIGHT, ha="center")
    diamond(ax, 1.8, 2.7, 0.5, ["2B"], "Runner on 2nd, 0 out")

    # Arrow
    ax.annotate("", xy=(3.5, 2.7), xytext=(2.7, 2.7),
                arrowprops=dict(arrowstyle="-|>", color=ACCENT, lw=2.5))
    ax.text(3.1, 3.05, "1B to CF", fontsize=9, fontfamily=FONT, fontweight="bold",
            color=ACCENT, ha="center")

    # OUTCOME A: Runner scores (60%)
    ax.text(5.5, 3.6, "Outcome A", fontsize=11, fontfamily=FONT, fontweight="bold",
            color=GREEN, ha="center")
    diamond(ax, 5.5, 2.7, 0.45, ["1B"])
    ax.text(5.5, 1.6, "Runner scores from 2nd", fontsize=9, fontfamily=FONT,
            color=TEXT, ha="center")

    # Probability badge A
    rect_a = mpatches.FancyBboxPatch((4.55, 3.65), 1.9, 0.45,
                                      boxstyle="round,pad=0.08",
                                      facecolor=GREEN, edgecolor=GREEN, alpha=0.2, linewidth=0)
    ax.add_patch(rect_a)
    ax.text(5.5, 3.88, "≈ 60% probability", fontsize=10, fontfamily=FONT,
            fontweight="bold", color=GREEN, ha="center")

    # OUTCOME B: Runner holds at 3rd (40%)
    ax.text(8.5, 3.6, "Outcome B", fontsize=11, fontfamily=FONT, fontweight="bold",
            color=AMBER, ha="center")
    diamond(ax, 8.5, 2.7, 0.45, ["1B", "3B"])
    ax.text(8.5, 1.6, "Runner holds at 3rd", fontsize=9, fontfamily=FONT,
            color=TEXT, ha="center")

    # Probability badge B
    rect_b = mpatches.FancyBboxPatch((7.55, 3.65), 1.9, 0.45,
                                      boxstyle="round,pad=0.08",
                                      facecolor=AMBER, edgecolor=AMBER, alpha=0.2, linewidth=0)
    ax.add_patch(rect_b)
    ax.text(8.5, 3.88, "≈ 40% probability", fontsize=10, fontfamily=FONT,
            fontweight="bold", color=AMBER, ha="center")

    # Bottom note
    ax.text(5, 0.6, "Probabilities from advancement_probs table",
            fontsize=9, fontfamily=FONT, color=MUTED, ha="center", style="italic")
    ax.text(5, 0.25, "Indexed by: event type × base state × outs × spray direction",
            fontsize=9, fontfamily=FONT, color="#5a6578", ha="center")

    fig.savefig(f"{OUT_DIR}/mc-advancement-example.png", dpi=180, facecolor=BG,
                bbox_inches="tight", pad_inches=0.3)
    plt.close()
    print("Saved mc-advancement-example.png")


# ============================================
# 3. WIN GAUGE — Mock of the Analytics tab gauge
# ============================================
def make_win_gauge():
    fig, ax = plt.subplots(figsize=(8, 5))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    ax.set_xlim(-1.8, 1.8)
    ax.set_ylim(-0.5, 1.7)
    ax.set_aspect("equal")
    ax.axis("off")

    ax.text(0, 1.6, "WIN ODDS", fontsize=16, fontfamily=FONT, fontweight="bold",
            color=TEXT_BRIGHT, ha="center")
    ax.text(0, 1.38, "After 1,200 sims", fontsize=10, fontfamily=FONT,
            color=MUTED, ha="center")

    # Draw arc background
    theta = np.linspace(np.pi, 0, 200)
    r = 1.2
    xb = r * np.cos(theta)
    yb = r * np.sin(theta)
    ax.plot(xb, yb, color="#2a3a4a", linewidth=20, solid_capstyle="round", zorder=1)

    # Away arc (left side, blue) — 61%
    away_pct = 0.61
    theta_away = np.linspace(np.pi, np.pi * (1 - away_pct), 100)
    xa = r * np.cos(theta_away)
    ya = r * np.sin(theta_away)
    ax.plot(xa, ya, color=BLUE, linewidth=20, solid_capstyle="round", zorder=2, alpha=0.85)

    # Home arc (right side, red) — 39%
    home_pct = 0.39
    theta_home = np.linspace(0, np.pi * home_pct, 100)
    xh = r * np.cos(theta_home)
    yh = r * np.sin(theta_home)
    ax.plot(xh, yh, color=RED, linewidth=20, solid_capstyle="round", zorder=2, alpha=0.85)

    # Needle
    needle_angle = np.pi * (1 - away_pct)
    nx = 1.05 * np.cos(needle_angle)
    ny = 1.05 * np.sin(needle_angle)
    ax.plot([0, nx], [0, ny], color="white", linewidth=2.5, solid_capstyle="round", zorder=3)
    ax.plot(0, 0, "o", color="white", markersize=8, zorder=4)

    # Labels
    ax.text(-1.55, -0.2, "NYY", fontsize=12, fontfamily=FONT, fontweight="bold",
            color=BLUE, ha="center")
    ax.text(-1.55, -0.42, "61.0%", fontsize=11, fontfamily=FONT, color=MUTED, ha="center")
    ax.text(1.55, -0.2, "BOS", fontsize=12, fontfamily=FONT, fontweight="bold",
            color=RED, ha="center")
    ax.text(1.55, -0.42, "39.0%", fontsize=11, fontfamily=FONT, color=MUTED, ha="center")

    # Score context
    rect = mpatches.FancyBboxPatch((-1.3, -0.95), 2.6, 0.45,
                                    boxstyle="round,pad=0.1",
                                    facecolor=ROW_ALT, edgecolor=BORDER, linewidth=1.2)
    ax.add_patch(rect)
    ax.text(0, -0.72, "Expected Score:  NYY 4.1  —  BOS 3.3    |    Actual:  NYY 2  —  BOS 3",
            fontsize=8.5, fontfamily=FONT, color=MUTED, ha="center")

    fig.savefig(f"{OUT_DIR}/mc-win-gauge.png", dpi=180, facecolor=BG,
                bbox_inches="tight", pad_inches=0.3)
    plt.close()
    print("Saved mc-win-gauge.png")


# ============================================
# 4. THUMBNAIL (1200x630)
# ============================================
def make_thumbnail():
    fig, ax = plt.subplots(figsize=(12, 6.3))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 6.3)
    ax.axis("off")

    # Label
    ax.text(6, 5.5, "BASENERD RESEARCH", fontsize=11, fontfamily=FONT,
            fontweight="bold", color=ACCENT, ha="center")

    # Title
    ax.text(6, 4.4, "Who Should Have\nWon the Game?", fontsize=36, fontfamily=FONT,
            fontweight="bold", color=TEXT_BRIGHT, ha="center", va="center",
            linespacing=1.15)

    # Subtitle
    ax.text(6, 3.1, "Monte Carlo Simulation  •  Expected Winner  •  Analytics Tab",
            fontsize=12, fontfamily=FONT, color=MUTED, ha="center")

    # Mini gauge illustration
    theta = np.linspace(np.pi, 0, 200)
    r_gauge = 0.8
    cx_g, cy_g = 6, 1.5
    xb = cx_g + r_gauge * np.cos(theta)
    yb = cy_g + r_gauge * np.sin(theta)
    ax.plot(xb, yb, color="#2a3a4a", linewidth=14, solid_capstyle="round", zorder=1)

    # Away arc (61%)
    theta_a = np.linspace(np.pi, np.pi * 0.39, 80)
    xa = cx_g + r_gauge * np.cos(theta_a)
    ya = cy_g + r_gauge * np.sin(theta_a)
    ax.plot(xa, ya, color=BLUE, linewidth=14, solid_capstyle="round", zorder=2, alpha=0.85)

    # Home arc (39%)
    theta_h = np.linspace(0, np.pi * 0.39, 80)
    xh = cx_g + r_gauge * np.cos(theta_h)
    yh = cy_g + r_gauge * np.sin(theta_h)
    ax.plot(xh, yh, color=RED, linewidth=14, solid_capstyle="round", zorder=2, alpha=0.85)

    # Needle
    n_angle = np.pi * 0.39
    nx = cx_g + 0.65 * np.cos(n_angle)
    ny = cy_g + 0.65 * np.sin(n_angle)
    ax.plot([cx_g, nx], [cy_g, ny], color="white", linewidth=2, solid_capstyle="round", zorder=3)
    ax.plot(cx_g, cy_g, "o", color="white", markersize=5, zorder=4)

    ax.text(cx_g - 1.2, cy_g - 0.3, "61%", fontsize=12, fontfamily=FONT,
            fontweight="bold", color=BLUE, ha="center")
    ax.text(cx_g + 1.2, cy_g - 0.3, "39%", fontsize=12, fontfamily=FONT,
            fontweight="bold", color=RED, ha="center")

    fig.savefig(f"{OUT_DIR}/mc-thumbnail.png", dpi=100, facecolor=BG,
                bbox_inches="tight", pad_inches=0.2)
    plt.close()
    print("Saved mc-thumbnail.png")


if __name__ == "__main__":
    import os
    os.makedirs(OUT_DIR, exist_ok=True)
    make_flow_diagram()
    make_advancement_example()
    make_win_gauge()
    make_thumbnail()
    print("\nAll Monte Carlo article visuals generated.")
