"""Generate all visualizations for the Stolen Base Break-Even article.
Uses the same dark theme as the main break-even matrix graphic."""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
import matplotlib.image as mpimg
import numpy as np

OUT_DIR = "c:/Users/nickl/Documents/basenerd-backend-1/static/articles"
LOGO_PATH = "c:/Users/nickl/Documents/basenerd-backend-1/static/basenerd-logo-official.png"

# ============================================
# DARK THEME (matches main matrix graphic)
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
FONT = "monospace"


def make_color(val):
    """Green for low %, red for high %. Same as main matrix."""
    if val >= 100:
        return "#c0392b"
    norm = (val - 50) / 50
    norm = max(0, min(1, norm))
    if norm < 0.5:
        r = norm * 2
        g = 0.75
    else:
        r = 1.0
        g = 0.75 * (1 - (norm - 0.5) * 2)
    return mcolors.to_hex((r, g, 0.15))


# ============================================
# 1. RUN EXPECTANCY MATRIX HEATMAP
# ============================================
def make_re_heatmap():
    labels = ["---", "1--", "-2-", "12-", "--3", "1-3", "-23", "123"]
    re = np.array([
        [0.476, 0.262, 0.105],
        [0.829, 0.487, 0.219],
        [1.000, 0.652, 0.329],
        [1.378, 0.882, 0.451],
        [1.222, 0.852, 0.347],
        [1.535, 0.994, 0.463],
        [1.793, 1.263, 0.578],
        [2.206, 1.507, 0.774],
    ])

    fig, ax = plt.subplots(figsize=(7, 6.5))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    # Custom dark colormap: deep blue -> teal -> amber -> red
    from matplotlib.colors import LinearSegmentedColormap
    colors_list = ["#1e2a3a", "#1a5c4c", "#2d8a6e", "#64ffda", "#ffd93d", "#ff6b6b", "#c0392b"]
    cmap = LinearSegmentedColormap.from_list("dark_re", colors_list, N=256)

    im = ax.imshow(re, cmap=cmap, aspect="auto", vmin=0, vmax=2.3)

    ax.set_xticks([0, 1, 2])
    ax.set_xticklabels(["0 Outs", "1 Out", "2 Outs"], fontsize=12, fontfamily=FONT,
                        color=ACCENT, fontweight="bold")
    ax.set_yticks(range(8))
    ax.set_yticklabels(labels, fontsize=12, fontfamily=FONT, fontweight="bold", color=TEXT)

    ax.tick_params(top=True, bottom=False, labeltop=True, labelbottom=False, length=0)

    for i in range(8):
        for j in range(3):
            val = re[i, j]
            color = BG if val > 1.0 else TEXT_BRIGHT
            ax.text(j, i, f"{val:.3f}", ha="center", va="center",
                    fontsize=13, fontfamily=FONT, fontweight="bold", color=color)

    # Grid lines in background color
    for i in range(9):
        ax.axhline(i - 0.5, color=BG, linewidth=3)
    for j in range(4):
        ax.axvline(j - 0.5, color=BG, linewidth=3)

    ax.set_title("RUN EXPECTANCY MATRIX\nExpected Runs for Rest of Inning",
                 fontsize=15, fontfamily=FONT, fontweight="bold", color=TEXT_BRIGHT, pad=30)

    cbar = fig.colorbar(im, ax=ax, shrink=0.8, pad=0.04)
    cbar.set_label("Expected Runs", fontsize=10, fontfamily=FONT, color=MUTED)
    cbar.ax.tick_params(labelsize=9, colors=MUTED)
    cbar.outline.set_edgecolor(BORDER)

    plt.tight_layout()
    fig.savefig(f"{OUT_DIR}/sb-re-matrix.png", dpi=180, facecolor=BG,
                bbox_inches="tight", pad_inches=0.3)
    plt.close()
    print("Saved sb-re-matrix.png")


# ============================================
# 2. STEAL SCENARIO DIAGRAM
# ============================================
def draw_diamond(ax, cx, cy, size, runners, label_below=None, label_above=None):
    positions = {
        "1B": (cx + size, cy),
        "2B": (cx, cy + size),
        "3B": (cx - size, cy),
    }

    for base, (bx, by) in positions.items():
        diamond = plt.Polygon([
            (bx, by + size * 0.45),
            (bx + size * 0.45, by),
            (bx, by - size * 0.45),
            (bx - size * 0.45, by),
        ], closed=True, facecolor="none", edgecolor=BORDER, linewidth=2)
        ax.add_patch(diamond)

    for base in runners:
        bx, by = positions[base]
        diamond = plt.Polygon([
            (bx, by + size * 0.45),
            (bx + size * 0.45, by),
            (bx, by - size * 0.45),
            (bx - size * 0.45, by),
        ], closed=True, facecolor=ACCENT, edgecolor=ACCENT, linewidth=2)
        ax.add_patch(diamond)

    ax.plot(cx, cy - size * 0.8, marker="^", color=BORDER, markersize=8,
            markeredgecolor=BORDER)

    if label_above:
        ax.text(cx, cy + size * 1.6, label_above, ha="center", va="center",
                fontsize=12, fontfamily=FONT, fontweight="bold", color=TEXT_BRIGHT)
    if label_below:
        ax.text(cx, cy - size * 1.4, label_below, ha="center", va="center",
                fontsize=11, fontfamily=FONT, color=MUTED)


def make_scenario_diagram():
    fig, ax = plt.subplots(figsize=(10, 4.5))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 4.5)
    ax.axis("off")

    ax.text(5, 4.15, "EXAMPLE: RUNNER ON 1ST, 0 OUTS — STEAL OF 2ND",
            fontsize=14, fontfamily=FONT, fontweight="bold", color=TEXT_BRIGHT,
            ha="center", va="center")

    # BEFORE
    draw_diamond(ax, 1.8, 2.0, 0.55, ["1B"],
                 label_above="Before", label_below="RE = 0.829")

    # Arrow to SUCCESS
    ax.annotate("", xy=(4.2, 2.8), xytext=(3.0, 2.5),
                arrowprops=dict(arrowstyle="-|>", color=GREEN, lw=2.5))
    ax.text(3.4, 3.15, "Safe", fontsize=11, fontfamily=FONT, fontweight="bold",
            color=GREEN, ha="center")

    # Arrow to FAILURE
    ax.annotate("", xy=(4.2, 1.2), xytext=(3.0, 1.5),
                arrowprops=dict(arrowstyle="-|>", color=RED, lw=2.5))
    ax.text(3.4, 0.85, "Out", fontsize=11, fontfamily=FONT, fontweight="bold",
            color=RED, ha="center")

    # SUCCESS diamond
    draw_diamond(ax, 5.5, 2.8, 0.50, ["2B"],
                 label_above="Success", label_below="RE = 1.000")

    # FAILURE diamond
    draw_diamond(ax, 5.5, 1.0, 0.50, [],
                 label_above="Caught", label_below="RE = 0.262 (1 out)")

    # Formula box
    box_x = 8.2
    rect = mpatches.FancyBboxPatch((7.0, 0.5), 2.5, 3.2,
                                    boxstyle="round,pad=0.15",
                                    facecolor=ROW_ALT, edgecolor=BORDER, linewidth=1.5)
    ax.add_patch(rect)

    ax.text(box_x, 3.35, "Break-Even", fontsize=11, fontfamily=FONT,
            fontweight="bold", color=ACCENT, ha="center")
    ax.text(box_x, 2.85, "0.829 - 0.262", fontsize=10, fontfamily=FONT,
            color=MUTED, ha="center")
    ax.plot([7.35, 9.05], [2.55, 2.55], color=TEXT, linewidth=1.5)
    ax.text(box_x, 2.2, "1.000 - 0.262", fontsize=10, fontfamily=FONT,
            color=MUTED, ha="center")
    ax.text(box_x, 1.6, "=", fontsize=14, fontfamily=FONT, fontweight="bold",
            color=TEXT, ha="center")
    ax.text(box_x, 1.05, "76.9%", fontsize=20, fontfamily=FONT, fontweight="bold",
            color=ACCENT, ha="center")

    fig.savefig(f"{OUT_DIR}/sb-scenario-example.png", dpi=180, facecolor=BG,
                bbox_inches="tight", pad_inches=0.2)
    plt.close()
    print("Saved sb-scenario-example.png")


# ============================================
# 3. BREAK-EVEN BAR CHART
# ============================================
def make_bar_chart():
    scenarios = [
        ("1st steals\n2nd", [76.9, 69.9, 67.9]),
        ("2nd steals\n3rd", [76.9, 74.6, 94.7]),
        ("Lead steals\n3rd (12-)", [85.0, 83.6, 98.2]),
        ("Double steal\n(12-)", [65.0, 59.3, 78.1]),
        ("1st steals\n2nd (1-3)", [74.8, 72.6, 79.6]),
    ]

    labels = [s[0] for s in scenarios]
    vals_0 = [s[1][0] for s in scenarios]
    vals_1 = [s[1][1] for s in scenarios]
    vals_2 = [s[1][2] for s in scenarios]

    x = np.arange(len(labels))
    width = 0.25

    fig, ax = plt.subplots(figsize=(10, 5.5))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    bars0 = ax.bar(x - width, vals_0, width, label="0 Outs", color=ACCENT, alpha=0.9)
    bars1 = ax.bar(x, vals_1, width, label="1 Out", color=AMBER, alpha=0.85)
    bars2 = ax.bar(x + width, vals_2, width, label="2 Outs", color=RED, alpha=0.8)

    ax.set_ylabel("Break-Even Success Rate (%)", fontsize=11, fontfamily=FONT, color=MUTED)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10, fontfamily=FONT, color=TEXT)
    ax.set_ylim(50, 108)
    ax.set_yticks([50, 60, 70, 80, 90, 100])

    # Reference line at 70%
    ax.axhline(70, color=MUTED, linestyle="--", linewidth=1, alpha=0.5)
    ax.text(len(labels) - 0.5, 71.2, "70% rule of thumb", fontsize=9,
            fontfamily=FONT, color=MUTED, ha="right", style="italic")

    # Value labels
    for bars in [bars0, bars1, bars2]:
        for bar in bars:
            h = bar.get_height()
            label = "N/A" if h >= 99.5 else f"{h:.0f}%"
            ax.text(bar.get_x() + bar.get_width() / 2, h + 0.8, label,
                    ha="center", va="bottom", fontsize=8, fontfamily=FONT,
                    color=TEXT_BRIGHT, fontweight="bold")

    legend = ax.legend(fontsize=10, loc="upper left", facecolor=ROW_ALT,
                       edgecolor=BORDER, labelcolor=TEXT)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(BORDER)
    ax.spines["bottom"].set_color(BORDER)
    ax.tick_params(colors=BORDER, labelcolor=MUTED)
    ax.yaxis.label.set_color(MUTED)

    ax.set_title("BREAK-EVEN STEAL SUCCESS RATE BY SCENARIO AND OUTS",
                 fontsize=13, fontfamily=FONT, fontweight="bold", color=TEXT_BRIGHT, pad=14)

    plt.tight_layout()
    fig.savefig(f"{OUT_DIR}/sb-breakeven-bars.png", dpi=180, facecolor=BG,
                bbox_inches="tight", pad_inches=0.3)
    plt.close()
    print("Saved sb-breakeven-bars.png")


# ============================================
# 4. DOUBLE STEAL VS LEAD STEAL
# ============================================
def make_double_vs_lead():
    fig, ax = plt.subplots(figsize=(8, 4.5))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    outs = ["0 Outs", "1 Out", "2 Outs"]
    lead = [85.0, 83.6, 98.2]
    double = [65.0, 59.3, 78.1]

    x = np.arange(3)
    width = 0.32

    b1 = ax.bar(x - width / 2, lead, width, label="Lead Runner Only",
                color=BORDER, edgecolor="#5a6578", alpha=0.8)
    b2 = ax.bar(x + width / 2, double, width, label="Double Steal",
                color=ACCENT, edgecolor=ACCENT, alpha=0.9)

    ax.set_xticks(x)
    ax.set_xticklabels(outs, fontsize=11, fontfamily=FONT, color=TEXT)
    ax.set_ylabel("Break-Even %", fontsize=11, fontfamily=FONT, color=MUTED)
    ax.set_ylim(45, 108)

    for bars, c in [(b1, MUTED), (b2, TEXT_BRIGHT)]:
        for bar in bars:
            h = bar.get_height()
            label = "N/A" if h >= 99.5 else f"{h:.0f}%"
            ax.text(bar.get_x() + bar.get_width() / 2, h + 1.2, label,
                    ha="center", va="bottom", fontsize=11, fontfamily=FONT,
                    color=c, fontweight="bold")

    # Difference annotations
    for i in range(3):
        diff = lead[i] - double[i]
        mid = (lead[i] + double[i]) / 2
        if diff > 0 and lead[i] < 99:
            ax.annotate(f"-{diff:.0f}pp", xy=(i + width / 2, double[i] + 0.5),
                        xytext=(i + 0.55, mid), fontsize=10, fontfamily=FONT,
                        color=GREEN, fontweight="bold", ha="left",
                        arrowprops=dict(arrowstyle="->", color=GREEN, lw=1.5))

    legend = ax.legend(fontsize=10, loc="upper left", facecolor=ROW_ALT,
                       edgecolor=BORDER, labelcolor=TEXT)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(BORDER)
    ax.spines["bottom"].set_color(BORDER)
    ax.tick_params(colors=BORDER, labelcolor=MUTED)

    ax.set_title("RUNNERS ON 1ST & 2ND: LEAD STEAL VS. DOUBLE STEAL",
                 fontsize=13, fontfamily=FONT, fontweight="bold", color=TEXT_BRIGHT, pad=12)

    plt.tight_layout()
    fig.savefig(f"{OUT_DIR}/sb-double-vs-lead.png", dpi=180, facecolor=BG,
                bbox_inches="tight", pad_inches=0.3)
    plt.close()
    print("Saved sb-double-vs-lead.png")


if __name__ == "__main__":
    import os
    os.makedirs(OUT_DIR, exist_ok=True)
    make_re_heatmap()
    make_scenario_diagram()
    make_bar_chart()
    make_double_vs_lead()
    print("\nAll article visuals generated.")
