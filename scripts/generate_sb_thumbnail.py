"""Generate a thumbnail image for the Stolen Base Break-Even article."""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
import matplotlib.image as mpimg
import numpy as np

# Theme (matches article visuals)
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

LOGO_PATH = "c:/Users/nickl/Documents/basenerd-backend-1/static/basenerd-logo-official.png"
OUT_PATH = "c:/Users/nickl/Documents/basenerd-backend-1/static/articles/sb-thumbnail.png"


def draw_base_diamond(ax, cx, cy, size, occupied, arrow_from=None, arrow_to=None):
    """Draw a baseball diamond with optional occupied bases and steal arrow."""
    positions = {
        "1B": (cx + size, cy),
        "2B": (cx, cy + size),
        "3B": (cx - size, cy),
    }
    # Home plate
    ax.plot(cx, cy - size * 0.85, marker="^", color=BORDER, markersize=10,
            markeredgecolor=BORDER)

    for base, (bx, by) in positions.items():
        filled = base in occupied
        fc = ACCENT if filled else "none"
        ec = ACCENT if filled else BORDER
        diamond = plt.Polygon([
            (bx, by + size * 0.5),
            (bx + size * 0.5, by),
            (bx, by - size * 0.5),
            (bx - size * 0.5, by),
        ], closed=True, facecolor=fc, edgecolor=ec, linewidth=2.5)
        ax.add_patch(diamond)

    # Steal arrow
    if arrow_from and arrow_to:
        fx, fy = positions[arrow_from]
        tx, ty = positions[arrow_to]
        ax.annotate("", xy=(tx, ty), xytext=(fx, fy),
                    arrowprops=dict(arrowstyle="-|>", color=AMBER, lw=3.5,
                                    connectionstyle="arc3,rad=0.3"))


def make_thumbnail():
    # 1200x630 is standard social/article thumbnail ratio
    fig, ax = plt.subplots(figsize=(12, 6.3))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 6.3)
    ax.axis("off")

    # --- Left side: diamond visual with steal arrow ---
    draw_base_diamond(ax, 2.8, 3.0, 0.9, ["1B"], arrow_from="1B", arrow_to="2B")

    # Speed lines around the arrow for motion effect
    for offset in [0.15, 0.30, 0.45]:
        ax.plot([3.5 + offset, 3.3 + offset], [3.55 + offset * 0.5, 3.65 + offset * 0.5],
                color=AMBER, alpha=0.4 - offset * 0.6, linewidth=1.5)

    # --- Right side: title and key stat ---
    # Title
    ax.text(7.8, 5.1, "WHEN IS A", fontsize=26, fontweight="bold",
            color=MUTED, ha="center", va="center", fontfamily=FONT)
    ax.text(7.8, 4.2, "STOLEN BASE", fontsize=34, fontweight="bold",
            color=TEXT_BRIGHT, ha="center", va="center", fontfamily=FONT)
    ax.text(7.8, 3.3, "WORTH THE RISK?", fontsize=26, fontweight="bold",
            color=MUTED, ha="center", va="center", fontfamily=FONT)

    # Accent line under title
    ax.plot([5.5, 10.1], [2.7, 2.7], color=ACCENT, linewidth=2.5, alpha=0.7)

    # Key stat callout
    ax.text(7.8, 2.05, "A Monte Carlo Approach to Steal Break-Evens",
            fontsize=11, color=ACCENT, ha="center", va="center", fontfamily=FONT,
            style="italic")

    # Bottom row: mini break-even numbers as a teaser
    scenarios = [
        ("1st\u21922nd", "76.9%"),
        ("2nd\u21923rd", "76.9%"),
        ("Dbl Steal", "65.0%"),
    ]
    box_y = 0.95
    box_w = 1.9
    box_h = 0.7
    start_x = 5.0
    gap = 2.2

    for i, (label, val) in enumerate(scenarios):
        bx = start_x + i * gap
        rect = mpatches.FancyBboxPatch(
            (bx - box_w / 2, box_y - box_h / 2), box_w, box_h,
            boxstyle="round,pad=0.1", facecolor=ROW_ALT, edgecolor=BORDER,
            linewidth=1.5)
        ax.add_patch(rect)
        ax.text(bx, box_y + 0.12, val, fontsize=14, fontweight="bold",
                color=ACCENT, ha="center", va="center", fontfamily=FONT)
        ax.text(bx, box_y - 0.18, label, fontsize=8, color=MUTED,
                ha="center", va="center", fontfamily=FONT)

    # Label the boxes
    ax.text(start_x + gap, 0.25, "0 Out Break-Evens", fontsize=8,
            color=MUTED, ha="center", va="center", fontfamily=FONT, alpha=0.7)

    # Logo (top-left)
    try:
        logo = mpimg.imread(LOGO_PATH)
        logo_ax = fig.add_axes([0.01, 0.78, 0.12, 0.2], anchor="NW")
        logo_ax.imshow(logo)
        logo_ax.axis("off")
    except Exception:
        pass

    # "basenerd.com" watermark bottom-left
    ax.text(0.3, 0.15, "basenerd.com", fontsize=9, color=MUTED, alpha=0.5,
            fontfamily=FONT, va="center")

    fig.savefig(OUT_PATH, dpi=180, facecolor=BG, bbox_inches="tight", pad_inches=0.15)
    plt.close()
    print(f"Saved thumbnail to {OUT_PATH}")


if __name__ == "__main__":
    make_thumbnail()
