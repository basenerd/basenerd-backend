"""Generate a styled table graphic for the Stolen Base Break-Even Matrix."""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.image as mpimg
import numpy as np

# Results from 20k sim run (no stealing home, with double steals + lead steals 3rd)
DATA = [
    ("1--", "1st steals 2nd",    76.9, 69.9, 67.9),
    ("-2-", "2nd steals 3rd",    76.9, 74.6, 94.7),
    ("12-", "Lead steals 3rd",   85.0, 83.6, 98.2),
    ("12-", "Double steal",      65.0, 59.3, 78.1),
    ("1-3", "1st steals 2nd",    74.8, 72.6, 79.6),
]


def make_color(val):
    """Green (easy steal) for low %, red (risky) for high %. 100% = don't steal."""
    if val >= 100:
        return "#c0392b"
    norm = (val - 50) / 50  # 0 at 50%, 1 at 100%
    norm = max(0, min(1, norm))
    if norm < 0.5:
        r = norm * 2
        g = 0.75
    else:
        r = 1.0
        g = 0.75 * (1 - (norm - 0.5) * 2)
    return mcolors.to_hex((r, g, 0.15))


def text_color(val):
    return "white" if val >= 88 or val <= 55 else "#1a1a2e"


fig, ax = plt.subplots(figsize=(12, 8))
ax.set_xlim(0, 12)
ax.set_ylim(0, 8)
ax.axis("off")
fig.patch.set_facecolor("#1a1a2e")

# Logo
logo = mpimg.imread("c:/Users/nickl/Documents/basenerd-backend-1/static/basenerd-logo-official.png")
logo_ax = fig.add_axes([0.83, 0.82, 0.13, 0.16], anchor="NE")
logo_ax.imshow(logo)
logo_ax.axis("off")

# Title
ax.text(5.5, 7.45, "STOLEN BASE BREAK-EVEN MATRIX", fontsize=22, fontweight="bold",
        color="white", ha="center", va="center", fontfamily="monospace")
ax.text(5.5, 7.0, "Minimum success rate needed for a +EV steal attempt",
        fontsize=12, color="#8892b0", ha="center", va="center", fontfamily="monospace")

# Column headers
col_x = [1.5, 4.5, 7.0, 8.8, 10.6]
headers = ["Bases", "Steal", "0 Out", "1 Out", "2 Out"]
header_y = 6.35

for x, h in zip(col_x, headers):
    ax.text(x, header_y, h, fontsize=13, fontweight="bold", color="#64ffda",
            ha="center", va="center", fontfamily="monospace")

# Divider line
ax.plot([0.3, 11.7], [6.05, 6.05], color="#64ffda", linewidth=1.5, alpha=0.5)

# Data rows
row_height = 0.92
start_y = 5.4

for i, (bases, steal, v0, v1, v2) in enumerate(DATA):
    y = start_y - i * row_height

    # Row background (alternating)
    if i % 2 == 0:
        rect = plt.Rectangle((0.3, y - 0.38), 11.4, 0.76, facecolor="#1e2a3a",
                              edgecolor="none", alpha=0.6)
        ax.add_patch(rect)

    # Base diamonds
    diamond_x, diamond_y = 1.5, y
    diamond_size = 0.16
    positions = {
        "1": (diamond_x + diamond_size, diamond_y),
        "2": (diamond_x, diamond_y + diamond_size),
        "3": (diamond_x - diamond_size, diamond_y),
    }

    # Draw empty diamond outline
    for pos_key, (dx, dy) in positions.items():
        diamond = plt.Polygon([
            (dx, dy + diamond_size * 0.6),
            (dx + diamond_size * 0.6, dy),
            (dx, dy - diamond_size * 0.6),
            (dx - diamond_size * 0.6, dy),
        ], closed=True, facecolor="none", edgecolor="#4a5568", linewidth=1.2)
        ax.add_patch(diamond)

    # Fill occupied bases
    occupied = []
    if bases[0] == "1":
        occupied.append("1")
    if bases[1] == "2":
        occupied.append("2")
    if bases[2] == "3":
        occupied.append("3")

    for pos_key in occupied:
        dx, dy = positions[pos_key]
        diamond = plt.Polygon([
            (dx, dy + diamond_size * 0.6),
            (dx + diamond_size * 0.6, dy),
            (dx, dy - diamond_size * 0.6),
            (dx - diamond_size * 0.6, dy),
        ], closed=True, facecolor="#64ffda", edgecolor="#64ffda", linewidth=1.2)
        ax.add_patch(diamond)

    # Home plate
    ax.plot(diamond_x, diamond_y - diamond_size * 1.1, marker="^", color="#4a5568",
            markersize=6, markeredgecolor="#4a5568")

    # Steal description
    ax.text(4.5, y, steal, fontsize=12, color="#ccd6f6", ha="center", va="center",
            fontfamily="monospace")

    # Break-even cells
    for j, (val, cx) in enumerate(zip([v0, v1, v2], [7.0, 8.8, 10.6])):
        cell_color = make_color(val)
        cell_w, cell_h = 1.5, 0.65
        rect = plt.Rectangle((cx - cell_w / 2, y - cell_h / 2), cell_w, cell_h,
                              facecolor=cell_color, edgecolor="#1a1a2e",
                              linewidth=2, alpha=0.9)
        ax.add_patch(rect)

        label = "N/A" if val >= 100 else f"{val:.1f}%"
        tc = text_color(val)
        ax.text(cx, y, label, fontsize=14, fontweight="bold", color=tc,
                ha="center", va="center", fontfamily="monospace")

# Legend bar at bottom
ax.text(6, 0.75, "Lower % = easier to justify stealing    |    Higher % = riskier steal",
        fontsize=9, color="#8892b0", ha="center", va="center", fontfamily="monospace")

# Color scale bar
bar_x_start, bar_x_end = 3.5, 8.5
bar_y = 0.3
bar_height = 0.2
n_segments = 60
for s in range(n_segments):
    val = 50 + (50 * s / n_segments)
    c = make_color(val)
    seg_w = (bar_x_end - bar_x_start) / n_segments
    rect = plt.Rectangle((bar_x_start + s * seg_w, bar_y), seg_w, bar_height,
                          facecolor=c, edgecolor="none")
    ax.add_patch(rect)

ax.text(bar_x_start - 0.1, bar_y + bar_height / 2, "50%", fontsize=8,
        color="#8892b0", ha="right", va="center", fontfamily="monospace")
ax.text(bar_x_end + 0.1, bar_y + bar_height / 2, "100%", fontsize=8,
        color="#8892b0", ha="left", va="center", fontfamily="monospace")

out = "c:/Users/nickl/Documents/basenerd-backend-1/reports/stolen_base_breakeven.png"
fig.savefig(out, dpi=200, facecolor="#1a1a2e", bbox_inches="tight", pad_inches=0.3)
print(f"Saved to {out}")
plt.close()
