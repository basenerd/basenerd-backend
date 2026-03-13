"""
HR Graphic Generator
====================
Generates shareable home-run images with stadium overlay,
trajectory visualization, and parks HR count.

Output: 1200 x 675 px PNG (16:9, optimized for Twitter/X).
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
import matplotlib.image as mpimg
import numpy as np
import math
import os
from io import BytesIO

from services.hr_park_calc import (
    _eval_fence_distance,
    _eval_fence_height,
    _STADIUMS,
    stadiums_hr_count,
    _simulate_trajectory,
)

# ── colours (basenerd dark theme) ──────────────────────────────
BG          = "#0b1220"
FIELD       = "#0f2847"
FENCE       = "#ffffff"
TRAJ        = "#ef4444"
TRAJ_GLOW   = "#ff6b6b"
GOLD        = "#fbbf24"
CYAN        = "#38bdf8"
GREEN       = "#22c55e"
RED_BAD     = "#ef4444"
TEXT        = "#f1f5f9"
TEXT2       = "#94a3b8"
MUTED       = "#64748b"
BORDER      = "#1e3a5f"
WHITE       = "#ffffff"

W, H = 12.0, 6.75  # inches
DPI = 100           # → 1200 x 675 px

_LOGO_DIR = os.path.join(os.path.dirname(__file__), "..", "static", "team_logos")
_logo_cache = {}

def _load_team_logo(team_code):
    """Load and cache a team logo PNG as an RGBA numpy array."""
    if team_code in _logo_cache:
        return _logo_cache[team_code]
    path = os.path.join(_LOGO_DIR, f"{team_code}.png")
    if not os.path.exists(path):
        _logo_cache[team_code] = None
        return None
    img = mpimg.imread(path)
    # ensure RGBA float 0-1
    if img.dtype == np.uint8:
        img = img.astype(np.float32) / 255.0
    if img.ndim == 2:
        img = np.stack([img]*3 + [np.ones_like(img)], axis=-1)
    elif img.shape[2] == 3:
        img = np.concatenate([img, np.ones((*img.shape[:2], 1))], axis=-1)
    _logo_cache[team_code] = img
    return img


def _logo_with_halo(img, is_hr):
    """Return the original logo unchanged (halo is drawn separately)."""
    out = img.copy()
    if not is_hr:
        # dim non-HR logos slightly so they recede
        out[:, :, 3] = out[:, :, 3] * 0.55
    return out


# ── helpers ────────────────────────────────────────────────────

def _fence_xy(stadium_code):
    """Return fence outline as (xs, ys) arrays in feet from home."""
    info = _STADIUMS.get(stadium_code)
    if not info:
        return None, None
    xs, ys = [], []
    for t10 in range(2, 899):
        theta = t10 / 10.0
        try:
            r = _eval_fence_distance(info["dist"], theta)
        except Exception:
            continue
        phi = math.radians(theta - 45)
        xs.append(r * math.sin(phi))
        ys.append(r * math.cos(phi))
    return np.array(xs), np.array(ys)


def _shadow(lw=3):
    """Dark stroke behind light text for readability."""
    return [pe.withStroke(linewidth=lw, foreground=BG)]


def _badge(ax, x, y, label, value, color=GOLD, size=13, label_size=6.5):
    """Draw a floating metric badge (rounded rect) in axes-fraction coords."""
    from matplotlib.patches import FancyBboxPatch
    bw, bh = 0.12, 0.55
    box = FancyBboxPatch(
        (x - bw / 2, y - bh / 2), bw, bh,
        boxstyle="round,pad=0.02", transform=ax.transAxes,
        facecolor=color, edgecolor="none", alpha=0.92,
        clip_on=False, zorder=10,
    )
    ax.add_patch(box)
    ax.text(
        x, y + 0.12, label, transform=ax.transAxes, ha="center",
        va="bottom", fontsize=label_size, fontweight="bold",
        color="#00000088", zorder=11,
    )
    ax.text(
        x, y - 0.04, value, transform=ax.transAxes, ha="center",
        va="top", fontsize=size, fontweight="bold", color="#000000",
        zorder=11,
    )


# ── main generator ─────────────────────────────────────────────

def generate_hr_image(
    batter_name: str,
    exit_velo: float,
    launch_angle: float,
    spray_angle: float,
    distance: float = None,
    stadium_code: str = None,
    game_date: str = None,
    inning_text: str = None,
    away_team: str = None,
    home_team: str = None,
    away_score: int = None,
    home_score: int = None,
    batter_team: str = None,
) -> bytes:
    """Return PNG bytes of a shareable HR graphic."""

    stad = stadium_code or "NYY"
    info = _STADIUMS.get(stad, _STADIUMS["NYY"])

    # ── data ───────────────────────────────────────────────
    parks = stadiums_hr_count(exit_velo, launch_angle, spray_angle)
    theta = max(0.1, min(89.9, 45.0 + spray_angle))
    fence_dist = _eval_fence_distance(info["dist"], theta)
    fence_ht = _eval_fence_height(info["height"], theta)

    traj = _simulate_trajectory(
        exit_velo, launch_angle, info["elevation"], info["temp"],
    )
    traj_d = np.array([p[0] for p in traj])
    traj_h = np.array([p[1] for p in traj])
    max_ht = float(np.max(traj_h)) if len(traj_h) else 0
    est_dist = distance or (float(traj_d[-1]) if len(traj_d) else 0)

    spray_rad = math.radians(spray_angle)
    land_x = est_dist * math.sin(spray_rad)
    land_y = est_dist * math.cos(spray_rad)
    fence_cx = fence_dist * math.sin(spray_rad)
    fence_cy = fence_dist * math.cos(spray_rad)

    fx, fy = _fence_xy(stad)

    # ── figure ─────────────────────────────────────────────
    fig = plt.figure(figsize=(W, H), facecolor=BG)

    # Main stadium axes (left 60%) — raised to make room for badges below
    ax = fig.add_axes([0.02, 0.18, 0.58, 0.73])
    ax.set_facecolor(BG)
    ax.set_aspect("equal")
    ax.axis("off")

    # Side-view axes (right side, upper)
    ax_s = fig.add_axes([0.64, 0.52, 0.33, 0.28])
    ax_s.set_facecolor("#0a1830")

    # ── draw stadium ───────────────────────────────────────
    if fx is not None:
        # outfield fill
        poly_x = np.concatenate([[0], fx, [0]])
        poly_y = np.concatenate([[0], fy, [0]])
        ax.fill(poly_x, poly_y, color=FIELD, alpha=0.65, zorder=1)
        # fence line
        ax.plot(fx, fy, color=FENCE, lw=2.5, alpha=0.85, zorder=3,
                solid_capstyle="round")

    # foul lines
    fl = 370
    for sign in (-1, 1):
        ax.plot(
            [0, sign * fl * math.sin(math.pi / 4)],
            [0, fl * math.cos(math.pi / 4)],
            color=WHITE, lw=0.8, alpha=0.2, zorder=2,
        )

    # infield diamond
    b = 90 / math.sqrt(2)
    diamond = np.array([[0,0],[b,b],[0,2*b],[-b,b],[0,0]])
    ax.plot(diamond[:, 0], diamond[:, 1], color=WHITE, lw=0.8,
            alpha=0.2, zorder=2)

    # home plate
    ax.plot(0, 0, "o", color=WHITE, ms=4, zorder=5, alpha=0.7)

    # ── trajectory line (gradient fade-in) ─────────────────
    n = 50
    for i in range(n):
        f0, f1 = i / n, (i + 1) / n
        ax.plot(
            [f0 * land_x, f1 * land_x],
            [f0 * land_y, f1 * land_y],
            color=TRAJ, lw=2.5 + f1 * 2, alpha=0.25 + 0.7 * f1,
            solid_capstyle="round", zorder=6,
        )
    # glow
    ax.plot([0, land_x], [0, land_y], color=TRAJ_GLOW, lw=9,
            alpha=0.12, zorder=5)

    # fence-crossing marker
    ax.plot(fence_cx, fence_cy, "D", color=WHITE, ms=5, zorder=7,
            markeredgecolor=CYAN, markeredgewidth=1.5, alpha=0.9)

    # landing dot
    ax.plot(land_x, land_y, "o", color=GOLD, ms=11, zorder=8,
            markeredgecolor=WHITE, markeredgewidth=2)

    # ── stadium view limits ────────────────────────────────
    if fx is not None:
        pad = 55
        ax.set_xlim(min(fx.min(), land_x) - pad,
                     max(fx.max(), land_x) + pad)
        ax.set_ylim(-25, max(fy.max(), land_y) + pad)

    # ── metric badges below stadium ─────────────────────────
    badge_ax = fig.add_axes([0.02, 0.02, 0.58, 0.14])
    badge_ax.set_xlim(0, 1)
    badge_ax.set_ylim(0, 1)
    badge_ax.axis("off")
    badge_ax.set_facecolor("none")
    _badge(badge_ax, 0.13, 0.45, "EXIT VELO", f"{exit_velo:.1f}", GOLD, 14)
    _badge(badge_ax, 0.38, 0.45, "LAUNCH", f"{launch_angle:.0f}\u00b0", GOLD, 14)
    _badge(badge_ax, 0.63, 0.45, "DISTANCE", f"{est_dist:.0f} ft", GOLD, 14)
    _badge(badge_ax, 0.88, 0.45, "HEIGHT", f"{max_ht:.0f} ft", GOLD, 14)

    # wall distance label near fence crossing
    wall_fx = ax.transData.transform((fence_cx, fence_cy))
    wall_ax = ax.transAxes.inverted().transform(wall_fx)
    wx, wy = float(wall_ax[0]), float(wall_ax[1])
    if 0.05 < wx < 0.95 and 0.05 < wy < 0.95:
        ax.text(
            wx + 0.04, wy, f"Wall {fence_dist:.0f} ft",
            transform=ax.transAxes, fontsize=8, color=TEXT2,
            fontweight="bold", ha="left", va="center", zorder=9,
            path_effects=_shadow(4),
        )

    # ── side-view trajectory ───────────────────────────────
    ax_s.fill_between(traj_d, 0, traj_h, color=TRAJ, alpha=0.12)
    ax_s.plot(traj_d, traj_h, color=TRAJ, lw=2, alpha=0.9)

    # ground line
    ax_s.axhline(0, color=BORDER, lw=1, alpha=0.5)

    # fence wall on side view
    ax_s.plot(
        [fence_dist, fence_dist], [0, fence_ht],
        color=CYAN, lw=4, alpha=0.7, solid_capstyle="round",
    )
    ax_s.plot(
        [fence_dist - 12, fence_dist + 12], [fence_ht, fence_ht],
        color=CYAN, lw=2, alpha=0.5,
    )
    ax_s.text(
        fence_dist, -8, f"{fence_dist:.0f}ft",
        ha="center", va="top", fontsize=6, color=CYAN, alpha=0.8,
    )

    # max-height marker
    pk = int(np.argmax(traj_h))
    ax_s.plot(traj_d[pk], traj_h[pk], "v", color=GOLD, ms=7, zorder=5)
    ax_s.text(
        traj_d[pk], traj_h[pk] + 6, f"{max_ht:.0f} ft",
        ha="center", va="bottom", fontsize=7, color=GOLD,
        fontweight="bold",
    )

    # landing marker
    ax_s.plot(traj_d[-1], traj_h[-1], "o", color=GOLD, ms=5, zorder=5)

    ax_s.set_xlim(0, max(est_dist * 1.08, 420))
    ax_s.set_ylim(0, max(max_ht * 1.35, 110))
    for sp in ("top", "right"):
        ax_s.spines[sp].set_visible(False)
    for sp in ("bottom", "left"):
        ax_s.spines[sp].set_color(BORDER)
    ax_s.tick_params(colors=MUTED, labelsize=6)
    ax_s.set_xlabel("Distance (ft)", fontsize=7, color=MUTED, labelpad=2)
    ax_s.set_ylabel("Height (ft)", fontsize=7, color=MUTED, labelpad=2)

    # ── PARKS HR count (hero element, right side) ──────────
    pc = parks["count"]
    pt = parks["total"]
    pct = pc / pt if pt else 0
    parks_color = GREEN if pct >= 0.8 else (GOLD if pct >= 0.4 else RED_BAD)

    # "GONE IN" header + big number
    cx = 0.805
    fig.text(cx, 0.44, "GONE IN", ha="center", va="bottom",
             fontsize=11, color=TEXT2, fontweight="bold",
             fontfamily="sans-serif")
    fig.text(cx, 0.30, f"{pc}/{pt}", ha="center", va="bottom",
             fontsize=44, color=parks_color, fontweight="bold",
             fontfamily="sans-serif",
             path_effects=_shadow(5))
    fig.text(cx, 0.28, "BALLPARKS", ha="center", va="top",
             fontsize=9, color=TEXT2, fontweight="bold")

    # team logo grid — 6 columns x 5 rows with green/red tint
    sorted_parks = sorted(parks["parks"], key=lambda p: -p["margin_ft"])
    cols, rows = 6, 5
    logo_size = 0.036  # figure fraction per logo
    gap = 0.004
    grid_w = cols * logo_size + (cols - 1) * gap
    grid_h = rows * logo_size + (rows - 1) * gap
    grid_x0 = cx - grid_w / 2
    grid_y0 = 0.245 - grid_h  # position below BALLPARKS text

    for i, pk in enumerate(sorted_parks):
        if i >= cols * rows:
            break
        col = i % cols
        row = i // cols
        lx = grid_x0 + col * (logo_size + gap)
        ly = grid_y0 + (rows - 1 - row) * (logo_size + gap)
        # halo glow circle behind logo
        halo_color = GREEN if pk["is_hr"] else RED_BAD
        halo_alpha = 0.45 if pk["is_hr"] else 0.35
        cx_logo = lx + logo_size / 2
        cy_logo = ly + logo_size / 2
        halo_r = logo_size * 0.55
        halo = plt.Circle(
            (cx_logo, cy_logo), halo_r, transform=fig.transFigure,
            facecolor=halo_color, edgecolor="none", alpha=halo_alpha,
            clip_on=False, zorder=0,
        )
        fig.add_artist(halo)
        # logo image on top
        logo_img = _load_team_logo(pk["team"])
        if logo_img is not None:
            processed = _logo_with_halo(logo_img, pk["is_hr"])
            logo_ax = fig.add_axes([lx, ly, logo_size, logo_size])
            logo_ax.imshow(processed)
            logo_ax.axis("off")
        else:
            c = GREEN if pk["is_hr"] else "#334155"
            fig.text(cx_logo, cy_logo,
                     pk["team"], fontsize=5, color=c,
                     ha="center", va="center", fontweight="bold",
                     transform=fig.transFigure)

    # ── header ─────────────────────────────────────────────
    # batter's team logo next to name
    header_x = 0.03
    if batter_team:
        bt_logo = _load_team_logo(batter_team)
        if bt_logo is not None:
            logo_ax2 = fig.add_axes([0.02, 0.90, 0.05, 0.07])
            logo_ax2.imshow(bt_logo)
            logo_ax2.axis("off")
            header_x = 0.075
    fig.text(header_x, 0.96, batter_name.upper(),
             fontsize=20, color=TEXT, fontweight="bold", va="top",
             fontfamily="sans-serif")

    # game info line
    parts = []
    if inning_text:
        parts.append(inning_text)
    if game_date:
        parts.append(game_date)
    info_line = " \u2022 ".join(parts) if parts else ""
    if info_line:
        fig.text(header_x, 0.915, info_line, fontsize=10, color=TEXT2,
                 va="top", fontfamily="sans-serif")

    # stadium name
    fig.text(header_x, 0.89, f"@ {info['name']}",
             fontsize=9, color=MUTED, va="top", fontfamily="sans-serif")

    # score (top right, left of basenerd logo)
    if away_team and home_team and away_score is not None:
        score_str = f"{away_team} {away_score}  \u2022  {home_team} {home_score}"
        fig.text(0.92, 0.96, score_str, fontsize=18, color=TEXT,
                 fontweight="bold", ha="right", va="top",
                 fontfamily="sans-serif")

    # ── branding (logo top-right, text bottom-right) ────────
    logo_path = os.path.join(os.path.dirname(__file__), "..", "static",
                             "basenerd-logo-official.png")
    if os.path.exists(logo_path):
        logo_img = mpimg.imread(logo_path)
        logo_ax = fig.add_axes([0.94, 0.90, 0.05, 0.07])
        logo_ax.imshow(logo_img)
        logo_ax.axis("off")
    fig.text(0.97, 0.01, "basenerd.com", fontsize=8, color=MUTED,
             ha="right", va="bottom", fontstyle="italic")

    # ── save ───────────────────────────────────────────────
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=DPI, facecolor=BG,
                edgecolor="none", bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()
