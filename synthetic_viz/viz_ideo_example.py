"""
Plot demonstrating the multi-layer attributed graph of stances across layers of topics.

Lazer develops a view of ideology as layers of values, attitudes, beliefs, and rationalizations
which are observed in natural language as mediated by framing. This visualization shows the
observable end of this process, similarly structured as multi-layer networks of stances.
"""

import os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
from matplotlib.patches import FancyArrowPatch

os.makedirs("results/figures", exist_ok=True)

TEAL   = '#0F8A6A'
TEAL_L = '#3EC49A'
PURP   = '#5C52C4'
PURP_L = '#8F88E0'
GRAY   = '#9B9690'
GRAY_L = '#C5C1BB'
BG     = '#F8F6F1'
BAND_A = '#F0EDE4'
BAND_B = '#EAE6DC'
RULE   = '#D4CEBF'
INK    = '#2E2B26'
MUTED  = '#7A766E'

TN = [
    [(1.0,1.5),(2.0,0.8),(3.0,2.5),(2.5,3.2),(4.0,1.8)],
    [(1.1,1.6),(2.1,2.6),(3.1,0.9),(4.1,2.0)],
    [(1.0,1.5),(2.0,2.5),(3.0,0.8),(4.0,2.0)],
    [(1.5,1.8),(2.5,1.0),(3.5,2.5)],
]
PN = [
    [(5.5,1.5),(6.3,2.5)],
    [(5.5,1.5),(6.3,2.5)],
    [(5.9,2.0)],
    [(5.8,1.8),(6.4,2.3)],
]
GN = [
    [(7.5,1.5)],
    [(7.2,1.0),(7.7,2.2)],
    [(7.4,1.8)],
    [],
]

LAYER_LABELS = ['Topic\nlayer 1','Topic\nlayer 2','Topic\nlayer 3','Topic\nlayer 4']
LAYER_Y  = [1.6, 4.0, 6.4, 8.8]
BAND_H   = 1.05

def npos(pt, zi):
    # map node (x,y) into display space within layer band
    x = pt[0]
    y = LAYER_Y[zi] + (pt[1] - 2.0) * 0.32
    return x, y

fig, ax = plt.subplots(figsize=(6.5, 11))
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG)
ax.set_xlim(-1.8, 9.2)
ax.set_ylim(-0.6, 11.0)
ax.axis('off')

# alternating bands
for i, ly in enumerate(LAYER_Y):
    c = BAND_A if i % 2 == 0 else BAND_B
    ax.add_patch(plt.Rectangle((0.0, ly - BAND_H), 8.0, BAND_H * 2,
                                color=c, zorder=0, linewidth=0))
    ax.plot([0.0, 8.0], [ly - BAND_H, ly - BAND_H], color=RULE, lw=0.7, zorder=1)
    ax.plot([0.0, 8.0], [ly + BAND_H, ly + BAND_H], color=RULE, lw=0.7, zorder=1)

# cross-layer teal edges
for zi in range(3):
    for i in range(min(len(TN[zi]), len(TN[zi+1]))):
        x1,y1 = npos(TN[zi][i],   zi)
        x2,y2 = npos(TN[zi+1][i], zi+1)
        ax.plot([x1,x2],[y1,y2], color=TEAL_L, lw=1.1, alpha=0.35, zorder=2, solid_capstyle='round')

# cross-layer purple edges
for i in range(min(len(PN[0]), len(PN[1]))):
    x1,y1 = npos(PN[0][i], 0)
    x2,y2 = npos(PN[1][i], 1)
    ax.plot([x1,x2],[y1,y2], color=PURP_L, lw=1.1, alpha=0.35, zorder=2, solid_capstyle='round')
hub = npos(PN[2][0], 2)
for p in PN[1]:
    x1,y1 = npos(p, 1)
    ax.plot([x1,hub[0]],[y1,hub[1]], color=PURP_L, lw=1.1, alpha=0.35, zorder=2)
for p in PN[3]:
    x2,y2 = npos(p, 3)
    ax.plot([hub[0],x2],[hub[1],y2], color=PURP_L, lw=1.1, alpha=0.35, zorder=2)

# within-layer teal edges
pairs_t = [
    [(0,1),(1,2),(2,3),(0,4),(3,4)],
    [(0,1),(1,2),(0,3),(2,3)],
    [(0,1),(1,2),(0,3),(2,3)],
    [(0,1),(1,2)],
]
for zi, ns in enumerate(TN):
    for i,j in pairs_t[zi]:
        if i < len(ns) and j < len(ns):
            x1,y1 = npos(ns[i], zi)
            x2,y2 = npos(ns[j], zi)
            ax.plot([x1,x2],[y1,y2], color=TEAL, lw=2.0, alpha=0.85, zorder=3, solid_capstyle='round')

# within-layer purple edges
pairs_p = [[(0,1)],[(0,1)],[],[(0,1)]]
for zi, ns in enumerate(PN):
    for i,j in pairs_p[zi]:
        if i < len(ns) and j < len(ns):
            x1,y1 = npos(ns[i], zi)
            x2,y2 = npos(ns[j], zi)
            ax.plot([x1,x2],[y1,y2], color=PURP, lw=2.0, alpha=0.85, zorder=3, solid_capstyle='round')

NS = 85

# teal nodes
for zi, ns in enumerate(TN):
    xs,ys = zip(*[npos(p, zi) for p in ns])
    ax.scatter(xs, ys, s=NS, color=TEAL, zorder=5, linewidths=1.8, edgecolors='white')

# purple nodes
for zi, ns in enumerate(PN):
    for pi, p in enumerate(ns):
        x,y = npos(p, zi)
        sz = NS * 2.2 if (zi == 2) else NS
        ax.scatter([x],[y], s=sz, color=PURP, zorder=5, linewidths=1.8, edgecolors='white')

# gray inactive nodes
for zi, ns in enumerate(GN):
    if not ns: continue
    xs,ys = zip(*[npos(p, zi) for p in ns])
    ax.scatter(xs, ys, s=NS, color=GRAY, zorder=5, linewidths=1.5, edgecolors='white', alpha=0.65)

# layer labels left margin
for zi,(lbl,ly) in enumerate(zip(LAYER_LABELS, LAYER_Y)):
    ax.text(-0.3, ly, lbl, fontsize=8.5, color=MUTED, va='center', ha='right',
            linespacing=1.45,
            fontfamily='sans-serif')

# thin left border rule
ax.plot([-0.1,-0.1],[LAYER_Y[0]-BAND_H, LAYER_Y[-1]+BAND_H], color=RULE, lw=1.2, zorder=1)

# ideology header labels
ax.text(2.4, 10.3, 'Ideology A', fontsize=11.5,
        color=TEAL, ha='center', fontweight='bold', fontfamily='sans-serif')
ax.text(6.1, 10.3, 'Ideology B', fontsize=11.5,
        color=PURP, ha='center', fontweight='bold', fontfamily='sans-serif')

# subtle header underline dots
for cx, col in [(2.4, TEAL), (6.1, PURP)]:
    ax.plot(cx, 10.05, 'o', color=col, ms=3.5, zorder=6, alpha=0.6)

# legend
legend_els = [
    Line2D([0],[0], marker='o', color='none', markerfacecolor=TEAL,
           markeredgecolor='white', markeredgewidth=1.4, markersize=8,
           label='Ideology (Sub)network A'),
    Line2D([0],[0], marker='o', color='none', markerfacecolor=PURP,
           markeredgecolor='white', markeredgewidth=1.4, markersize=8,
           label='Ideology (Sub)network B'),
    Line2D([0],[0], marker='o', color='none', markerfacecolor=GRAY,
           markeredgecolor='white', markeredgewidth=1.4, markersize=8, alpha=0.65,
           label='Inactive'),
]
leg = ax.legend(
    handles=legend_els,
    title='Stance entailments',
    loc='lower center',
    bbox_to_anchor=(0.5, -0.055),
    ncol=3,
    frameon=True,
    facecolor=BG,
    edgecolor=RULE,
    fontsize=8.5,
    title_fontsize=8.5,
    handletextpad=0.4,
    columnspacing=1.0,
)
leg.get_frame().set_linewidth(0.8)
leg.get_title().set_color(INK)
leg.get_title().set_fontweight('bold')
for text in leg.get_texts():
    text.set_color(MUTED)

# caption
ax.text(4.0, -0.45,
        'Stances as attributed multi-level (sub)networks over layers of topics',
        fontsize=8, color=MUTED, ha='center', style='italic', fontfamily='sans-serif')

plt.subplots_adjust(left=0.18, right=0.97, top=0.97, bottom=0.07)
plt.savefig('results/figures/stance_network.png',
            dpi=220, bbox_inches='tight', facecolor=BG)
print('saved.')