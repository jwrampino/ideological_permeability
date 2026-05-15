from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

COLORS = {"random": "#4C72B0", "degree": "#DD8452", "bridge": "#55A868"}
rng = np.random.default_rng(9)


def spoke_positions(cx, cy, r, n):
    angles = np.linspace(0, 2 * np.pi, n, endpoint=False) + rng.uniform(0, 0.4)
    return [(cx + r * np.cos(a) + rng.normal(0, 0.07),
             cy + r * np.sin(a) + rng.normal(0, 0.07)) for a in angles]


def edge(ax, p1, p2, color="#cccccc", lw=0.9, rad=0.1):
    ax.annotate("", xy=p2, xytext=p1,
                arrowprops=dict(arrowstyle="-", color=color, lw=lw,
                                connectionstyle=f"arc3,rad={rad}"))


def node(ax, p, size, color, zorder=3):
    ax.scatter(*p, s=size, color=color, edgecolors="white",
               linewidths=1.3, zorder=zorder)


def plot():
    # three clusters in a horizontal line
    centers = [(-4.5, 0), (0, 0), (4.5, 0)]
    n_spokes = 7
    r = 1.4

    fig, ax = plt.subplots(figsize=(13, 5))
    ax.set_aspect("equal")
    ax.axis("off")

    hubs   = {}
    spokes = {}

    for ci, (cx, cy) in enumerate(centers):
        hubs[ci]   = (cx + rng.normal(0, 0.06), cy + rng.normal(0, 0.06))
        spokes[ci] = spoke_positions(cx, cy, r, n_spokes)

        for s in spokes[ci]:
            edge(ax, hubs[ci], s, color="#cccccc", lw=0.9,
                 rad=rng.uniform(-0.12, 0.12))
        for i in range(n_spokes):
            if rng.random() < 0.45:
                j = (i + 1) % n_spokes
                edge(ax, spokes[ci][i], spokes[ci][j],
                     color="#dddddd", lw=0.6, rad=rng.uniform(-0.1, 0.1))

    # bridge nodes between adjacent clusters
    bridges = {}
    for ci, cj in [(0, 1), (1, 2)]:
        bx = (centers[ci][0] + centers[cj][0]) / 2 + rng.normal(0, 0.1)
        by = rng.normal(0, 0.15)
        bridges[(ci, cj)] = (bx, by)
        edge(ax, (bx, by), hubs[ci], color="#aaaaaa", lw=1.1, rad=0.15)
        edge(ax, (bx, by), hubs[cj], color="#aaaaaa", lw=1.1, rad=-0.15)

    # random bots: two arbitrary spokes
    random_nodes = {spokes[0][4], spokes[2][2]}

    # draw organic spokes
    for ci in range(3):
        for s in spokes[ci]:
            if s in random_nodes:
                node(ax, s, 420, COLORS["random"])
            else:
                node(ax, s, 220, "#e2e2e2", zorder=2)

    # draw hubs (degree bots)
    for ci in range(3):
        node(ax, hubs[ci], 560, COLORS["degree"])

    # draw bridge bots
    for bp in bridges.values():
        node(ax, bp, 480, COLORS["bridge"])

    ax.legend(handles=[
        mpatches.Patch(color=COLORS["bridge"], label="bridge bots"),
        mpatches.Patch(color=COLORS["degree"], label="degree bots"),
        mpatches.Patch(color=COLORS["random"], label="random bots"),
        mpatches.Patch(color="#e2e2e2",        label="organic users"),
    ], fontsize=11, loc="lower center", edgecolor="#dddddd",
       ncol=4, bbox_to_anchor=(0.5, -0.08), framealpha=0.9)

    ax.set_title("Bot placement strategies on a scale-free network",
                 fontsize=13, fontweight="bold", pad=14)

    fig.tight_layout()
    Path("results/figures").mkdir(parents=True, exist_ok=True)
    fig.savefig("results/figures/bot_placement.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("saved results/figures/bot_placement.png")


if __name__ == "__main__":
    plot()