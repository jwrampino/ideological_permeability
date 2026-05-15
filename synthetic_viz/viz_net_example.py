from pathlib import Path
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch

COLORS = {"random": "#4C72B0", "degree": "#DD8452", "bridge": "#55A868"}
rng = np.random.default_rng(10)


def make_community(n, p=0.5, seed=None):
    while True:
        G = nx.erdos_renyi_graph(n, p, seed=seed)
        if nx.is_connected(G):
            return G


def offset_layout(G, cx, cy, scale=1.0, seed=None):
    pos = nx.spring_layout(G, seed=seed, k=1.2)
    return {v: (cx + pos[v][0] * scale, cy + pos[v][1] * scale)
            for v in G.nodes()}


def plot():
    c1 = make_community(9, p=0.45, seed=1)
    c2 = make_community(9, p=0.45, seed=3)

    communities = [c1, c2]
    offsets     = [(-2.8, 0.0), (2.8, 0.0)]
    scales      = [1.1, 1.1]
    seeds       = [10, 30]

    pos = {}
    node_offset = 0
    community_nodes = []
    G_full = nx.Graph()

    for G, (cx, cy), sc, sd in zip(communities, offsets, scales, seeds):
        mapping = {v: v + node_offset for v in G.nodes()}
        G_r     = nx.relabel_nodes(G, mapping)
        G_full  = nx.compose(G_full, G_r)
        lpos    = offset_layout(G, cx, cy, scale=sc, seed=sd)
        for v, p in lpos.items():
            pos[v + node_offset] = p
        community_nodes.append(list(mapping.values()))
        node_offset += len(G)

    # one bridge edge connecting the two communities
    bridge_edge = (community_nodes[0][2], community_nodes[1][4])
    G_full.add_edge(*bridge_edge)

    # metric-derived bot targets
    # degree bots: highest degree node in each community
    degree_bots = {
        max(c, key=lambda v: G_full.degree(v))
        for c in community_nodes
    }

    # bridge bots: endpoints of the bridge edge (highest betweenness in full graph)
    bridge_bots = set(bridge_edge)

    # random bots: arbitrary nodes not already targeted
    taken       = degree_bots | bridge_bots
    candidates  = [v for v in G_full.nodes() if v not in taken]
    random_bots = set(rng.choice(candidates, size=4, replace=False))

    all_bots = degree_bots | bridge_bots | random_bots

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.set_aspect("equal")
    ax.axis("off")

    # internal edges
    internal = [(u, v) for u, v in G_full.edges()
                if (u, v) != bridge_edge and (v, u) != bridge_edge]
    nx.draw_networkx_edges(G_full, pos, edgelist=internal, ax=ax,
                           edge_color="#666666", width=0.9, alpha=0.6)

    # bridge edge
    nx.draw_networkx_edges(G_full, pos, edgelist=[bridge_edge], ax=ax,
                           edge_color="#5588DD", width=2.0, alpha=0.9)

    # organic nodes
    organic = [v for v in G_full.nodes() if v not in all_bots]
    nx.draw_networkx_nodes(G_full, pos, nodelist=organic, ax=ax,
                           node_color="white", edgecolors="#444444",
                           node_size=280, linewidths=1.3)

    # bot nodes priority: bridge > degree > random (no overlap)
    drawn = set()
    for label, nodes in [("bridge", bridge_bots),
                          ("degree", degree_bots),
                          ("random", random_bots)]:
        targets = [v for v in nodes if v not in drawn]
        nx.draw_networkx_nodes(G_full, pos, nodelist=targets, ax=ax,
                               node_color=COLORS[label], edgecolors="white",
                               node_size=380, linewidths=1.3)
        drawn |= set(targets)

    # community labels
    for i, (cx, cy) in enumerate(offsets):
        ax.text(cx, cy - 1.6, f"Community {i + 1}",
                ha="center", fontsize=11, fontweight="bold",
                color="#222222", fontfamily="sans-serif")

    # dashed border
    ax.add_patch(FancyBboxPatch(
        (-4.4, -2.0), 8.8, 4.2,
        boxstyle="round,pad=0.1",
        linewidth=1.4, edgecolor="#999999",
        facecolor="none", linestyle="--", zorder=0
    ))

    ax.legend(handles=[
        mpatches.Patch(color=COLORS["bridge"], label="bridge bots"),
        mpatches.Patch(color=COLORS["degree"], label="degree bots"),
        mpatches.Patch(color=COLORS["random"], label="random bots"),
        mpatches.Patch(facecolor="white", edgecolor="#444444", label="organic users"),
    ], fontsize=10, loc="upper center", edgecolor="#dddddd",
       ncol=4, bbox_to_anchor=(0.5, 1.05), framealpha=1.0)

    fig.tight_layout()
    Path("results/figures").mkdir(parents=True, exist_ok=True)
    fig.savefig("results/figures/bot_placement.png", dpi=150,
                bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print("saved results/figures/bot_placement.png")


if __name__ == "__main__":
    plot()