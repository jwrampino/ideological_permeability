import pandas as pd
import networkx as nx
from pathlib import Path

# load data
nodes_df = pd.read_csv("data/nodes_latest.csv")
edges_df = pd.read_csv("data/edges_latest.csv")

# build directed graph
G = nx.DiGraph()

# add nodes
for _, row in nodes_df.iterrows():
    G.add_node(
        row["did"],
        size=5 + row["post_count"] + row["reply_count"]
    )

# add edges
for _, row in edges_df.iterrows():
    G.add_edge(row["src"], row["dst"])

print("\nGRAPH CONNECTIVITY METRICS\n")

# basic counts
num_nodes = G.number_of_nodes()
num_edges = G.number_of_edges()
density = nx.density(G)

print(f"nodes: {num_nodes:,}")
print(f"edges: {num_edges:,}")
print(f"density: {density:.6f}")

# undirected view for overall connectivity
undirected = G.to_undirected()

components = list(nx.connected_components(undirected))
components = sorted(components, key=len, reverse=True)

num_components = len(components)
largest_cc_size = len(components[0]) if components else 0

print(f"\nconnected components: {num_components:,}")
print(f"largest component size: {largest_cc_size:,}")

# share of graph in largest component
largest_fraction = largest_cc_size / num_nodes if num_nodes > 0 else 0
print(f"largest component fraction: {largest_fraction:.4f}")

# fragmentation (how broken it is)
fragmentation = 1 - largest_fraction
print(f"fragmentation: {fragmentation:.4f}")

# average component size
avg_comp_size = (
    sum(len(c) for c in components) / num_components
    if num_components > 0 else 0
)
print(f"average component size: {avg_comp_size:.2f}")

# degree stats
degrees = [d for _, d in G.degree()]
avg_degree = sum(degrees) / len(degrees) if degrees else 0

print(f"\naverage degree: {avg_degree:.2f}")
print(f"max degree: {max(degrees) if degrees else 0}")

# directed connectivity
is_weak = nx.is_weakly_connected(G) if num_nodes > 0 else False
is_strong = nx.is_strongly_connected(G) if num_nodes > 0 else False

print(f"\nweakly connected: {is_weak}")
print(f"strongly connected: {is_strong}")

# strongly connected components (true directed reachability)
sccs = list(nx.strongly_connected_components(G))
largest_scc = max(len(c) for c in sccs) if sccs else 0

print(f"strongly connected components: {len(sccs):,}")
print(f"largest SCC size: {largest_scc:,}")

# collect metrics into a dict
metrics = {
    "nodes": num_nodes,
    "edges": num_edges,
    "density": density,
    "connected_components": num_components,
    "largest_component_size": largest_cc_size,
    "largest_component_fraction": largest_fraction,
    "fragmentation": fragmentation,
    "average_component_size": avg_comp_size,
    "average_degree": avg_degree,
    "max_degree": max(degrees) if degrees else 0,
    "weakly_connected": is_weak,
    "strongly_connected": is_strong,
    "num_scc": len(sccs),
    "largest_scc_size": largest_scc,
}

# save to csv in data/
out_path = Path("data/connectivity_metrics.csv")

df = pd.DataFrame([metrics])

# append if file exists, otherwise create
df.to_csv(out_path, mode="a", header=not out_path.exists(), index=False)

print(f"\nSaved metrics to {out_path}")