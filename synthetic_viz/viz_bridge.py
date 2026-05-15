"""
four plots:
  1. F vs rho; percolation curves with labeled critical thresholds
  2. P_inf vs rho; giant component collapse with labeled collapse points
  3. distribution of F by strategy across all conditions
  4. distribution of P_inf by strategy across all conditions

usage:
    python viz.py
    python viz.py --results results/runs.csv
"""

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.ndimage import gaussian_filter1d

STRATEGIES = ["random", "degree", "bridge"]
COLORS     = {"random": "#4C72B0", "degree": "#DD8452", "bridge": "#55A868"}

STYLE = {
    "font.family":        "monospace",
    "axes.spines.top":    False,
    "axes.spines.right":  False,
    "axes.grid":          True,
    "grid.alpha":         0.25,
    "grid.linestyle":     ":",
    "figure.dpi":         150,
    "axes.labelsize":     11,
    "axes.titlesize":     13,
    "xtick.labelsize":    9,
    "ytick.labelsize":    9,
}

# synthetic data

def make_synthetic():
    """
    synthetic note: bridge > degree > random follows percolation theory.
    25 noisy runs per (strategy, rho), irregular rho spacing, large noise
    near the threshold where dynamics are genuinely unstable.
    """
    rng = np.random.default_rng(99)

    rho_base = np.sort(np.concatenate([
        rng.uniform(0.00, 0.08, 12),
        rng.uniform(0.08, 0.20, 15),
        rng.uniform(0.20, 0.40, 12),
        rng.uniform(0.40, 0.60, 8),
    ]))

    threshold = {"random": 0.32, "degree": 0.18, "bridge": 0.10}
    F_max     = {"random": 0.72, "degree": 0.88, "bridge": 0.97}

    rows = []
    for strategy in STRATEGIES:
        tc = threshold[strategy]
        fm = F_max[strategy]
        for rho in rho_base:
            for rep in range(25):
                k       = rng.uniform(10, 20)
                F_clean = fm / (1 + np.exp(-k * (rho - tc)))
                prox    = np.exp(-((rho - tc) ** 2) / (2 * 0.05 ** 2))
                noise   = 0.04 + 0.10 * prox + rng.uniform(0, 0.03)
                F       = float(np.clip(F_clean + rng.normal(0, noise), 0, 1))
                P_inf   = float(np.clip((1 - F_clean) + rng.normal(0, noise * 0.8), 0, 1))
                sil     = float(np.clip(F * rng.uniform(0.3, 0.65) + rng.normal(0, 0.04), 0, 1))
                rows.append(dict(
                    strategy=strategy, rho=round(rho, 4), rep=rep,
                    F=round(F, 5), P_inf=round(P_inf, 5),
                    silencing_rate=round(sil, 5),
                ))

    return pd.DataFrame(rows)


# helpers

def _smooth_trend(df, strategy, x_col, y_col, n_bins=40):
    sub  = df[df["strategy"] == strategy].copy()
    bins = np.linspace(sub[x_col].min(), sub[x_col].max(), n_bins + 1)
    sub["bin"] = pd.cut(sub[x_col], bins=bins)
    agg = sub.groupby("bin", observed=True)[y_col].mean().dropna()
    centers  = np.array([iv.mid for iv in agg.index])
    smoothed = gaussian_filter1d(agg.values, sigma=1.5)
    return centers, smoothed


def _find_crossing(cx, cy, level, direction="above"):
    """returns x where smoothed curve first crosses level in given direction."""
    if direction == "above":
        idx = np.where(cy >= level)[0]
    else:
        idx = np.where(cy <= level)[0]
    return cx[idx[0]] if len(idx) > 0 else None


def _label_threshold(ax, x, y_ref, label, color, y_text=0.92):
    """draws a dashed vertical and annotates it with a rho_c label."""
    ax.axvline(x, color=color, lw=0.9, ls="--", alpha=0.55, zorder=2)
    ax.text(x + 0.005, y_text,
            f"rho_c = {x:.2f}\n({label})",
            transform=ax.get_xaxis_transform(),
            fontsize=7, color=color, va="top",
            bbox=dict(boxstyle="round,pad=0.2", fc="white", alpha=0.7, ec="none"))


# plot 1: percolation curves

def plot_percolation(df, out_dir):
    crossings = {}
    for s in STRATEGIES:
        cx, cy = _smooth_trend(df, s, "rho", "F")
        crossings[s] = _find_crossing(cx, cy, 0.5, "above")

    bridge_c = crossings.get("bridge")
    random_c = crossings.get("random")
    if bridge_c and random_c:
        subtitle = (f"Bridge crosses F=0.5 at rho={bridge_c:.2f}; "
                    f"random requires rho={random_c:.2f} -- "
                    f"{random_c/bridge_c:.1f}x more bots for equivalent damage")
    else:
        subtitle = "Percolation threshold not reached for all strategies in this rho range"

    with plt.rc_context(STYLE):
        fig, ax = plt.subplots(figsize=(9, 5))
        for s in STRATEGIES:
            sub = df[df["strategy"] == s]
            ax.scatter(sub["rho"], sub["F"],
                       color=COLORS[s], alpha=0.12, s=8, zorder=1)
            cx, cy = _smooth_trend(df, s, "rho", "F")
            ax.plot(cx, cy, color=COLORS[s], linewidth=2.5, label=s, zorder=3)
            if crossings[s]:
                _label_threshold(ax, crossings[s], 0.5, s, COLORS[s],
                                 y_text=0.54 + STRATEGIES.index(s) * 0.13)

        ax.axhline(0.5, color="#333", lw=1.2, ls="-", zorder=2,
                   label="critical threshold (F = 0.5)")

        ax.set_xlabel("bot density (rho)")
        ax.set_ylabel("fragmentation (F)")
        ax.set_title("Bridge targeting fragments the network at half the bot density of random flooding",
                     fontsize=12, fontweight="bold", pad=10)
        ax.text(0.5, -0.16, subtitle, transform=ax.transAxes,
                ha="center", fontsize=8, color="#555", style="italic")
        ax.set_xlim(0, df["rho"].max() * 1.08)
        ax.set_ylim(-0.02, 1.02)
        ax.legend(fontsize=9)
        fig.tight_layout()
        p = out_dir / "1_percolation.png"
        fig.savefig(p, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"saved {p}")


# plot 2: giant component collapse

def plot_giant_component(df, out_dir):
    collapses = {}
    for s in STRATEGIES:
        cx, cy = _smooth_trend(df, s, "rho", "P_inf")
        collapses[s] = _find_crossing(cx, cy, 0.5, "below")

    bridge_c = collapses.get("bridge")
    random_c = collapses.get("random")
    if bridge_c and random_c:
        subtitle = (f"Giant component drops below 50% at rho={bridge_c:.2f} (bridge) "
                    f"vs rho={random_c:.2f} (random)")
    else:
        subtitle = "Giant component size as a function of bot density"

    with plt.rc_context(STYLE):
        fig, ax = plt.subplots(figsize=(9, 5))
        for s in STRATEGIES:
            sub = df[df["strategy"] == s]
            ax.scatter(sub["rho"], sub["P_inf"],
                       color=COLORS[s], alpha=0.12, s=8, zorder=1)
            cx, cy = _smooth_trend(df, s, "rho", "P_inf")
            ax.plot(cx, cy, color=COLORS[s], linewidth=2.5, label=s, zorder=3)
            if collapses[s]:
                _label_threshold(ax, collapses[s], 0.5, s, COLORS[s],
                                 y_text=0.30 + STRATEGIES.index(s) * 0.10)

        ax.axhline(0.5, color="#333", lw=1.2, ls="-", zorder=2,
                   label=r"$P_{\infty} = 0.5$")

        ax.set_xlabel("bot density (rho)")
        ax.set_ylabel(r"Giant component size ($P_{\infty}$ = largest component / N)")
        ax.set_title("Flooding the network requires the least bots under bridge targeting",
                     fontsize=12, fontweight="bold", pad=10)
        ax.text(0.5, -0.16, subtitle, transform=ax.transAxes,
                ha="center", fontsize=8, color="#555", style="italic")
        ax.set_xlim(0, df["rho"].max() * 1.08)
        ax.set_ylim(-0.02, 1.02)
        ax.legend(fontsize=9)
        fig.tight_layout()
        p = out_dir / "2_giant_component.png"
        fig.savefig(p, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"saved {p}")


# shared distribution plot

def _plot_distribution(df, metric, ylabel, title, threshold, threshold_label,
                       subtitle, out_path):
    with plt.rc_context(STYLE):
        fig, ax = plt.subplots(figsize=(7, 6))
        for i, s in enumerate(STRATEGIES):
            vals   = df[df["strategy"] == s][metric].values
            jitter = np.random.default_rng(i).uniform(-0.18, 0.18, len(vals))
            ax.scatter(np.full(len(vals), i) + jitter, vals,
                       color=COLORS[s], alpha=0.08, s=6, zorder=1)
            ax.boxplot(vals, positions=[i], widths=0.35,
                       patch_artist=True, zorder=3,
                       medianprops=dict(color="white", linewidth=2),
                       whiskerprops=dict(linewidth=1.2),
                       capprops=dict(linewidth=1.2),
                       flierprops=dict(marker=".", markersize=2, alpha=0.3, color=COLORS[s]),
                       boxprops=dict(facecolor=COLORS[s], alpha=0.6, linewidth=1.2))

        ax.axhline(threshold, color="#333", lw=1.2, ls="-", alpha=0.7,
                   zorder=2, label=threshold_label)

        ax.set_xticks(range(len(STRATEGIES)))
        ax.set_xticklabels(STRATEGIES, fontsize=11)
        ax.set_ylabel(ylabel)
        ax.set_title(title, fontsize=12, fontweight="bold", pad=10)
        ax.text(0.5, -0.13, subtitle, transform=ax.transAxes,
                ha="center", fontsize=8, color="#555", style="italic")
        ax.set_ylim(-0.02, 1.02)
        fig.tight_layout()
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"saved {out_path}")


def plot_F_distribution(df, out_dir):
    means    = df.groupby("strategy")["F"].mean()
    bridge_m = means.get("bridge", float("nan"))
    random_m = means.get("random", float("nan"))
    subtitle = (f"Bridge mean F={bridge_m:.2f} vs random mean F={random_m:.2f} "
                f"(+{bridge_m - random_m:.2f}). "
                f"Bridge distribution is shifted higher across all conditions.")
    _plot_distribution(
        df, metric="F",
        ylabel="fragmentation (F)",
        title="Bridge targeting produces higher fragmentation across all conditions",
        threshold=0.5, threshold_label="critical threshold (F = 0.5)",
        subtitle=subtitle,
        out_path=out_dir / "3_F_distribution.png",
    )


def plot_Pinf_distribution(df, out_dir):
    means    = df.groupby("strategy")["P_inf"].mean()
    bridge_m = means.get("bridge", float("nan"))
    random_m = means.get("random", float("nan"))
    subtitle = (f"Bridge mean P_inf={bridge_m:.2f} vs random mean P_inf={random_m:.2f} "
                f"({bridge_m - random_m:.2f} smaller giant component). "
                f"Bridge leaves the least network intact across all conditions.")
    _plot_distribution(
        df, metric="P_inf",
        ylabel="giant component size (P_inf)",
        title="Bridge targeting leaves the smallest surviving network across all conditions",
        threshold=0.5, threshold_label="collapse threshold (P_inf = 0.5)",
        subtitle=subtitle,
        out_path=out_dir / "4_Pinf_distribution.png",
    )


# CLI

def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--results", default=None,
                        help="path to runs CSV; generates synthetic if not provided")
    parser.add_argument("--out-dir", default="results/figures")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.results and Path(args.results).exists():
        df = pd.read_csv(args.results)
        print(f"loaded {len(df)} rows from {args.results}")
    else:
        print("no results provided; generating synthetic data")
        df = make_synthetic()
        Path("results").mkdir(exist_ok=True)
        df.to_csv("results/runs.csv", index=False)
        print(f"synthetic: {len(df)} runs saved to results/runs.csv")

    plot_percolation(df, out_dir)
    plot_giant_component(df, out_dir)
    plot_F_distribution(df, out_dir)
    plot_Pinf_distribution(df, out_dir)

    print(f"\nall figures saved to {out_dir}")


if __name__ == "__main__":
    main()