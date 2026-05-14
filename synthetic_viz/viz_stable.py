from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.ndimage import gaussian_filter1d

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

df = pd.read_csv("results/runs.csv")

out_dir = Path("results/figures")
out_dir.mkdir(parents=True, exist_ok=True)

# pool across strategies and reps; this asks about system instability, not strategy effects
vol = (
    df.groupby("rho", as_index=False)
      .agg(
          F_sd=("F", "std"),
          F_mean=("F", "mean"),
          P_inf_sd=("P_inf", "std"),
          P_inf_mean=("P_inf", "mean"),
          n=("F", "size")
      )
      .sort_values("rho")
)

x = vol["rho"].values
y = vol["F_sd"].values
y_smooth = gaussian_filter1d(y, sigma=1.25)

peak_idx = int(np.nanargmax(y_smooth))
peak_rho = x[peak_idx]
peak_sd = y_smooth[peak_idx]

with plt.rc_context(STYLE):
    fig, ax = plt.subplots(figsize=(9, 5))

    ax.scatter(
        x,
        y,
        color="#333",
        alpha=0.35,
        s=24,
        zorder=2,
        label="observed volatility"
    )

    ax.plot(
        x,
        y_smooth,
        color="#333",
        linewidth=2.5,
        zorder=3,
        label="smoothed volatility"
    )

    ax.axvline(
        peak_rho,
        color="#333",
        lw=1.0,
        ls="--",
        alpha=0.65,
        zorder=1
    )

    ax.text(
        peak_rho + 0.006,
        peak_sd,
        rf"peak instability at $\rho \approx {peak_rho:.2f}$",
        fontsize=8,
        color="#333",
        va="bottom",
        bbox=dict(boxstyle="round,pad=0.2", fc="white", alpha=0.75, ec="none")
    )

    ax.set_xlabel(r"bot density ($\rho$)")
    ax.set_ylabel(r"fragmentation volatility ($SD(F)$)")
    ax.set_title(
        "Fragmentation becomes most unstable near the critical threshold",
        fontsize=12,
        fontweight="bold",
        pad=10
    )

    ax.text(
        0.5,
        -0.16,
        "Pooling across synthetic conditions shows where small changes in bot density produce the largest variation in fragmentation.",
        transform=ax.transAxes,
        ha="center",
        fontsize=8,
        color="#555",
        style="italic"
    )

    ax.set_xlim(0, df["rho"].max() * 1.08)
    ax.set_ylim(0, max(y_smooth) * 1.25)

    ax.legend(fontsize=9)

    fig.tight_layout()

    out_path = out_dir / "5_fragmentation_volatility.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

print(f"saved {out_path}")