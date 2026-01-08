"""
BTC Return Distribution Analysis
Generates histogram and Q-Q plot
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
import os

# =============================================================================
# CONFIGURATION
# =============================================================================

CSV_PATH = "/Users/thiloholstein/Desktop/HSLU/Semester 3/Data Lake and Data Warehouse/Project/scripts/data/downloaded/master_analytics.csv"
OUTPUT_DIR = "/Users/thiloholstein/Desktop/HSLU/Semester 3/Data Lake and Data Warehouse/Project/outputs"


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("📂 Loading data...")
    df = pd.read_csv(CSV_PATH)
    returns = df['btc_return_pct'].dropna()

    print(f"✅ Loaded {len(returns)} return observations")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # --- HISTOGRAM ---
    print("📊 Generating histogram...")
    fig, ax = plt.subplots(figsize=(10, 6))

    ax.hist(returns, bins=50, density=True, alpha=0.7, color='#1f77b4', edgecolor='white', label='BTC Returns')

    # Overlay normal distribution
    mu, std = returns.mean(), returns.std()
    x = np.linspace(returns.min(), returns.max(), 100)
    ax.plot(x, stats.norm.pdf(x, mu, std), 'r-', lw=2, label=f'Normal (μ={mu:.2f}, σ={std:.2f})')

    ax.set_xlabel('Daily Return (%)', fontsize=12)
    ax.set_ylabel('Density', fontsize=12)
    ax.set_title('BTC Daily Returns Distribution vs Normal', fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "btc_returns_histogram.png"), dpi=300, bbox_inches='tight', facecolor='white')
    print("✅ Saved: btc_returns_histogram.png")

    # --- Q-Q PLOT ---
    print("📊 Generating Q-Q plot...")
    fig, ax = plt.subplots(figsize=(8, 8))

    stats.probplot(returns, dist="norm", plot=ax)
    ax.set_title('Q-Q Plot: BTC Returns vs Normal Distribution', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "btc_qq_plot.png"), dpi=300, bbox_inches='tight', facecolor='white')
    print("✅ Saved: btc_qq_plot.png")

    # Stats summary
    print(f"\n📈 Distribution Stats:")
    print(f"   Mean: {mu:.4f}%")
    print(f"   Std:  {std:.4f}%")
    print(f"   Skew: {returns.skew():.4f}")
    print(f"   Kurt: {returns.kurtosis():.4f}")

    plt.show()


if __name__ == "__main__":
    main()