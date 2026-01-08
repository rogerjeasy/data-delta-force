"""
Regime Timeline and Risk Metric Comparisons
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import os

# =============================================================================
# CONFIGURATION
# =============================================================================

MASTER_CSV = "/Users/thiloholstein/Desktop/HSLU/Semester 3/Data Lake and Data Warehouse/Project/scripts/data/downloaded/master_analytics.csv"
REGIMES_CSV = "/Users/thiloholstein/Desktop/HSLU/Semester 3/Data Lake and Data Warehouse/Project/scripts/data/downloaded/macro_regimes.csv"
OUTPUT_DIR = "/Users/thiloholstein/Desktop/HSLU/Semester 3/Data Lake and Data Warehouse/Project/outputs"


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("📂 Loading data...")
    df = pd.read_csv(MASTER_CSV)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # --- 1. REGIME TIMELINE ---
    print("📊 Generating regime timeline...")
    fig, ax = plt.subplots(figsize=(14, 6))

    # Plot BTC returns
    ax.plot(df['date'], df['btc_return_pct'], color='#1f77b4', alpha=0.7, linewidth=0.8)
    ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)

    # Color background by regime
    regime_colors = {
        'Neutral': '#90EE90',
        'Risk-Off Stagflation': '#FFB6C1',
        'Mixed Moderate Vol': '#87CEEB',
        'Transitional Easing': '#DDA0DD',
        'Risk-Off Tightening': '#FF6347'
    }

    if 'fomc_regime' in df.columns:
        regime_col = 'fomc_regime'
    else:
        regime_col = None

    if regime_col:
        for regime, color in regime_colors.items():
            mask = df[regime_col] == regime
            if mask.any():
                for idx in df[mask].index:
                    ax.axvspan(df.loc[idx, 'date'], df.loc[idx, 'date'] + pd.Timedelta(days=1),
                               alpha=0.3, color=color, linewidth=0)

    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('BTC Daily Return (%)', fontsize=12)
    ax.set_title('Market Regime Timeline with BTC Returns (2024-2025)', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)

    # Legend
    patches = [mpatches.Patch(color=c, alpha=0.5, label=r) for r, c in regime_colors.items()]
    ax.legend(handles=patches, loc='upper right', fontsize=9)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "regime_timeline.png"), dpi=300, bbox_inches='tight', facecolor='white')
    print("✅ Saved: regime_timeline.png")

    # --- 2. SHARPE COMPARISON ---
    print("📊 Generating Sharpe comparison...")
    fig, ax = plt.subplots(figsize=(8, 6))

    # Calculate Sharpe ratios
    rf = 0.0433 / 365  # Daily risk-free rate
    assets = {
        'BTC': df['btc_return_pct'],
        'ETH': df['eth_return_pct']
    }

    sharpe_ratios = {}
    for name, returns in assets.items():
        returns_clean = returns.dropna()
        excess_return = returns_clean.mean() - rf
        sharpe = (excess_return / returns_clean.std()) * np.sqrt(365)
        sharpe_ratios[name] = sharpe

    colors = ['#1f77b4', '#ff7f0e']
    bars = ax.bar(sharpe_ratios.keys(), sharpe_ratios.values(), color=colors, edgecolor='black')

    ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax.set_ylabel('Annualized Sharpe Ratio', fontsize=12)
    ax.set_title('Sharpe Ratios by Asset', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3, axis='y')

    # Add value labels
    for bar, val in zip(bars, sharpe_ratios.values()):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.02,
                f'{val:.3f}', ha='center', va='bottom', fontsize=11, fontweight='bold')

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "sharpe_comparison.png"), dpi=300, bbox_inches='tight', facecolor='white')
    print("✅ Saved: sharpe_comparison.png")

    # --- 3. VaR COMPARISON ---
    print("📊 Generating VaR comparison...")
    fig, ax = plt.subplots(figsize=(8, 6))

    # Calculate 95% VaR
    var_95 = {}
    for name, returns in assets.items():
        returns_clean = returns.dropna()
        var_95[name] = np.percentile(returns_clean, 5)  # 5th percentile = 95% VaR

    colors = ['#d62728', '#ff7f0e']
    bars = ax.bar(var_95.keys(), [abs(v) for v in var_95.values()], color=colors, edgecolor='black')

    ax.set_ylabel('VaR 95% (% Daily Loss)', fontsize=12)
    ax.set_title('Value at Risk (95%) by Asset', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3, axis='y')

    # Add value labels
    for bar, val in zip(bars, var_95.values()):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
                f'{abs(val):.2f}%', ha='center', va='bottom', fontsize=11, fontweight='bold')

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "var_comparison.png"), dpi=300, bbox_inches='tight', facecolor='white')
    print("✅ Saved: var_comparison.png")

    print("\n✅ ALL PLOTS COMPLETE!")
    plt.show()


if __name__ == "__main__":
    main()