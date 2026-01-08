"""
BTC Time Series Decomposition
Generates trend, seasonality, and residuals plot
"""

import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose
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
    print("Loading data...")
    df = pd.read_csv(CSV_PATH)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date')

    # Get BTC price series
    series = df['btc_price'].dropna()

    print(f"Loaded {len(series)} observations")
    print("Decomposing time series...")

    # Decompose
    decomposition = seasonal_decompose(series, model='additive', period=30)

    # Plot
    fig, axes = plt.subplots(4, 1, figsize=(14, 10))

    decomposition.observed.plot(ax=axes[0], title='Observed (BTC Price USD)', color='#1f77b4')
    decomposition.trend.plot(ax=axes[1], title='Trend', color='#ff7f0e')
    decomposition.seasonal.plot(ax=axes[2], title='Seasonality (30-day)', color='#2ca02c')
    decomposition.resid.plot(ax=axes[3], title='Residuals', color='#d62728')

    for ax in axes:
        ax.grid(True, alpha=0.3)

    plt.suptitle('Bitcoin Price Decomposition: Trend, Seasonality, Residuals', fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, "btc_decomposition.png")
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"Saved: {output_path}")
    plt.show()


if __name__ == "__main__":
    main()