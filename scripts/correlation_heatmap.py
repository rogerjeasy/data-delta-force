"""
Correlation Heatmap Generator
Generates publication-quality correlation heatmap from local CSV
For: Data Delta Force - Crypto-Macro Risk Intelligence Platform
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import os

# =============================================================================
# CONFIGURATION
# =============================================================================

CSV_PATH = "/Users/thiloholstein/Desktop/HSLU/Semester 3/Data Lake and Data Warehouse/Project/scripts/data/downloaded/rolling_correlations.csv"
OUTPUT_DIR = "/Users/thiloholstein/Desktop/HSLU/Semester 3/Data Lake and Data Warehouse/Project/outputs"


# =============================================================================
# MAIN FUNCTIONS
# =============================================================================

def load_data():
    """Load rolling correlations data from local CSV"""
    print(f"📂 Loading data from: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    print(f"✅ Loaded {len(df)} rows")
    return df


def create_correlation_matrix(df):
    """Transform long-format correlation data to correlation matrix"""
    print("🔄 Creating correlation matrix...")

    # Calculate average 30-day correlation per asset pair
    avg_corr = df.groupby('asset_pair')['corr_30d'].mean()
    print(f"   Asset pairs found: {list(avg_corr.index)}")

    # Extract unique assets from pairs
    assets = set()
    for pair in avg_corr.index:
        parts = pair.split('-')
        assets.update(parts)

    assets = sorted(list(assets))
    print(f"   Assets: {assets}")

    # Create empty correlation matrix
    n = len(assets)
    corr_matrix = pd.DataFrame(np.eye(n), index=assets, columns=assets)

    # Fill in correlations
    for pair, corr in avg_corr.items():
        parts = pair.split('-')
        if len(parts) == 2:
            a1, a2 = parts[0], parts[1]
            if a1 in assets and a2 in assets:
                corr_matrix.loc[a1, a2] = corr
                corr_matrix.loc[a2, a1] = corr  # Symmetric

    print("✅ Correlation matrix created")
    return corr_matrix


def generate_heatmap(corr_matrix, output_path):
    """Generate publication-quality correlation heatmap"""
    print("🎨 Generating heatmap...")

    fig, ax = plt.subplots(figsize=(10, 8))

    sns.heatmap(
        corr_matrix,
        annot=True,
        fmt='.3f',
        cmap='RdYlBu_r',
        center=0,
        vmin=-1,
        vmax=1,
        square=True,
        linewidths=0.5,
        linecolor='white',
        cbar_kws={'label': 'Correlation Coefficient', 'shrink': 0.8},
        annot_kws={'size': 11, 'weight': 'bold'},
        ax=ax
    )

    ax.set_title(
        'Cross-Asset Correlation Matrix\n(30-Day Rolling Average, Dec 2024 - Dec 2025)',
        fontsize=14, fontweight='bold', pad=20
    )

    plt.xticks(rotation=45, ha='right', fontsize=11)
    plt.yticks(rotation=0, fontsize=11)
    plt.tight_layout()

    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.savefig(output_path.replace('.png', '.pdf'), bbox_inches='tight', facecolor='white')

    print(f"✅ Saved: {output_path}")
    print(f"✅ Saved: {output_path.replace('.png', '.pdf')}")
    plt.show()


def generate_correlation_timeseries(df, output_path):
    """Generate correlation over time chart"""
    print("📈 Generating correlation timeseries...")

    df['date'] = pd.to_datetime(df['date'])

    fig, ax = plt.subplots(figsize=(14, 6))

    for pair in df['asset_pair'].unique():
        pair_data = df[df['asset_pair'] == pair].sort_values('date')
        ax.plot(pair_data['date'], pair_data['corr_30d'], label=pair, linewidth=1.5)

    ax.axhline(y=0, color='black', linestyle='--', linewidth=0.8, alpha=0.5)
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('30-Day Rolling Correlation', fontsize=12)
    ax.set_title('Cross-Asset Correlations Over Time', fontsize=14, fontweight='bold')
    ax.legend(loc='upper right', fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.set_ylim(-1, 1)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"✅ Saved: {output_path}")
    plt.show()


def generate_regime_distribution(df, output_path):
    """Generate correlation regime distribution pie chart"""
    print("🥧 Generating regime distribution...")

    regime_counts = df['correlation_regime'].value_counts()

    fig, ax = plt.subplots(figsize=(10, 8))
    colors = sns.color_palette('RdYlBu_r', len(regime_counts))

    ax.pie(
        regime_counts.values,
        labels=regime_counts.index,
        autopct='%1.1f%%',
        colors=colors,
        explode=[0.02] * len(regime_counts),
        shadow=True
    )

    ax.set_title('Distribution of Correlation Regimes\n(All Asset Pairs)', fontsize=14, fontweight='bold')

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"✅ Saved: {output_path}")
    plt.show()


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 60)
    print("CORRELATION HEATMAP GENERATOR")
    print("Data Delta Force - Crypto-Macro Risk Intelligence Platform")
    print("=" * 60)

    # Load data
    df = load_data()

    print(f"\n📊 Data shape: {df.shape}")
    print(f"📊 Asset pairs: {df['asset_pair'].unique()}")

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Generate visualizations
    corr_matrix = create_correlation_matrix(df)
    print(f"\n📋 Correlation Matrix:\n{corr_matrix}")

    generate_heatmap(corr_matrix, os.path.join(OUTPUT_DIR, "correlation_heatmap_full.png"))
    generate_correlation_timeseries(df, os.path.join(OUTPUT_DIR, "correlation_timeseries.png"))
    generate_regime_distribution(df, os.path.join(OUTPUT_DIR, "correlation_regime_distribution.png"))

    print("\n" + "=" * 60)
    print("✅ ALL VISUALIZATIONS COMPLETE")
    print(f"📁 Output folder: {OUTPUT_DIR}/")
    print("=" * 60)


if __name__ == "__main__":
    main()