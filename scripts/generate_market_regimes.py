
"""
Generate Market Regime Classification Data
===========================================

This script generates historical market regime data for the period 2020-2025.
It identifies:
- Bull/Bear markets based on S&P 500 drawdowns (20% threshold)
- Volatility regimes based on VIX levels
- Risk-on/Risk-off periods

Data is saved to: data/raw/market_regimes.csv

Usage:
    python scripts/generate_market_regimes.py
"""

import pandas as pd
import yfinance as yf
from datetime import datetime
import os


def download_market_data(start_date='2020-01-01', end_date='2025-12-31'):
    """
    Download S&P 500 and VIX data from Yahoo Finance

    Args:
        start_date: Start date for historical data
        end_date: End date for historical data

    Returns:
        DataFrame with Date, SP500_Close, VIX_Close
    """
    print(f"📥 Downloading S&P 500 data from {start_date} to {end_date}...")
    sp500 = yf.download('^GSPC', start=start_date, end=end_date, progress=False, auto_adjust=True)

    print("📥 Downloading VIX data...")
    vix = yf.download('^VIX', start=start_date, end=end_date, progress=False, auto_adjust=True)

    # Reset index to get dates as column
    sp500 = sp500.reset_index()
    vix = vix.reset_index()

    # Combine data - handle both single and multi-level column formats
    if isinstance(sp500.columns, pd.MultiIndex):
        sp500_close = sp500['Close'].iloc[:, 0]
    else:
        sp500_close = sp500['Close']

    if isinstance(vix.columns, pd.MultiIndex):
        vix_close = vix['Close'].iloc[:, 0]
    else:
        vix_close = vix['Close']

    data = pd.DataFrame({
        'date': sp500['Date'],
        'sp500_close': sp500_close,
        'vix_close': vix_close
    })

    # Drop any rows with missing data
    data = data.dropna()

    print(f"✅ Downloaded {len(data)} days of data")
    return data

def calculate_drawdown(prices):
    """
    Calculate drawdown from peak

    Args:
        prices: Series of prices

    Returns:
        Series of drawdown percentages
    """
    cummax = prices.cummax()
    drawdown = (prices - cummax) / cummax * 100
    return drawdown

def classify_bull_bear(data):
    """
    Classify Bull/Bear markets based on 20% drawdown threshold

    Args:
        data: DataFrame with sp500_close column

    Returns:
        DataFrame with added bull_bear_regime column
    """
    print("📊 Classifying Bull/Bear markets (20% drawdown threshold)...")

    data['drawdown'] = calculate_drawdown(data['sp500_close'])

    # Bear market: drawdown >= -20%
    # Bull market: drawdown > -20%
    data['bull_bear_regime'] = data['drawdown'].apply(
        lambda x: 'Bear' if x <= -20 else 'Bull'
    )

    bull_days = (data['bull_bear_regime'] == 'Bull').sum()
    bear_days = (data['bull_bear_regime'] == 'Bear').sum()

    print(f"   Bull market days: {bull_days} ({bull_days / len(data) * 100:.1f}%)")
    print(f"   Bear market days: {bear_days} ({bear_days / len(data) * 100:.1f}%)")
    return data

def classify_vix_regime(data):
    """
    Classify volatility regimes based on VIX levels

    VIX < 20: Low Volatility (Risk-on)
    VIX 20-30: Medium Volatility
    VIX > 30: High Volatility (Risk-off)

    Args:
        data: DataFrame with vix_close column

    Returns:
        DataFrame with added vix_regime column
    """
    print("📊 Classifying VIX volatility regimes...")

    def vix_category(vix):
        if vix < 20:
            return 'Low_Volatility'
        elif vix < 30:
            return 'Medium_Volatility'
        else:
            return 'High_Volatility'

    data['vix_regime'] = data['vix_close'].apply(vix_category)

    low = (data['vix_regime'] == 'Low_Volatility').sum()
    med = (data['vix_regime'] == 'Medium_Volatility').sum()
    high = (data['vix_regime'] == 'High_Volatility').sum()

    print(f"   Low volatility days: {low} ({low /len(data) *100:.1f}%)")
    print(f"   Medium volatility days: {med} ({med /len(data) *100:.1f}%)")
    print(f"   High volatility days: {high} ({high /len(data) *100:.1f}%)")

    return data

def create_combined_regime(data):
    """
    Create combined market regime classification

    Args:
        data: DataFrame with bull_bear_regime and vix_regime

    Returns:
        DataFrame with added market_regime column
    """
    print("📊 Creating combined market regime...")

    def combined_regime(row):
        if row['bull_bear_regime'] == 'Bull' and row['vix_regime'] == 'Low_Volatility':
            return 'Bull_Low_Vol'
        elif row['bull_bear_regime'] == 'Bull' and row['vix_regime'] in ['Medium_Volatility', 'High_Volatility']:
            return 'Bull_High_Vol'
        elif row['bull_bear_regime'] == 'Bear' and row['vix_regime'] == 'High_Volatility':
            return 'Bear_High_Vol'
        else:
            return 'Bear_Medium_Vol'

    data['market_regime'] = data.apply(combined_regime, axis=1)

    print("   Regime distribution:")
    for regime in data['market_regime'].unique():
        count = (data['market_regime'] == regime).sum()
        print(f'- {regime}: {count} days ({count / len(data) * 100:.1f}%)')

    return data


def save_to_csv(data, output_path='data/raw/market_regimes.csv'):
    """
    Save market regime data to CSV

    Args:
        data: DataFrame with regime classifications
        output_path: Path to save CSV file
    """
    # Get script directory and go up to project root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)  # Go up from scripts/ to Project/
    full_path = os.path.join(project_root, output_path)

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(full_path), exist_ok=True)

    # Select final columns
    final_data = data[[
        'date',
        'sp500_close',
        'vix_close',
        'drawdown',
        'bull_bear_regime',
        'vix_regime',
        'market_regime'
    ]].copy()

    # Save
    final_data.to_csv(full_path, index=False)
    print(f"\n✅ Market regimes saved to: {full_path}")
    print(f"   Total rows: {len(final_data)}")
    print(f"   Date range: {final_data['date'].min()} to {final_data['date'].max()}")

def main():
    """
    Main function to generate market regime data
    """
    print("=" * 60)
    print("🚀 Generating Market Regime Classification Data")
    print("=" * 60)
    print()

    # Download data
    data = download_market_data(start_date='2020-01-01', end_date='2025-12-31')

    # Calculate regimes
    data = classify_bull_bear(data)
    data = classify_vix_regime(data)
    data = create_combined_regime(data)

    # Save to CSV
    save_to_csv(data)

    print()
    print("=" * 60)
    print("✅ Done! You can now load this data with:")
    print("   from src.data_ingestion.static_data_loader import StaticDataLoader")
    print("   loader = StaticDataLoader()")
    print("   regimes = loader.load_market_regimes()")
    print("=" * 60)

if __name__ == "__main__":
    main()