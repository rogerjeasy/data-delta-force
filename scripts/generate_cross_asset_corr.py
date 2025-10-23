# scripts/generate_cross_asset_corr.py
"""
Generates baseline and rolling cross-asset correlation matrices
for equities, bonds, commodities, and FX.
Outputs:
- data/static/cross_asset_correlation_baseline.csv
- data/static/cross_asset_correlation_rolling.csv
"""

import os
import pandas as pd
import yfinance as yf

# Create output folder
os.makedirs("data/static", exist_ok=True)

# Define tickers (broad market proxies)
tickers = {
    "SP500": "^GSPC",        # US Equities
    "US10Y": "^TNX",         # 10-Year Treasury Yield
    "GOLD": "GC=F",          # Gold Futures
    "EURUSD": "EURUSD=X"     # EUR/USD exchange rate
}

# Download historical data
print(" Downloading market data...")
data = yf.download(
    list(tickers.values()),
    start="2015-01-01",
    end="2025-01-01",
    auto_adjust=False,
    progress=True
)["Close"]
data.columns = tickers.keys()
data = data.dropna()
print(f"Data loaded: {data.shape[0]} rows")

# Compute daily returns
returns = data.pct_change().dropna()

# Baseline correlation (whole period)
baseline_corr = returns.corr()
baseline_corr.to_csv("data/static/cross_asset_correlation_baseline.csv")
print("Baseline correlation matrix saved.")

# Rolling 90-day correlations
print("📈 Computing rolling correlations (90-day window)...")
rolling_corr = returns.rolling(window=90).corr().dropna()
rolling_corr.to_csv("data/static/cross_asset_correlation_rolling.csv")
print("Rolling correlation matrix saved.")

# Optional: Quick summary output
print("\nBaseline Correlation Matrix:")
print(baseline_corr.round(2))
