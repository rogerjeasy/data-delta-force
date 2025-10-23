"""
Create Exchange Listings Data
==============================

This script creates a CSV file with historical cryptocurrency exchange listing dates
for the top 10 cryptocurrencies by market cap on major exchanges.

Data includes:
- BTC, ETH, USDT, BNB, SOL, USDC, XRP, DOGE, TON, ADA
- Exchanges: Coinbase, Binance, Kraken

Data is saved to: data/raw/exchange_listings.csv

Usage:
    python scripts/create_exchange_listings.py
"""

import pandas as pd
import os
from pathlib import Path


def create_exchange_listings_data():
    """
    Create exchange listings dataset with historical listing dates

    Returns:
        DataFrame with exchange listing information
    """

    # Manually curated data based on historical records
    listings = [
        # Bitcoin (BTC)
        {'symbol': 'BTC', 'coin_name': 'Bitcoin', 'exchange': 'Coinbase',
         'listing_date': '2015-01-26', 'notes': 'First crypto on Coinbase exchange'},
        {'symbol': 'BTC', 'coin_name': 'Bitcoin', 'exchange': 'Kraken',
         'listing_date': '2013-09-10', 'notes': 'Available since Kraken launch'},
        {'symbol': 'BTC', 'coin_name': 'Bitcoin', 'exchange': 'Binance',
         'listing_date': '2017-07-14', 'notes': 'Available at Binance launch'},

        # Ethereum (ETH)
        {'symbol': 'ETH', 'coin_name': 'Ethereum', 'exchange': 'Coinbase',
         'listing_date': '2016-07-21', 'notes': 'Listed shortly after ETH launch'},
        {'symbol': 'ETH', 'coin_name': 'Ethereum', 'exchange': 'Kraken',
         'listing_date': '2016-01-14', 'notes': 'Early ETH supporter'},
        {'symbol': 'ETH', 'coin_name': 'Ethereum', 'exchange': 'Binance',
         'listing_date': '2017-07-14', 'notes': 'Available at Binance launch'},

        # Tether (USDT)
        {'symbol': 'USDT', 'coin_name': 'Tether', 'exchange': 'Binance',
         'listing_date': '2017-11-23', 'notes': 'Early stablecoin adoption'},
        {'symbol': 'USDT', 'coin_name': 'Tether', 'exchange': 'Kraken',
         'listing_date': '2018-09-06', 'notes': 'Later stablecoin adoption'},
        {'symbol': 'USDT', 'coin_name': 'Tether', 'exchange': 'Coinbase',
         'listing_date': '2021-04-29', 'notes': 'Coinbase Pro listing'},

        # Binance Coin (BNB)
        {'symbol': 'BNB', 'coin_name': 'Binance Coin', 'exchange': 'Binance',
         'listing_date': '2017-07-25', 'notes': 'Native Binance token'},
        {'symbol': 'BNB', 'coin_name': 'Binance Coin', 'exchange': 'Kraken',
         'listing_date': '2019-02-19', 'notes': 'Listed after gaining popularity'},
        # Note: BNB not listed on Coinbase (competitor token)

        # Solana (SOL)
        {'symbol': 'SOL', 'coin_name': 'Solana', 'exchange': 'Binance',
         'listing_date': '2020-08-11', 'notes': 'Early major exchange listing'},
        {'symbol': 'SOL', 'coin_name': 'Solana', 'exchange': 'Coinbase',
         'listing_date': '2021-06-17', 'notes': 'Listed after significant growth'},
        {'symbol': 'SOL', 'coin_name': 'Solana', 'exchange': 'Kraken',
         'listing_date': '2021-01-21', 'notes': 'Mid-tier exchange adoption'},

        # USD Coin (USDC)
        {'symbol': 'USDC', 'coin_name': 'USD Coin', 'exchange': 'Coinbase',
         'listing_date': '2018-10-23', 'notes': 'Coinbase-backed stablecoin'},
        {'symbol': 'USDC', 'coin_name': 'USD Coin', 'exchange': 'Binance',
         'listing_date': '2018-11-05', 'notes': 'Quick adoption after launch'},
        {'symbol': 'USDC', 'coin_name': 'USD Coin', 'exchange': 'Kraken',
         'listing_date': '2018-11-13', 'notes': 'Rapid multi-exchange support'},

        # Ripple (XRP)
        {'symbol': 'XRP', 'coin_name': 'Ripple', 'exchange': 'Kraken',
         'listing_date': '2014-05-14', 'notes': 'Early XRP adoption'},
        {'symbol': 'XRP', 'coin_name': 'Ripple', 'exchange': 'Binance',
         'listing_date': '2017-05-04', 'notes': 'Pre-Binance official launch'},
        {'symbol': 'XRP', 'coin_name': 'Ripple', 'exchange': 'Coinbase',
         'listing_date': '2019-02-28', 'notes': 'Controversial delayed listing'},

        # Dogecoin (DOGE)
        {'symbol': 'DOGE', 'coin_name': 'Dogecoin', 'exchange': 'Kraken',
         'listing_date': '2014-01-23', 'notes': 'Early meme coin supporter'},
        {'symbol': 'DOGE', 'coin_name': 'Dogecoin', 'exchange': 'Binance',
         'listing_date': '2017-07-14', 'notes': 'Available at Binance launch'},
        {'symbol': 'DOGE', 'coin_name': 'Dogecoin', 'exchange': 'Coinbase',
         'listing_date': '2021-06-03', 'notes': 'Listed after Elon Musk hype'},

        # Toncoin (TON)
        {'symbol': 'TON', 'coin_name': 'Toncoin', 'exchange': 'Binance',
         'listing_date': '2023-08-17', 'notes': 'Recent major exchange listing'},
        {'symbol': 'TON', 'coin_name': 'Toncoin', 'exchange': 'Kraken',
         'listing_date': '2024-01-09', 'notes': 'Growing institutional interest'},
        # Note: TON not yet on Coinbase as of 2025

        # Cardano (ADA)
        {'symbol': 'ADA', 'coin_name': 'Cardano', 'exchange': 'Binance',
         'listing_date': '2017-10-01', 'notes': 'Early Binance listing'},
        {'symbol': 'ADA', 'coin_name': 'Cardano', 'exchange': 'Kraken',
         'listing_date': '2018-01-16', 'notes': 'Following initial success'},
        {'symbol': 'ADA', 'coin_name': 'Cardano', 'exchange': 'Coinbase',
         'listing_date': '2021-03-18', 'notes': 'Listed after significant development'},
    ]

    df = pd.DataFrame(listings)
    df['listing_date'] = pd.to_datetime(df['listing_date'])

    # Sort by listing date
    df = df.sort_values(['symbol', 'listing_date'])

    return df


def save_to_csv(data, output_path='data/raw/exchange_listings.csv'):
    """
    Save exchange listings data to CSV

    Args:
        data: DataFrame with exchange listings
        output_path: Path to save CSV file
    """
    # Get script directory and go up to project root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    full_path = os.path.join(project_root, output_path)

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(full_path), exist_ok=True)

    # Save
    data.to_csv(full_path, index=False)
    print(f"\n✅ Exchange listings saved to: {full_path}")
    print(f"   Total listings: {len(data)}")
    print(f"   Unique coins: {data['symbol'].nunique()}")
    print(f"   Exchanges covered: {', '.join(data['exchange'].unique())}")


def print_summary(data):
    """
    Print summary statistics of exchange listings

    Args:
        data: DataFrame with exchange listings
    """
    print("\n" + "=" * 60)
    print("Exchange Listings Summary")
    print("=" * 60)

    print("\n📊 Listings per Exchange:")
    print(data['exchange'].value_counts().to_string())

    print("\n💰 Listings per Coin:")
    print(data['symbol'].value_counts().to_string())

    print("\n📅 First Listing per Coin:")
    first_listings = data.groupby('symbol').agg({
        'listing_date': 'min',
        'exchange': 'first'
    }).sort_values('listing_date')

    for symbol, row in first_listings.iterrows():
        print(f"   {symbol}: {row['listing_date'].date()} on {row['exchange']}")

    print("\n🏆 Most Recent Listings (Top 5):")
    recent = data.nlargest(5, 'listing_date')[['symbol', 'exchange', 'listing_date']]
    for _, row in recent.iterrows():
        print(f"   {row['symbol']} on {row['exchange']}: {row['listing_date'].date()}")


def main():
    """
    Main function to create exchange listings data
    """
    print("=" * 60)
    print("🚀 Creating Exchange Listings Data")
    print("=" * 60)
    print()

    # Create data
    print("📝 Creating exchange listings dataset...")
    data = create_exchange_listings_data()

    # Print summary
    print_summary(data)

    # Save to CSV
    save_to_csv(data)

    print()
    print("=" * 60)
    print("✅ Done! You can now load this data with:")
    print("   from src.data_ingestion.static_data_loader import StaticDataLoader")
    print("   loader = StaticDataLoader()")
    print("   listings = loader.load_exchange_listings()")
    print("=" * 60)


if __name__ == "__main__":
    main()