"""
Fetch CoinGecko Data → S3
==========================

This script fetches cryptocurrency price data from the CoinGecko API
and saves it DIRECTLY to S3 without storing locally (Phase 4!).

Demonstrates automated data pipeline for dynamic data sources.

Usage:
    python scripts/fetch_coingecko_to_s3.py
"""

import boto3
import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Load environment variables
load_dotenv()

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

# S3 Configuration
BUCKET_NAME = 'crypto-macro-datalake-ddf'
BRONZE_COINGECKO_PREFIX = 'bronze/coingecko/'

# CoinGecko API Configuration
COINGECKO_API_URL = 'https://api.coingecko.com/api/v3'


def create_s3_client():
    """Create and return an S3 client"""
    print("🔑 Loading AWS credentials...")

    if not AWS_ACCESS_KEY_ID:
        raise ValueError("❌ AWS_ACCESS_KEY_ID not found in .env file!")

    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        region_name=AWS_REGION
    )

    print("✅ S3 client created successfully!")
    return s3_client


def fetch_coin_data(coin_id):
    """
    Fetch current market data for a coin from CoinGecko

    Args:
        coin_id: CoinGecko coin ID (e.g., 'bitcoin', 'ethereum')

    Returns:
        dict: Market data or None if error
    """
    print(f"\n📡 Fetching data for {coin_id} from CoinGecko API...")

    url = f"{COINGECKO_API_URL}/coins/{coin_id}"
    params = {
        'localization': 'false',
        'tickers': 'false',
        'market_data': 'true',
        'community_data': 'false',
        'developer_data': 'false'
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()

        # Extract relevant market data
        market_data = {
            'coin_id': coin_id,
            'symbol': data.get('symbol', '').upper(),
            'name': data.get('name', ''),
            'timestamp': datetime.utcnow().isoformat(),
            'current_price_usd': data['market_data']['current_price'].get('usd'),
            'market_cap_usd': data['market_data'].get('market_cap', {}).get('usd'),
            'total_volume_usd': data['market_data'].get('total_volume', {}).get('usd'),
            'price_change_24h': data['market_data'].get('price_change_24h'),
            'price_change_percentage_24h': data['market_data'].get('price_change_percentage_24h'),
            'market_cap_rank': data.get('market_cap_rank'),
            'circulating_supply': data['market_data'].get('circulating_supply'),
            'total_supply': data['market_data'].get('total_supply'),
            'ath_usd': data['market_data'].get('ath', {}).get('usd'),
            'atl_usd': data['market_data'].get('atl', {}).get('usd')
        }

        print(f"✅ Fetched {coin_id}: ${market_data['current_price_usd']:,.2f}")
        return market_data

    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {e}")
        return None
    except (KeyError, TypeError) as e:
        print(f"❌ Error parsing response: {e}")
        return None


def fetch_multiple_coins_simple(coin_ids):
    """
    Fetch simple price data for multiple coins at once

    Args:
        coin_ids: List of coin IDs

    Returns:
        dict: Price data for all coins
    """
    print(f"\n📡 Fetching data for {len(coin_ids)} coins from CoinGecko API...")

    # CoinGecko simple price endpoint (faster for multiple coins)
    url = f"{COINGECKO_API_URL}/simple/price"
    params = {
        'ids': ','.join(coin_ids),
        'vs_currencies': 'usd',
        'include_market_cap': 'true',
        'include_24hr_vol': 'true',
        'include_24hr_change': 'true'
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()

        # Transform to consistent format
        result = {
            'timestamp': datetime.utcnow().isoformat(),
            'coins': []
        }

        for coin_id, coin_data in data.items():
            result['coins'].append({
                'coin_id': coin_id,
                'price_usd': coin_data.get('usd'),
                'market_cap_usd': coin_data.get('usd_market_cap'),
                'volume_24h_usd': coin_data.get('usd_24h_vol'),
                'price_change_24h_percent': coin_data.get('usd_24h_change')
            })

        print(f"✅ Fetched {len(result['coins'])} coins successfully")
        return result

    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {e}")
        return None


def save_to_s3(s3_client, data, s3_key):
    """
    Save data directly to S3 (Phase 4 - no local file!)

    Args:
        s3_client: boto3 S3 client
        data: Dictionary data to save
        s3_key: S3 destination key
    """
    print(f"\n⬆️  Saving to S3: s3://{BUCKET_NAME}/{s3_key}")

    try:
        # Convert data to JSON string
        json_data = json.dumps(data, indent=2)

        # Upload directly to S3 (no local file!)
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json_data.encode('utf-8'),
            ContentType='application/json'
        )

        data_size_kb = len(json_data) / 1024
        print(f"✅ Saved successfully! ({data_size_kb:.2f} KB)")
        return True

    except ClientError as e:
        print(f"❌ Upload failed: {e}")
        return False


def main():
    """
    Main function to fetch CoinGecko data and save to S3
    """
    print("=" * 80)
    print("🚀 Fetch CoinGecko Data → S3 Data Lake")
    print("=" * 80)

    # Create S3 client
    try:
        s3 = create_s3_client()
    except Exception as e:
        print(f"❌ Failed to create S3 client: {e}")
        return

    # Example 1: Fetch detailed data for Bitcoin
    print("\n" + "=" * 80)
    print("📊 Example 1: Fetch Detailed Bitcoin Data")
    print("=" * 80)

    btc_data = fetch_coin_data('bitcoin')

    if btc_data:
        # Generate timestamped filename
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        s3_key = f"{BRONZE_COINGECKO_PREFIX}bitcoin_detailed_{timestamp}.json"

        save_to_s3(s3, btc_data, s3_key)

    # Example 2: Fetch multiple coins (Top 10)
    print("\n" + "=" * 80)
    print("📊 Example 2: Fetch Top 10 Crypto Prices")
    print("=" * 80)

    top_10_coins = [
        'bitcoin',
        'ethereum',
        'tether',
        'binancecoin',
        'solana',
        'usd-coin',
        'ripple',
        'dogecoin',
        'toncoin',
        'cardano'
    ]

    multi_coin_data = fetch_multiple_coins_simple(top_10_coins)

    if multi_coin_data:
        # Generate timestamped filename
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        s3_key = f"{BRONZE_COINGECKO_PREFIX}top10_prices_{timestamp}.json"

        save_to_s3(s3, multi_coin_data, s3_key)

        # Print summary
        print("\n💰 Current Prices:")
        for coin in multi_coin_data['coins']:
            price = coin['price_usd']
            change = coin['price_change_24h_percent']
            change_symbol = "📈" if change and change > 0 else "📉"
            print(f"   {coin['coin_id']:15} ${price:>12,.2f}  {change_symbol} {change:>6.2f}%")

    # Summary
    print("\n" + "=" * 80)
    print("✅ Data Pipeline Complete!")
    print("=" * 80)
    print("\n📋 Summary:")
    print(f"   - Bucket: {BUCKET_NAME}")
    print(f"   - Location: {BRONZE_COINGECKO_PREFIX}")
    print(f"   - Files created: 2 (bitcoin_detailed + top10_prices)")
    print(f"   - Storage: Bronze Layer (Raw API Data)")
    print("\n💡 Key Achievement:")
    print("   ✅ API → S3 Direct Pipeline (Phase 4!)")
    print("   ✅ No local file storage required")
    print("   ✅ Automated data collection")
    print("\n🎯 Next Steps:")
    print("   - Automate with cron/Airflow for daily updates")
    print("   - Transform to Silver layer (cleaned data)")
    print("   - Build correlations in Gold layer")


if __name__ == "__main__":
    main()