"""
API Client Testing Script

This script tests the CoinGecko and FRED API clients with real API calls.
Run this to verify your API keys and basic functionality.

Usage:
    python test_api_clients.py

Before running:
    1. Set environment variables or create .env file:
       - COINGECKO_API_KEY (optional for free tier)
       - FRED_API_KEY (required)
    
    2. Install dependencies:
       pip install python-dotenv requests pandas

Authors: Data Delta Force
Created: September 2025
"""

import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.data_ingestion import CoinGeckoClient, FREDClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_coingecko_client():
    """Test CoinGecko API client functionality."""
    print("\n" + "="*60)
    print("TESTING COINGECKO CLIENT")
    print("="*60 + "\n")
    
    # Load API key if available
    api_key = os.getenv('COINGECKO_API_KEY')
    is_pro_str = os.getenv('COINGECKO_IS_PRO', 'false').lower()
    is_pro = is_pro_str in ('true', '1', 'yes')
    
    try:
        # Initialize client
        print("1. Initializing CoinGecko client...")
        client = CoinGeckoClient(
            api_key=api_key,
            is_pro=is_pro,
            validate_data=True
        )
        print(f"   ✓ Client initialized (Pro: {is_pro})")
        
        # Test 1: Get Bitcoin data
        print("\n2. Testing get_coin_data() - Bitcoin...")
        btc_data = client.get_coin_data("bitcoin")
        print(f"   ✓ Bitcoin (BTC)")
        print(f"   - Current Price: ${btc_data['market_data']['current_price']['usd']:,.2f}")
        print(f"   - Market Cap: ${btc_data['market_data']['market_cap']['usd']:,.0f}")
        print(f"   - 24h Volume: ${btc_data['market_data']['total_volume']['usd']:,.0f}")
        print(f"   - 24h Change: {btc_data['market_data']['price_change_percentage_24h']:.2f}%")
        
        # Test 2: Get market data for top coins
        print("\n3. Testing get_coin_market_data() - Top 5 coins...")
        market_data = client.get_coin_market_data(per_page=5, page=1)
        print(f"   ✓ Retrieved {len(market_data)} coins:")
        for i, coin in enumerate(market_data, 1):
            print(f"   {i}. {coin['name']} ({coin['symbol'].upper()}): ${coin['current_price']:,.2f}")
        
        # Test 3: Get historical data
        print("\n4. Testing get_coin_market_chart() - Bitcoin 7 days...")
        chart_data = client.get_coin_market_chart("bitcoin", days=7)
        print(f"   ✓ Retrieved {len(chart_data['prices'])} price points")
        print(f"   - First price: ${chart_data['prices'][0][1]:,.2f}")
        print(f"   - Last price: ${chart_data['prices'][-1][1]:,.2f}")
        
        # Test 4: Get trending coins
        print("\n5. Testing get_trending_coins()...")
        trending = client.get_trending_coins()
        print(f"   ✓ Trending coins:")
        for i, item in enumerate(trending['coins'][:3], 1):
            coin = item['item']
            print(f"   {i}. {coin['name']} ({coin['symbol']})")
        
        # Test 5: Get global market data
        print("\n6. Testing get_global_data()...")
        global_data = client.get_global_data()
        data = global_data['data']
        print(f"   ✓ Global Market Data:")
        print(f"   - Total Market Cap: ${data['total_market_cap']['usd']:,.0f}")
        print(f"   - Total Volume (24h): ${data['total_volume']['usd']:,.0f}")
        print(f"   - BTC Dominance: {data['market_cap_percentage']['btc']:.2f}%")
        print(f"   - Active Cryptocurrencies: {data['active_cryptocurrencies']}")
        
        # Test 6: Search coins
        print("\n7. Testing search_coins()...")
        search_results = client.search_coins("ethereum")
        print(f"   ✓ Found {len(search_results['coins'])} results for 'ethereum'")
        if search_results['coins']:
            eth = search_results['coins'][0]
            print(f"   - Top result: {eth['name']} ({eth['symbol']})")
        
        # Display rate limit stats
        print("\n8. Rate Limit Statistics:")
        stats = client.get_rate_limit_stats()
        for period, data in stats.items():
            print(f"   - {period.capitalize()}: {data['current_calls']}/{data['max_calls']} "
                  f"({data['utilization_pct']:.1f}% used)")
        
        # Display validation summary
        print("\n9. Validation Summary:")
        val_summary = client.get_validation_summary()
        if val_summary['total_validations'] > 0:
            print(f"   - Total Validations: {val_summary['total_validations']}")
            print(f"   - Passed: {val_summary['passed']}")
            print(f"   - Failed: {val_summary['failed']}")
            print(f"   - Success Rate: {val_summary['success_rate']:.1f}%")
        
        print("\n✓ CoinGecko client tests PASSED\n")
        return True
        
    except Exception as e:
        print(f"\n✗ CoinGecko client tests FAILED: {str(e)}\n")
        logger.exception("CoinGecko test failed")
        return False
    finally:
        if 'client' in locals():
            client.close()


def test_fred_client():
    """Test FRED API client functionality."""
    print("\n" + "="*60)
    print("TESTING FRED CLIENT")
    print("="*60 + "\n")
    
    # Load API key
    api_key = os.getenv('FRED_API_KEY')
    
    if not api_key:
        print("✗ FRED_API_KEY not found in environment variables")
        print("  Get your API key from: https://fred.stlouisfed.org/docs/api/api_key.html")
        return False
    
    try:
        # Initialize client
        print("1. Initializing FRED client...")
        client = FREDClient(api_key=api_key, validate_data=True)
        print("   ✓ Client initialized")
        
        # Test 1: Get CPI data (last 12 months)
        print("\n2. Testing get_series() - Consumer Price Index...")
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        cpi_data = client.get_series(
            'CPIAUCSL',
            observation_start=start_date,
            observation_end=end_date
        )
        print(f"   ✓ Retrieved {len(cpi_data)} CPI observations")
        latest = cpi_data.iloc[-1]
        print(f"   - Latest ({latest['date'].strftime('%Y-%m-%d')}): {latest['value']:.2f}")
        
        # Test 2: Get Federal Funds Rate
        print("\n3. Testing get_series() - Federal Funds Rate...")
        ffr_data = client.get_series(
            'DFF',
            observation_start=start_date,
            observation_end=end_date
        )
        print(f"   ✓ Retrieved {len(ffr_data)} FFR observations")
        latest = ffr_data.iloc[-1]
        print(f"   - Latest ({latest['date'].strftime('%Y-%m-%d')}): {latest['value']:.2f}%")
        
        # Test 3: Get unemployment rate
        print("\n4. Testing get_series() - Unemployment Rate...")
        unemp_data = client.get_series(
            'UNRATE',
            observation_start=start_date,
            observation_end=end_date
        )
        print(f"   ✓ Retrieved {len(unemp_data)} unemployment observations")
        latest = unemp_data.iloc[-1]
        print(f"   - Latest ({latest['date'].strftime('%Y-%m-%d')}): {latest['value']:.1f}%")
        
        # Test 4: Get series info
        print("\n5. Testing get_series_info()...")
        info = client.get_series_info('CPIAUCSL')
        print(f"   ✓ Series Info:")
        print(f"   - Title: {info['title']}")
        print(f"   - Units: {info['units']}")
        print(f"   - Frequency: {info['frequency']}")
        print(f"   - Last Updated: {info['last_updated']}")
        
        # Test 5: Search series
        print("\n6. Testing search_series()...")
        results = client.search_series("inflation", limit=5)
        print(f"   ✓ Found {len(results)} series for 'inflation':")
        for i, series in enumerate(results[:3], 1):
            print(f"   {i}. {series['id']}: {series['title']}")
        
        # Test 6: Get multiple series
        print("\n7. Testing get_multiple_series()...")
        series_ids = ['DFF', 'CPIAUCSL', 'UNRATE']
        multi_data = client.get_multiple_series(
            series_ids,
            observation_start=start_date,
            observation_end=end_date
        )
        print(f"   ✓ Retrieved data for {len(series_ids)} series")
        print(f"   - DataFrame shape: {multi_data.shape}")
        print(f"   - Columns: {', '.join(multi_data.columns)}")
        
        # Test 7: Get common series
        print("\n8. Testing get_common_series()...")
        common_data = client.get_common_series(
            series_names=['fed_funds_rate', 'cpi', 'unemployment_rate'],
            observation_start=start_date
        )
        print(f"   ✓ Retrieved common economic indicators")
        print(f"   - Shape: {common_data.shape}")
        
        # Test 8: Calculate growth rate
        print("\n9. Testing calculate_growth_rate()...")
        cpi_growth = client.calculate_growth_rate(
            'CPIAUCSL',
            periods=12,
            observation_start=start_date
        )
        latest_growth = cpi_growth.dropna().iloc[-1]
        print(f"   ✓ CPI YoY Growth Rate:")
        print(f"   - Latest ({latest_growth['date'].strftime('%Y-%m-%d')}): {latest_growth['growth_rate']:.2f}%")
        
        # Test 9: Get latest observation
        print("\n10. Testing get_latest_observation()...")
        latest_obs = client.get_latest_observation('DFF')
        print(f"   ✓ Latest Federal Funds Rate:")
        print(f"   - Date: {latest_obs['date'].strftime('%Y-%m-%d')}")
        print(f"   - Value: {latest_obs['value']:.2f}%")
        
        # Test 10: List common series
        print("\n11. Testing list_common_series()...")
        common_series = FREDClient.list_common_series()
        print(f"   ✓ Available common series: {len(common_series)}")
        print(f"   - Sample: {list(common_series.keys())[:5]}")
        
        # Display rate limit stats
        print("\n12. Rate Limit Statistics:")
        stats = client.get_rate_limit_stats()
        for period, data in stats.items():
            print(f"   - {period.capitalize()}: {data['current_calls']}/{data['max_calls']} "
                  f"({data['utilization_pct']:.1f}% used)")
        
        # Display validation summary
        print("\n13. Validation Summary:")
        val_summary = client.get_validation_summary()
        if val_summary['total_validations'] > 0:
            print(f"   - Total Validations: {val_summary['total_validations']}")
            print(f"   - Passed: {val_summary['passed']}")
            print(f"   - Failed: {val_summary['failed']}")
            print(f"   - Success Rate: {val_summary['success_rate']:.1f}%")
        
        print("\n✓ FRED client tests PASSED\n")
        return True
        
    except Exception as e:
        print(f"\n✗ FRED client tests FAILED: {str(e)}\n")
        logger.exception("FRED test failed")
        return False
    finally:
        if 'client' in locals():
            client.close()


def main():
    """Run all API client tests."""
    print("\n" + "="*60)
    print("API CLIENT TESTING SUITE")
    print("Data Delta Force - Macro-Crypto Risk Intelligence Platform")
    print("="*60)
    
    # Load environment variables
    load_dotenv()
    
    # Run tests
    coingecko_passed = test_coingecko_client()
    fred_passed = test_fred_client()
    
    # Summary
    print("="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"CoinGecko Client: {'✓ PASSED' if coingecko_passed else '✗ FAILED'}")
    print(f"FRED Client: {'✓ PASSED' if fred_passed else '✗ FAILED'}")
    print("="*60 + "\n")
    
    # Exit code
    sys.exit(0 if (coingecko_passed and fred_passed) else 1)


if __name__ == "__main__":
    main()