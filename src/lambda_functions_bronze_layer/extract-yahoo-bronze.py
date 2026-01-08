"""
AWS Lambda Function: Extract Yahoo Finance Data to S3 Bronze Layer
"""

import json
import boto3
import urllib3
import os
from datetime import datetime, timedelta
from typing import Dict, Any

# Initialize clients
s3_client = boto3.client('s3')
http = urllib3.PoolManager()

# Configuration
S3_BUCKET = os.environ['S3_BUCKET']

# Tickers to fetch
TICKERS = {
    'sp500': '^GSPC',
    'vix': '^VIX',
    'gold': 'GC=F',
    'us10y': '^TNX',
    'dxy': 'DX-Y.NYB'
}


def fetch_yahoo_data(ticker: str) -> Dict[str, Any]:
    """Fetch data from Yahoo Finance"""
    period1 = int((datetime.now() - timedelta(days=5)).timestamp())
    period2 = int(datetime.now().timestamp())
    
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = f"?period1={period1}&period2={period2}&interval=1d&events=div,splits"
    full_url = url + params
    
    headers = {
        'User-Agent': 'Mozilla/5.0'
    }
    
    try:
        response = http.request('GET', full_url, headers=headers, timeout=30.0)
        
        if response.status != 200:
            raise Exception(f"API returned status code {response.status}")
        
        data = json.loads(response.data.decode('utf-8'))
        
        if 'chart' in data and 'result' in data['chart']:
            result = data['chart']['result'][0]
            
            # Extract timestamps and prices
            timestamps = result.get('timestamp', [])
            quotes = result.get('indicators', {}).get('quote', [{}])[0]
            
            processed_data = {
                'ticker': ticker,
                'meta': result.get('meta', {}),
                'data_points': []
            }
            
            for i, ts in enumerate(timestamps):
                processed_data['data_points'].append({
                    'timestamp': datetime.fromtimestamp(ts).isoformat(),
                    'open': quotes.get('open', [])[i] if i < len(quotes.get('open', [])) else None,
                    'high': quotes.get('high', [])[i] if i < len(quotes.get('high', [])) else None,
                    'low': quotes.get('low', [])[i] if i < len(quotes.get('low', [])) else None,
                    'close': quotes.get('close', [])[i] if i < len(quotes.get('close', [])) else None,
                    'volume': quotes.get('volume', [])[i] if i < len(quotes.get('volume', [])) else None
                })
            
            print(f"✅ Fetched {len(processed_data['data_points'])} data points for {ticker}")
            return processed_data
            
        else:
            print(f"⚠️ No data found for {ticker}")
            return {'ticker': ticker, 'data_points': []}
            
    except Exception as e:
        print(f"❌ Error fetching {ticker}: {e}")
        return {'ticker': ticker, 'data_points': [], 'error': str(e)}


def fetch_all_yahoo_data() -> Dict[str, Any]:
    """Fetch data for all configured tickers"""
    all_data = {}
    
    for name, ticker in TICKERS.items():
        ticker_data = fetch_yahoo_data(ticker)
        all_data[name] = ticker_data
    
    return all_data


def save_to_s3_bronze(data: Dict[str, Any], timestamp: datetime) -> Dict[str, str]:
    """Save Yahoo Finance data to S3 Bronze layer"""
    date_str = timestamp.strftime('%Y-%m-%d')
    datetime_str = timestamp.strftime('%Y%m%d_%H%M%S')
    
    # Prepare data file
    data_key = f"bronze/yahoo/{date_str}/market_data_{datetime_str}.json"
    
    # Prepare metadata
    metadata = {
        'extraction_timestamp': timestamp.isoformat(),
        'source': 'yahoo_finance',
        'num_tickers': len(data),
        'tickers': list(data.keys()),
        'total_data_points': sum(len(item.get('data_points', [])) for item in data.values()),
        'data_file': data_key
    }
    
    metadata_key = f"bronze/yahoo/{date_str}/metadata_{datetime_str}.json"
    
    try:
        # Upload data file
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=data_key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json',
            Metadata={
                'source': 'yahoo_finance',
                'extraction_time': timestamp.isoformat(),
                'ticker_count': str(len(data))
            }
        )
        print(f"✅ Data uploaded to s3://{S3_BUCKET}/{data_key}")
        
        # Upload metadata file
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2),
            ContentType='application/json'
        )
        print(f"✅ Metadata uploaded to s3://{S3_BUCKET}/{metadata_key}")
        
        return {
            'data_key': data_key,
            'metadata_key': metadata_key
        }
        
    except Exception as e:
        print(f"❌ Error uploading to S3: {e}")
        raise


def lambda_handler(event, context):
    """AWS Lambda handler function"""
    print("=" * 80)
    print("🚀 Yahoo Finance Data Extraction - Bronze Layer")
    print("=" * 80)
    
    timestamp = datetime.utcnow()
    print(f"⏰ Execution time: {timestamp.isoformat()}")
    
    try:
        # Fetch all market data
        market_data = fetch_all_yahoo_data()
        
        # Save to S3 Bronze layer
        s3_keys = save_to_s3_bronze(market_data, timestamp)
        
        # Count total data points
        total_points = sum(len(item.get('data_points', [])) for item in market_data.values())
        
        # Prepare response
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Yahoo Finance data extraction successful',
                'timestamp': timestamp.isoformat(),
                'num_tickers': len(market_data),
                'total_data_points': total_points,
                's3_data_key': s3_keys['data_key'],
                's3_metadata_key': s3_keys['metadata_key']
            })
        }
        
        print("=" * 80)
        print("✅ Extraction completed successfully")
        print("=" * 80)
        
        return response
        
    except Exception as e:
        print(f"❌ Lambda execution failed: {e}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Yahoo Finance data extraction failed',
                'error': str(e),
                'timestamp': timestamp.isoformat()
            })
        }