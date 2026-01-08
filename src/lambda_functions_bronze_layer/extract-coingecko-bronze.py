"""
AWS Lambda Function: Extract CoinGecko Data to S3 Bronze Layer
"""

import json
import boto3
import urllib3
import os
from datetime import datetime
from typing import Dict, Any, List

# Initialize clients
s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')
http = urllib3.PoolManager()

# Configuration from environment variables
S3_BUCKET = os.environ['S3_BUCKET']
SECRET_NAME = os.environ['SECRET_NAME']

# Top 10 cryptocurrencies
TOP_10_COINS = [
    'bitcoin', 'ethereum', 'tether', 'binancecoin', 'ripple',
    'cardano', 'dogecoin', 'solana', 'polkadot', 'matic-network'
]


def get_api_key() -> str:
    """Retrieve CoinGecko API key from Secrets Manager"""
    try:
        response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        return response['SecretString']
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None


def fetch_coingecko_data(api_key: str = None) -> List[Dict[str, Any]]:
    """Fetch market data for top 10 cryptocurrencies"""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    
    # Build query parameters
    params = {
        'vs_currency': 'usd',
        'ids': ','.join(TOP_10_COINS),
        'order': 'market_cap_desc',
        'per_page': '10',
        'page': '1',
        'sparkline': 'false',
        'price_change_percentage': '24h'
    }
    
    # Build URL with parameters
    query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
    full_url = f"{url}?{query_string}"
    
    # Add headers
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'DataDeltaForce-Lambda/1.0'
    }
    
    if api_key:
        headers['x-cg-demo-api-key'] = api_key
    
    try:
        response = http.request('GET', full_url, headers=headers, timeout=30.0)
        
        if response.status != 200:
            raise Exception(f"API returned status code {response.status}")
        
        data = json.loads(response.data.decode('utf-8'))
        print(f"✅ Successfully fetched data for {len(data)} cryptocurrencies")
        return data
        
    except Exception as e:
        print(f"Error fetching CoinGecko data: {e}")
        raise


def save_to_s3_bronze(data: List[Dict[str, Any]], timestamp: datetime) -> Dict[str, str]:
    """Save data to S3 Bronze layer"""
    date_str = timestamp.strftime('%Y-%m-%d')
    datetime_str = timestamp.strftime('%Y%m%d_%H%M%S')
    
    # Prepare data file
    data_key = f"bronze/coingecko/{date_str}/top10_prices_{datetime_str}.json"
    
    # Prepare metadata
    metadata = {
        'extraction_timestamp': timestamp.isoformat(),
        'source': 'coingecko_api',
        'endpoint': '/coins/markets',
        'num_records': len(data),
        'coins': [item['id'] for item in data],
        'data_file': data_key
    }
    
    metadata_key = f"bronze/coingecko/{date_str}/metadata_{datetime_str}.json"
    
    try:
        # Upload data file
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=data_key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json',
            Metadata={
                'source': 'coingecko',
                'extraction_time': timestamp.isoformat(),
                'record_count': str(len(data))
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
    print("🚀 CoinGecko Data Extraction - Bronze Layer")
    print("=" * 80)
    
    timestamp = datetime.utcnow()
    print(f"⏰ Execution time: {timestamp.isoformat()}")
    
    try:
        # Get API key
        api_key = get_api_key()
        
        # Fetch data from CoinGecko
        crypto_data = fetch_coingecko_data(api_key)
        
        # Save to S3 Bronze layer
        s3_keys = save_to_s3_bronze(crypto_data, timestamp)
        
        # Prepare response
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'CoinGecko data extraction successful',
                'timestamp': timestamp.isoformat(),
                'num_records': len(crypto_data),
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
                'message': 'CoinGecko data extraction failed',
                'error': str(e),
                'timestamp': timestamp.isoformat()
            })
        }