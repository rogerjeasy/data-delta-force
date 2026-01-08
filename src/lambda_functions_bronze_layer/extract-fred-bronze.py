"""
AWS Lambda Function: Extract FRED Macro Data to S3 Bronze Layer
"""

import json
import boto3
import urllib3
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List

# Initialize clients
s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')
http = urllib3.PoolManager()

# Configuration
S3_BUCKET = os.environ['S3_BUCKET']
SECRET_NAME = os.environ['SECRET_NAME']
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# Key macro series to fetch
MACRO_SERIES = {
    'fed_funds_rate': 'DFF',
    'cpi': 'CPIAUCSL',
    'unemployment_rate': 'UNRATE',
    'gdp': 'GDP',
    '10y_treasury': 'DGS10',
    '2y_treasury': 'DGS2',
    'core_cpi': 'CPILFESL',
    'pce': 'PCE'
}


def get_api_key() -> str:
    """Retrieve FRED API key from Secrets Manager"""
    try:
        response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        return response['SecretString']
    except Exception as e:
        print(f"Error retrieving FRED API key: {e}")
        raise


def fetch_fred_series(series_id: str, api_key: str, days_back: int = 90) -> Dict[str, Any]:
    """Fetch a single FRED series"""
    observation_start = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    
    # Build URL with parameters
    params = {
        'series_id': series_id,
        'api_key': api_key,
        'file_type': 'json',
        'observation_start': observation_start
    }
    
    query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
    full_url = f"{FRED_BASE_URL}?{query_string}"
    
    try:
        response = http.request('GET', full_url, timeout=30.0)
        
        if response.status != 200:
            raise Exception(f"API returned status code {response.status}")
        
        data = json.loads(response.data.decode('utf-8'))
        
        if 'observations' in data:
            print(f"✅ Fetched {len(data['observations'])} observations for {series_id}")
            return {
                'series_id': series_id,
                'observations': data['observations'],
                'count': data.get('count', len(data['observations']))
            }
        else:
            print(f"⚠️ No observations found for {series_id}")
            return {'series_id': series_id, 'observations': [], 'count': 0}
            
    except Exception as e:
        print(f"❌ Error fetching {series_id}: {e}")
        return {'series_id': series_id, 'observations': [], 'count': 0, 'error': str(e)}


def fetch_all_macro_data(api_key: str) -> Dict[str, Any]:
    """Fetch all configured macro series"""
    all_data = {}
    
    for series_name, series_id in MACRO_SERIES.items():
        series_data = fetch_fred_series(series_id, api_key)
        all_data[series_name] = series_data
    
    return all_data


def save_to_s3_bronze(data: Dict[str, Any], timestamp: datetime) -> Dict[str, str]:
    """Save FRED data to S3 Bronze layer"""
    date_str = timestamp.strftime('%Y-%m-%d')
    datetime_str = timestamp.strftime('%Y%m%d_%H%M%S')
    
    # Prepare data file
    data_key = f"bronze/fred/{date_str}/macro_indicators_{datetime_str}.json"
    
    # Prepare metadata
    metadata = {
        'extraction_timestamp': timestamp.isoformat(),
        'source': 'fred_api',
        'num_series': len(data),
        'series_names': list(data.keys()),
        'total_observations': sum(item.get('count', 0) for item in data.values()),
        'data_file': data_key
    }
    
    metadata_key = f"bronze/fred/{date_str}/metadata_{datetime_str}.json"
    
    try:
        # Upload data file
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=data_key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json',
            Metadata={
                'source': 'fred',
                'extraction_time': timestamp.isoformat(),
                'series_count': str(len(data))
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
    print("🚀 FRED Macro Data Extraction - Bronze Layer")
    print("=" * 80)
    
    timestamp = datetime.utcnow()
    print(f"⏰ Execution time: {timestamp.isoformat()}")
    
    try:
        # Get API key
        api_key = get_api_key()
        
        # Fetch all macro data
        macro_data = fetch_all_macro_data(api_key)
        
        # Save to S3 Bronze layer
        s3_keys = save_to_s3_bronze(macro_data, timestamp)
        
        # Count total observations
        total_obs = sum(item.get('count', 0) for item in macro_data.values())
        
        # Prepare response
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'FRED data extraction successful',
                'timestamp': timestamp.isoformat(),
                'num_series': len(macro_data),
                'total_observations': total_obs,
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
                'message': 'FRED data extraction failed',
                'error': str(e),
                'timestamp': timestamp.isoformat()
            })
        }