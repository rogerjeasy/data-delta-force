"""
AWS Lambda: Backfill FRED Historical Data (2020-01-01 to 2025-12-02)
Fetches complete historical macroeconomic data - FREE, UNLIMITED ACCESS
"""

import json
import boto3
import urllib3
import time
from datetime import datetime
from typing import Dict, Any, List
import os

# Initialize clients
s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')
http = urllib3.PoolManager()

# Configuration
S3_BUCKET = os.environ['S3_BUCKET']
SECRET_NAME = os.environ['SECRET_NAME']
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# Key macro series to fetch (same as daily extraction)
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

# ✅ HISTORICAL DATE RANGE: 2020-01-01 to 2025-12-02
START_DATE = datetime(2020, 1, 1)
END_DATE = datetime(2025, 12, 2)


def lambda_handler(event, context):
    """AWS Lambda handler for historical backfill"""
    print("=" * 80)
    print("🔄 BACKFILL: FRED Historical Data (2020-2025)")
    print("=" * 80)
    print(f"📅 Date Range: {START_DATE.date()} to {END_DATE.date()}")
    print(f"📊 Series to fetch: {len(MACRO_SERIES)}")
    print("=" * 80)
    
    timestamp = datetime.utcnow()
    
    try:
        # Get API key
        api_key = get_api_key()
        
        # Fetch all macro data (historical range)
        macro_data = fetch_all_macro_data_historical(api_key)
        
        # Save to S3 Bronze layer
        s3_keys = save_to_s3_bronze(macro_data, timestamp)
        
        # Count total observations
        total_obs = sum(item.get('count', 0) for item in macro_data.values())
        
        print("\n" + "=" * 80)
        print("🎉 BACKFILL COMPLETE!")
        print(f"✅ Series fetched: {len(macro_data)}")
        print(f"📊 Total observations: {total_obs}")
        print(f"📅 Date range: {START_DATE.date()} to {END_DATE.date()}")
        print("=" * 80)
        
        # Prepare response
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'FRED historical backfill successful',
                'timestamp': timestamp.isoformat(),
                'start_date': START_DATE.date().isoformat(),
                'end_date': END_DATE.date().isoformat(),
                'num_series': len(macro_data),
                'total_observations': total_obs,
                's3_data_key': s3_keys['data_key'],
                's3_metadata_key': s3_keys['metadata_key']
            })
        }
        
        return response
        
    except Exception as e:
        print(f"❌ Backfill failed: {e}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'FRED backfill failed',
                'error': str(e),
                'timestamp': timestamp.isoformat()
            })
        }


def get_api_key() -> str:
    """Retrieve FRED API key from Secrets Manager"""
    try:
        response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        api_key = response['SecretString']
        
        print(f"🔑 FRED API Key retrieved successfully")
        print(f"🔑 Key length: {len(api_key)} characters")
        
        return api_key
    except Exception as e:
        print(f"❌ Error retrieving FRED API key: {e}")
        raise


def fetch_fred_series_historical(series_id: str, series_name: str, api_key: str) -> Dict[str, Any]:
    """Fetch a single FRED series with historical date range"""
    
    observation_start = START_DATE.strftime('%Y-%m-%d')
    observation_end = END_DATE.strftime('%Y-%m-%d')
    
    # Build URL with parameters
    params = {
        'series_id': series_id,
        'api_key': api_key,
        'file_type': 'json',
        'observation_start': observation_start,
        'observation_end': observation_end
    }
    
    query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
    full_url = f"{FRED_BASE_URL}?{query_string}"
    
    try:
        print(f"\n📈 Fetching {series_name} ({series_id})...")
        
        response = http.request('GET', full_url, timeout=30.0)
        
        if response.status == 200:
            data = json.loads(response.data.decode('utf-8'))
            
            if 'observations' in data:
                # Filter out missing values (value = ".")
                valid_observations = [
                    obs for obs in data['observations']
                    if obs.get('value') != '.' and obs.get('value') is not None
                ]
                
                print(f"  ✅ {series_name}: {len(valid_observations)} observations")
                
                return {
                    'series_id': series_id,
                    'series_name': series_name,
                    'observations': valid_observations,
                    'count': len(valid_observations)
                }
            else:
                print(f"  ⚠️  {series_name}: No observations found")
                return {
                    'series_id': series_id,
                    'series_name': series_name,
                    'observations': [],
                    'count': 0
                }
        
        elif response.status == 429:
            print(f"  ⚠️  Rate limit hit for {series_name}, waiting 60 seconds...")
            time.sleep(60)
            # Retry once
            response = http.request('GET', full_url, timeout=30.0)
            if response.status == 200:
                data = json.loads(response.data.decode('utf-8'))
                observations = data.get('observations', [])
                valid_observations = [
                    obs for obs in observations
                    if obs.get('value') != '.' and obs.get('value') is not None
                ]
                print(f"  ✅ {series_name}: Retry successful - {len(valid_observations)} observations")
                return {
                    'series_id': series_id,
                    'series_name': series_name,
                    'observations': valid_observations,
                    'count': len(valid_observations)
                }
            else:
                print(f"  ❌ {series_name}: Retry failed with status {response.status}")
                return {
                    'series_id': series_id,
                    'series_name': series_name,
                    'observations': [],
                    'count': 0,
                    'error': f"Status {response.status}"
                }
        else:
            print(f"  ❌ {series_name}: API returned status code {response.status}")
            return {
                'series_id': series_id,
                'series_name': series_name,
                'observations': [],
                'count': 0,
                'error': f"Status {response.status}"
            }
            
    except Exception as e:
        print(f"  ❌ Error fetching {series_name}: {e}")
        return {
            'series_id': series_id,
            'series_name': series_name,
            'observations': [],
            'count': 0,
            'error': str(e)
        }


def fetch_all_macro_data_historical(api_key: str) -> Dict[str, Any]:
    """Fetch all configured macro series with historical range"""
    all_data = {}
    
    for series_name, series_id in MACRO_SERIES.items():
        series_data = fetch_fred_series_historical(series_id, series_name, api_key)
        all_data[series_name] = series_data
        
        # Small delay between requests to be polite to API
        time.sleep(1)
    
    return all_data


def save_to_s3_bronze(data: Dict[str, Any], timestamp: datetime) -> Dict[str, str]:
    """Save FRED historical data to S3 Bronze layer"""
    
    # Use start date for folder structure
    date_str = START_DATE.strftime('%Y-%m-%d')
    datetime_str = timestamp.strftime('%Y%m%d_%H%M%S')
    
    # Prepare data file
    data_key = f"bronze/fred/{date_str}/historical_macro_indicators_{datetime_str}.json"
    
    # Prepare metadata
    metadata = {
        'extraction_timestamp': timestamp.isoformat(),
        'source': 'fred_api',
        'extraction_type': 'historical_backfill_2020_2025',
        'observation_start': START_DATE.strftime('%Y-%m-%d'),
        'observation_end': END_DATE.strftime('%Y-%m-%d'),
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
            Body=json.dumps(data, indent=2).encode('utf-8'),
            ContentType='application/json',
            Metadata={
                'source': 'fred',
                'extraction_type': 'historical_backfill',
                'extraction_time': timestamp.isoformat(),
                'series_count': str(len(data)),
                'date_range': f"{START_DATE.date()}_to_{END_DATE.date()}"
            }
        )
        print(f"\n✅ Data uploaded to s3://{S3_BUCKET}/{data_key}")
        
        # Upload metadata file
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2).encode('utf-8'),
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