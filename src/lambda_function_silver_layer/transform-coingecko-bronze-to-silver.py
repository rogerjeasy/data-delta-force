"""
AWS Lambda: Transform CoinGecko Bronze → Silver
Handles both daily extraction and historical backfill data formats
Triggered by S3 events when new Bronze data arrives
"""

import json
import boto3
from datetime import datetime
from typing import Dict, Any, List

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Triggered by S3 event when new Bronze CoinGecko data arrives
    """
    print("=" * 80)
    print("🔄 CoinGecko Bronze → Silver Transformation")
    print("=" * 80)
    
    try:
        # Get S3 event details
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        print(f"📥 Processing: s3://{bucket}/{key}")
        
        # Skip metadata files
        if 'metadata' in key:
            print("⏭️  Skipping metadata file")
            return {'statusCode': 200, 'body': 'Metadata file skipped'}
        
        # Read Bronze JSON data
        response = s3_client.get_object(Bucket=bucket, Key=key)
        bronze_data = json.loads(response['Body'].read().decode('utf-8'))
        
        print(f"✅ Loaded {len(bronze_data)} records from Bronze")
        
        # Detect data format
        data_format = detect_data_format(bronze_data)
        print(f"📋 Detected format: {data_format}")
        
        # Transform to Silver format
        csv_data = transform_coingecko_data(bronze_data, data_format)
        
        # Save to Silver layer as CSV
        silver_key = save_to_silver(bucket, csv_data)
        
        print("=" * 80)
        print(f"✅ Transformation complete: {silver_key}")
        print("=" * 80)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Transformation successful',
                'bronze_key': key,
                'silver_key': silver_key,
                'records_processed': len(bronze_data),
                'data_format': data_format
            })
        }
        
    except Exception as e:
        print(f"❌ Transformation failed: {e}")
        raise


def detect_data_format(bronze_data: List[Dict[str, Any]]) -> str:
    """
    Detect whether data is from daily extraction or historical backfill
    """
    if not bronze_data:
        return 'unknown'
    
    first_record = bronze_data[0]
    
    # Daily extraction has 'current_price' field
    if 'current_price' in first_record:
        return 'daily_extraction'
    
    # Historical backfill has 'price_usd' field
    elif 'price_usd' in first_record:
        return 'historical_backfill'
    
    else:
        return 'unknown'


def transform_coingecko_data(bronze_data: List[Dict[str, Any]], data_format: str) -> str:
    """
    Transform Bronze JSON to Silver CSV format
    Handles both daily and historical data schemas
    """
    # CSV header (standardized schema)
    csv_lines = [
        'timestamp,coin_id,symbol,name,current_price_usd,market_cap_usd,'
        'market_cap_rank,total_volume_usd,high_24h,low_24h,price_change_24h,'
        'price_change_24h_pct,circulating_supply,total_supply,max_supply,'
        'ath,ath_date,atl,atl_date,last_updated'
    ]
    
    # Transform each record based on format
    for item in bronze_data:
        if data_format == 'daily_extraction':
            row = format_daily_schema(item)
        elif data_format == 'historical_backfill':
            row = format_historical_schema(item)
        else:
            print(f"⚠️  Unknown format, skipping record")
            continue
        
        csv_lines.append(row)
    
    print(f"✅ Transformed {len(csv_lines)-1} records to CSV")
    return '\n'.join(csv_lines)


def format_daily_schema(item: Dict[str, Any]) -> str:
    """
    Format daily extraction data (from /coins/markets endpoint)
    Has complete market data with all fields
    """
    timestamp = datetime.utcnow().isoformat()
    
    row = [
        timestamp,
        clean_value(item.get('id')),
        clean_value(item.get('symbol')),
        clean_value(item.get('name')),
        clean_value(item.get('current_price')),
        clean_value(item.get('market_cap')),
        clean_value(item.get('market_cap_rank')),
        clean_value(item.get('total_volume')),
        clean_value(item.get('high_24h')),
        clean_value(item.get('low_24h')),
        clean_value(item.get('price_change_24h')),
        clean_value(item.get('price_change_percentage_24h')),
        clean_value(item.get('circulating_supply')),
        clean_value(item.get('total_supply')),
        clean_value(item.get('max_supply')),
        clean_value(item.get('ath')),
        clean_value(item.get('ath_date')),
        clean_value(item.get('atl')),
        clean_value(item.get('atl_date')),
        clean_value(item.get('last_updated'))
    ]
    
    return ','.join(str(v) for v in row)


def format_historical_schema(item: Dict[str, Any]) -> str:
    """
    Format historical backfill data (from /market_chart/range endpoint)
    Has simplified schema with only price, market_cap, and volume
    Missing fields are left empty for consistency with daily schema
    """
    row = [
        clean_value(item.get('timestamp')),        
        clean_value(item.get('coin_id')),          
        '',                                         
        '',                                         
        clean_value(item.get('price_usd')),        
        clean_value(item.get('market_cap_usd')),   
        '',                                         
        clean_value(item.get('volume_24h_usd')),   
        '',                                         
        '',                                         
        '',                                         
        '',                                         
        '',                                         
        '',                                         
        '',                                         
        '',                                         
        '',                                         
        '',                                         
        '',                                         
        clean_value(item.get('timestamp'))         
    ]
    
    return ','.join(str(v) for v in row)


def clean_value(value):
    """
    Clean and standardize values
    Handles None, empty strings, and special characters
    """
    if value is None or value == '':
        return ''
    
    if isinstance(value, str):
        # Remove any existing quotes
        value = value.replace('"', '')
        # Escape commas by wrapping in quotes
        if ',' in value:
            return f'"{value}"'
        return value
    
    # Convert numbers to string
    return str(value)


def save_to_silver(bucket: str, csv_data: str) -> str:
    """
    Save transformed data to Silver layer
    Structure: silver/crypto/market_data/year=YYYY/month=MM/file.csv
    """
    now = datetime.utcnow()
    year = now.strftime('%Y')
    month = now.strftime('%m')
    date_str = now.strftime('%Y%m%d_%H%M%S')
    
    # Silver layer path with partitioning
    silver_key = f"silver/crypto/market_data/year={year}/month={month}/crypto_market_data_{date_str}.csv"
    
    # Upload to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=silver_key,
        Body=csv_data.encode('utf-8'),
        ContentType='text/csv',
        Metadata={
            'source': 'bronze_coingecko',
            'transformation_timestamp': now.isoformat(),
            'format': 'csv'
        }
    )
    
    print(f"📤 Uploaded to: s3://{bucket}/{silver_key}")
    
    return silver_key