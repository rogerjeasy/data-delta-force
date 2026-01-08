"""
AWS Lambda: Transform FRED Bronze → Silver
Converts macro indicator JSON to structured CSV with separate files per category
"""

import json
import boto3
from datetime import datetime
from typing import Dict, Any, List

s3_client = boto3.client('s3')

# Series categorization
SERIES_CATEGORIES = {
    'fed_funds_rate': 'interest_rates',
    '10y_treasury': 'interest_rates',
    '2y_treasury': 'interest_rates',
    'cpi': 'inflation',
    'core_cpi': 'inflation',
    'pce': 'inflation',
    'unemployment_rate': 'employment',
    'gdp': 'gdp'
}

def lambda_handler(event, context):
    """Triggered by S3 event when new Bronze FRED data arrives"""
    print("=" * 80)
    print("🔄 FRED Bronze → Silver Transformation")
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
        
        print(f"✅ Loaded {len(bronze_data)} series from Bronze")
        
        # Transform and save by category
        silver_keys = []
        for series_name, series_data in bronze_data.items():
            category = SERIES_CATEGORIES.get(series_name, 'other')
            
            # Transform to CSV
            csv_data = transform_fred_series(series_name, series_data)
            
            if csv_data:
                # Save to Silver layer
                silver_key = save_to_silver(bucket, category, series_name, csv_data)
                silver_keys.append(silver_key)
        
        print("=" * 80)
        print(f"✅ Transformation complete: {len(silver_keys)} files created")
        print("=" * 80)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Transformation successful',
                'bronze_key': key,
                'silver_keys': silver_keys,
                'files_created': len(silver_keys)
            })
        }
        
    except Exception as e:
        print(f"❌ Transformation failed: {e}")
        raise


def transform_fred_series(series_name: str, series_data: Dict[str, Any]) -> str:
    """Transform single FRED series to CSV format"""
    
    observations = series_data.get('observations', [])
    
    if not observations:
        print(f"⚠️  No observations for {series_name}")
        return None
    
    # CSV header
    csv_lines = [
        'date,series_id,series_name,value,realtime_start,realtime_end'
    ]
    
    series_id = series_data.get('series_id', '')
    
    # Transform each observation
    for obs in observations:
        date = obs.get('date', '')
        value = obs.get('value', '')
        realtime_start = obs.get('realtime_start', '')
        realtime_end = obs.get('realtime_end', '')
        
        # Skip missing or "." values
        if value == '.' or value == '':
            continue
        
        row = [
            date,
            series_id,
            series_name,
            value,
            realtime_start,
            realtime_end
        ]
        
        csv_lines.append(','.join(str(v) for v in row))
    
    if len(csv_lines) <= 1:  # Only header, no data
        return None
    
    print(f"✅ Transformed {series_name}: {len(csv_lines)-1} observations")
    
    return '\n'.join(csv_lines)


def save_to_silver(bucket: str, category: str, series_name: str, csv_data: str) -> str:
    """
    Save transformed data to Silver layer
    Structure: silver/macro/{category}/year=YYYY/month=MM/file.csv
    """
    now = datetime.utcnow()
    year = now.strftime('%Y')
    month = now.strftime('%m')
    date_str = now.strftime('%Y%m%d_%H%M%S')
    
    # Silver layer path with partitioning
    silver_key = f"silver/macro/{category}/year={year}/month={month}/{series_name}_{date_str}.csv"
    
    # Upload to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=silver_key,
        Body=csv_data.encode('utf-8'),
        ContentType='text/csv',
        Metadata={
            'source': 'bronze_fred',
            'transformation_timestamp': now.isoformat(),
            'category': category,
            'series_name': series_name,
            'format': 'csv'
        }
    )
    
    print(f"📤 Uploaded: s3://{bucket}/{silver_key}")
    
    return silver_key