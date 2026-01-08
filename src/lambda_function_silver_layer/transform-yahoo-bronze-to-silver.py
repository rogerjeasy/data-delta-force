
"""
AWS Lambda: Transform Yahoo Finance Bronze → Silver
Converts market data JSON to structured CSV
"""

import json
import boto3
from datetime import datetime
from typing import Dict, Any, List

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """Triggered by S3 event when new Bronze Yahoo Finance data arrives"""
    print("=" * 80)
    print("🔄 Yahoo Finance Bronze → Silver Transformation")
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
        
        print(f"✅ Loaded {len(bronze_data)} tickers from Bronze")
        
        # Transform to Silver format
        csv_data = transform_yahoo_data(bronze_data)
        
        # Save to Silver layer
        silver_key = save_to_silver(bucket, csv_data)
        
        print("=" * 80)
        print(f"✅ Transformation complete: {silver_key}")
        print("=" * 80)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Transformation successful',
                'bronze_key': key,
                'silver_key': silver_key
            })
        }
        
    except Exception as e:
        print(f"❌ Transformation failed: {e}")
        raise


def transform_yahoo_data(bronze_data: Dict[str, Any]) -> str:
    """Transform Yahoo Finance JSON to CSV format"""
    
    # CSV header
    csv_lines = [
        'timestamp,ticker_name,ticker_symbol,date,open,high,low,close,volume,'
        'currency,exchange,regular_market_price'
    ]
    
    extraction_timestamp = datetime.utcnow().isoformat()
    
    # Process each ticker
    for ticker_name, ticker_data in bronze_data.items():
        ticker_symbol = ticker_data.get('ticker', '')
        meta = ticker_data.get('meta', {})
        data_points = ticker_data.get('data_points', [])
        
        currency = meta.get('currency', '')
        exchange = meta.get('exchangeName', '')
        regular_market_price = meta.get('regularMarketPrice', '')
        
        # Process each data point
        for point in data_points:
            row = [
                extraction_timestamp,
                ticker_name,
                ticker_symbol,
                point.get('timestamp', ''),
                clean_value(point.get('open')),
                clean_value(point.get('high')),
                clean_value(point.get('low')),
                clean_value(point.get('close')),
                clean_value(point.get('volume')),
                currency,
                exchange,
                regular_market_price
            ]
            
            csv_lines.append(','.join(str(v) for v in row))
    
    print(f"✅ Transformed {len(csv_lines)-1} data points")
    
    return '\n'.join(csv_lines)


def clean_value(value):
    """Clean and standardize values"""
    if value is None or value == '':
        return ''
    return value


def save_to_silver(bucket: str, csv_data: str) -> str:
    """
    Save transformed data to Silver layer
    Structure: silver/markets/traditional/year=YYYY/month=MM/file.csv
    """
    now = datetime.utcnow()
    year = now.strftime('%Y')
    month = now.strftime('%m')
    date_str = now.strftime('%Y%m%d_%H%M%S')
    
    # Silver layer path with partitioning
    silver_key = f"silver/markets/traditional/year={year}/month={month}/traditional_markets_{date_str}.csv"
    
    # Upload to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=silver_key,
        Body=csv_data.encode('utf-8'),
        ContentType='text/csv',
        Metadata={
            'source': 'bronze_yahoo',
            'transformation_timestamp': now.isoformat(),
            'format': 'csv'
        }
    )
    
    print(f"📤 Uploaded: s3://{bucket}/{silver_key}")
    
    return silver_key