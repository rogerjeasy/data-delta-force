"""
AWS Lambda Function: Transform FOMC Bronze to Silver Layer
Converts FOMC sentiment JSON to structured CSV format
"""

import json
import boto3
from datetime import datetime
from typing import Dict, List
import os
import csv
from io import StringIO

# Initialize clients
s3_client = boto3.client('s3')

# Configuration
BUCKET = os.environ['S3_BUCKET']


def lambda_handler(event, context):
    """S3 event-triggered transformation"""
    
    print("=" * 80)
    print("🏛️ FOMC Bronze to Silver Transformation")
    print("=" * 80)
    
    # Get S3 event details
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    
    print(f"📥 Processing: {key}")
    
    # Only process complete files (not batches)
    if 'fomc_sentiment_complete' not in key:
        print("⏭️  Skipping batch file (only processing complete files)")
        return {'statusCode': 200, 'body': 'Skipped batch file'}
    
    try:
        # Read Bronze data
        response = s3_client.get_object(Bucket=bucket, Key=key)
        bronze_data = json.loads(response['Body'].read().decode('utf-8'))
        
        print(f"📊 Loaded {len(bronze_data)} FOMC records")
        
        # Transform to CSV
        csv_data = transform_to_csv(bronze_data)
        
        # Save to Silver layer
        silver_key = save_to_silver(bucket, csv_data)
        
        print("=" * 80)
        print(f"✅ Transformation complete!")
        print(f"📁 Silver file: {silver_key}")
        print("=" * 80)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'FOMC transformation successful',
                'bronze_key': key,
                'silver_key': silver_key,
                'records_processed': len(bronze_data)
            })
        }
        
    except Exception as e:
        print(f"❌ Transformation failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def transform_to_csv(data: List[Dict]) -> str:
    """Transform FOMC JSON data to CSV format"""
    
    # CSV header
    fieldnames = [
        'date',
        'year',
        'month',
        'url',
        'title',
        'word_count',
        'sentiment_compound',
        'sentiment_positive',
        'sentiment_negative',
        'sentiment_neutral',
        'regime',
        'sentiment_source',
        'retrieved_on'
    ]
    
    # Create CSV in memory
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    
    for record in data:
        # Parse date
        date_str = record.get('date', '')
        year = record.get('year', '')
        month = extract_month(date_str)
        
        # Write row
        writer.writerow({
            'date': date_str,
            'year': year,
            'month': month,
            'url': record.get('url', ''),
            'title': record.get('title', ''),
            'word_count': record.get('word_count', 0),
            'sentiment_compound': record.get('compound', 0),
            'sentiment_positive': record.get('positive', 0),
            'sentiment_negative': record.get('negative', 0),
            'sentiment_neutral': record.get('neutral', 0),
            'regime': record.get('regime', 'neutral'),
            'sentiment_source': record.get('sentiment_source', 'VADER_LITE'),
            'retrieved_on': record.get('retrieved_on', '')
        })
    
    return output.getvalue()


def extract_month(date_str: str) -> str:
    """Extract month name from date string"""
    months = {
        'January': '01', 'February': '02', 'March': '03', 'April': '04',
        'May': '05', 'June': '06', 'July': '07', 'August': '08',
        'September': '09', 'October': '10', 'November': '11', 'December': '12'
    }
    
    for month_name, month_num in months.items():
        if month_name in date_str:
            return month_num
    
    return '00'  # Unknown


def save_to_silver(bucket: str, csv_data: str) -> str:
    """Save CSV to Silver layer with partitioning"""
    
    now = datetime.utcnow()
    year = now.strftime('%Y')
    month = now.strftime('%m')
    timestamp = now.strftime('%Y%m%d_%H%M%S')
    
    # Silver path with partitioning
    silver_key = f"silver/fomc/sentiment/year={year}/month={month}/fomc_sentiment_{timestamp}.csv"
    
    # Save to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=silver_key,
        Body=csv_data.encode('utf-8'),
        ContentType='text/csv',
        Metadata={
            'source': 'fomc_bronze',
            'transformation_time': now.isoformat()
        }
    )
    
    print(f"✅ Silver file saved: {silver_key}")
    
    return silver_key