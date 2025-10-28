"""
Transform Bronze → Silver Layer
=================================

This script transforms raw CoinGecko data from Bronze layer to cleaned,
analytics-ready data in Silver layer.

ETL Process:
1. READ: Load raw JSON from bronze/coingecko/
2. TRANSFORM: Clean, validate, standardize data
3. LOAD: Save as CSV to silver/crypto/

Usage:
    python scripts/transform_bronze_to_silver.py
"""

import boto3
import os
import json
import pandas as pd
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
BRONZE_PREFIX = 'bronze/coingecko/'
SILVER_PREFIX = 'silver/crypto/'


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


def list_bronze_files(s3_client):
    """
    List all JSON files in bronze/coingecko/

    Returns:
        list: List of file keys
    """
    print(f"\n📂 Scanning Bronze layer: s3://{BUCKET_NAME}/{BRONZE_PREFIX}")

    try:
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=BRONZE_PREFIX
        )

        if 'Contents' not in response:
            print("   ⚠️  No files found in Bronze layer")
            return []

        files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]

        print(f"   Found {len(files)} JSON file(s):")
        for file_key in files:
            print(f"   - {file_key}")

        return files

    except ClientError as e:
        print(f"❌ Error listing files: {e}")
        return []


def read_json_from_s3(s3_client, s3_key):
    """
    Read JSON file from S3

    Args:
        s3_client: boto3 S3 client
        s3_key: S3 object key

    Returns:
        dict: Parsed JSON data
    """
    print(f"\n📥 Reading: s3://{BUCKET_NAME}/{s3_key}")

    try:
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        data = json.loads(obj['Body'].read().decode('utf-8'))

        print(f"✅ Loaded successfully")
        return data

    except ClientError as e:
        print(f"❌ Error reading file: {e}")
        return None


def transform_top10_prices(raw_data):
    """
    Transform top10 prices data from Bronze to Silver

    Args:
        raw_data: Raw JSON data from CoinGecko

    Returns:
        pd.DataFrame: Cleaned and transformed data
    """
    print("\n🔄 Transforming top10 prices data...")

    # Extract coins array
    coins = raw_data.get('coins', [])

    if not coins:
        print("   ⚠️  No coin data found")
        return None

    # Create DataFrame
    df = pd.DataFrame(coins)

    # ===== TRANSFORMATION STEPS =====

    # 1. Add timestamp from parent object
    df['timestamp'] = raw_data.get('timestamp')
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # 2. Standardize coin IDs (uppercase)
    df['coin_id'] = df['coin_id'].str.upper()

    # 3. Round prices to 2 decimals
    df['price_usd'] = df['price_usd'].round(2)

    # 4. Handle nulls - remove rows with missing prices
    original_count = len(df)
    df = df.dropna(subset=['price_usd'])
    dropped = original_count - len(df)
    if dropped > 0:
        print(f"   ⚠️  Dropped {dropped} rows with missing prices")

    # 5. Validate data
    assert df['price_usd'].min() > 0, "❌ Negative prices found!"
    assert len(df) > 0, "❌ No valid data after transformation!"

    # 6. Add metadata columns
    df['data_source'] = 'coingecko_api'
    df['layer'] = 'silver'
    df['processed_at'] = datetime.utcnow().isoformat()

    # 7. Reorder columns
    column_order = [
        'timestamp',
        'coin_id',
        'price_usd',
        'market_cap_usd',
        'volume_24h_usd',
        'price_change_24h_percent',
        'data_source',
        'layer',
        'processed_at'
    ]

    # Only keep columns that exist
    column_order = [col for col in column_order if col in df.columns]
    df = df[column_order]

    print(f"✅ Transformation complete: {len(df)} rows")
    print(f"   Columns: {', '.join(df.columns.tolist())}")

    return df


def transform_bitcoin_detailed(raw_data):
    """
    Transform detailed Bitcoin data from Bronze to Silver

    Args:
        raw_data: Raw JSON data from CoinGecko

    Returns:
        pd.DataFrame: Cleaned and transformed data
    """
    print("\n🔄 Transforming Bitcoin detailed data...")

    # Create single-row DataFrame from dict
    df = pd.DataFrame([raw_data])

    # ===== TRANSFORMATION STEPS =====

    # 1. Parse timestamp
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # 2. Standardize symbol
    df['symbol'] = df['symbol'].str.upper()

    # 3. Round numeric columns
    numeric_cols = [
        'current_price_usd',
        'market_cap_usd',
        'total_volume_usd',
        'price_change_24h',
        'price_change_percentage_24h'
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    # 4. Handle nulls
    df = df.dropna(subset=['current_price_usd'])

    # 5. Validate
    assert df['current_price_usd'].iloc[0] > 0, "❌ Invalid price!"

    # 6. Add metadata
    df['data_source'] = 'coingecko_api'
    df['layer'] = 'silver'
    df['processed_at'] = datetime.utcnow().isoformat()

    print(f"✅ Transformation complete: 1 row")

    return df


def save_to_silver(s3_client, df, filename):
    """
    Save DataFrame as CSV to Silver layer in S3

    Args:
        s3_client: boto3 S3 client
        df: pandas DataFrame
        filename: Output filename
    """
    s3_key = f"{SILVER_PREFIX}{filename}"

    print(f"\n💾 Saving to Silver layer: s3://{BUCKET_NAME}/{s3_key}")

    try:
        # Convert DataFrame to CSV
        csv_buffer = df.to_csv(index=False)

        # Upload to S3
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=csv_buffer.encode('utf-8'),
            ContentType='text/csv'
        )

        size_kb = len(csv_buffer) / 1024
        print(f"✅ Saved successfully! ({size_kb:.2f} KB)")
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {len(df.columns)}")

        return True

    except ClientError as e:
        print(f"❌ Error saving file: {e}")
        return False


def preview_dataframe(df, title="Data Preview"):
    """
    Print a preview of the DataFrame

    Args:
        df: pandas DataFrame
        title: Title for the preview
    """
    print(f"\n{'=' * 80}")
    print(f"👀 {title}")
    print(f"{'=' * 80}")
    print(f"Shape: {df.shape[0]} rows × {df.shape[1]} columns")
    print(f"\nFirst 3 rows:")
    print(df.head(3).to_string(index=False))
    print(f"{'=' * 80}")


def main():
    """
    Main ETL pipeline: Bronze → Silver transformation
    """
    print("=" * 80)
    print("🔄 Bronze → Silver ETL Pipeline")
    print("=" * 80)
    print("\nETL Process:")
    print("  1️⃣  EXTRACT: Read raw JSON from Bronze layer")
    print("  2️⃣  TRANSFORM: Clean, validate, standardize data")
    print("  3️⃣  LOAD: Save as CSV to Silver layer")

    # Create S3 client
    try:
        s3 = create_s3_client()
    except Exception as e:
        print(f"❌ Failed to create S3 client: {e}")
        return

    # List files in Bronze
    bronze_files = list_bronze_files(s3)

    if not bronze_files:
        print("\n❌ No files found in Bronze layer!")
        print("   Run fetch_coingecko_to_s3.py first to populate Bronze layer")
        return

    transformed_count = 0

    # Process each file
    for bronze_key in bronze_files:
        print(f"\n{'=' * 80}")
        print(f"📄 Processing: {bronze_key}")
        print(f"{'=' * 80}")

        # EXTRACT
        raw_data = read_json_from_s3(s3, bronze_key)

        if not raw_data:
            continue

        # TRANSFORM (based on file type)
        if 'top10_prices' in bronze_key:
            df = transform_top10_prices(raw_data)
            output_filename = f"coingecko_top10_cleaned_{datetime.utcnow().strftime('%Y%m%d')}.csv"

        elif 'bitcoin_detailed' in bronze_key:
            df = transform_bitcoin_detailed(raw_data)
            output_filename = f"coingecko_bitcoin_detailed_{datetime.utcnow().strftime('%Y%m%d')}.csv"

        else:
            print(f"   ⚠️  Unknown file type, skipping...")
            continue

        if df is None or len(df) == 0:
            print(f"   ⚠️  No data after transformation, skipping...")
            continue

        # Preview
        preview_dataframe(df, f"Transformed Data from {bronze_key}")

        # LOAD
        if save_to_silver(s3, df, output_filename):
            transformed_count += 1

    # Summary
    print("\n" + "=" * 80)
    print("✅ ETL Pipeline Complete!")
    print("=" * 80)
    print(f"\n📊 Summary:")
    print(f"   - Bronze files processed: {len(bronze_files)}")
    print(f"   - Silver files created: {transformed_count}")
    print(f"   - Location: s3://{BUCKET_NAME}/{SILVER_PREFIX}")
    print("\n🎯 Data Quality Improvements:")
    print("   ✅ Nulls removed")
    print("   ✅ Data types standardized")
    print("   ✅ Prices validated (no negatives)")
    print("   ✅ Metadata added (source, timestamp)")
    print("   ✅ Format: CSV (easy to analyze)")
    print("\n💡 Next Steps:")
    print("   - Use Silver data for exploratory analysis")
    print("   - Create correlations in Gold layer")
    print("   - Build dashboards with clean Silver data")


if __name__ == "__main__":
    main()