"""
Upload Files to S3
==================

This script uploads local files to the crypto-macro-datalake-ddf S3 bucket.
Useful for pushing processed data or static files to the Data Lake.

Usage:
    python scripts/upload_to_s3.py
"""

import boto3
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from pathlib import Path

# Load environment variables
load_dotenv()

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

# S3 Configuration
BUCKET_NAME = 'crypto-macro-datalake-ddf'


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


def upload_file(s3_client, local_path, s3_key):
    """
    Upload a file from local path to S3

    Args:
        s3_client: boto3 S3 client
        local_path: Path to local file
        s3_key: S3 destination key (path in bucket)
    """
    if not os.path.exists(local_path):
        print(f"❌ File not found: {local_path}")
        return False

    file_size = os.path.getsize(local_path)
    file_size_mb = file_size / (1024 * 1024)

    print(f"\n⬆️  Uploading: {local_path}")
    print(f"   Size: {file_size_mb:.2f} MB")
    print(f"   Destination: s3://{BUCKET_NAME}/{s3_key}")

    try:
        s3_client.upload_file(
            Filename=local_path,
            Bucket=BUCKET_NAME,
            Key=s3_key
        )

        print(f"✅ Upload successful!")
        return True

    except ClientError as e:
        print(f"❌ Upload failed: {e}")
        return False


def verify_upload(s3_client, s3_key):
    """
    Verify that a file exists in S3

    Args:
        s3_client: boto3 S3 client
        s3_key: S3 object key to check
    """
    print(f"\n🔍 Verifying upload...")

    try:
        response = s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key)
        size = response['ContentLength']
        last_modified = response['LastModified']

        print(f"✅ File verified in S3!")
        print(f"   Size: {size / 1024:.2f} KB")
        print(f"   Last Modified: {last_modified}")
        return True

    except ClientError:
        print(f"❌ File not found in S3!")
        return False


def list_local_files(directory='data/raw'):
    """
    List CSV files in a local directory

    Args:
        directory: Local directory to scan

    Returns:
        list: List of CSV file paths
    """
    print(f"\n📂 Scanning local directory: {directory}")

    if not os.path.exists(directory):
        print(f"❌ Directory not found: {directory}")
        return []

    csv_files = list(Path(directory).glob('*.csv'))

    if not csv_files:
        print(f"   No CSV files found")
        return []

    print(f"   Found {len(csv_files)} CSV file(s):")
    for i, file_path in enumerate(csv_files, 1):
        size_kb = os.path.getsize(file_path) / 1024
        print(f"   {i}. {file_path.name} ({size_kb:.2f} KB)")

    return csv_files


def main():
    """
    Main function to upload files to S3
    """
    print("=" * 80)
    print("🚀 Upload Files to S3 Data Lake")
    print("=" * 80)

    # Create S3 client
    try:
        s3 = create_s3_client()
    except Exception as e:
        print(f"❌ Failed to create S3 client: {e}")
        return

    # List available local files
    local_files = list_local_files('data/raw')

    if not local_files:
        print("\n⚠️  No files found to upload!")
        print("   Make sure you have CSV files in data/raw/")
        return

    # Upload examples
    print("\n" + "=" * 80)
    print("📤 Uploading Files")
    print("=" * 80)

    uploaded_count = 0

    # Example 1: Upload market_regimes.csv to bronze/static/
    market_regimes = 'data/raw/market_regimes.csv'
    if os.path.exists(market_regimes):
        if upload_file(s3, market_regimes, 'bronze/static/market_regimes.csv'):
            verify_upload(s3, 'bronze/static/market_regimes.csv')
            uploaded_count += 1

    # Example 2: Upload exchange_listings.csv to bronze/static/
    exchange_listings = 'data/raw/exchange_listings.csv'
    if os.path.exists(exchange_listings):
        if upload_file(s3, exchange_listings, 'bronze/static/exchange_listings.csv'):
            verify_upload(s3, 'bronze/static/exchange_listings.csv')
            uploaded_count += 1

    # Summary
    print("\n" + "=" * 80)
    print("✅ Upload Complete!")
    print("=" * 80)
    print(f"\n📋 Summary:")
    print(f"   - Bucket: {BUCKET_NAME}")
    print(f"   - Files uploaded: {uploaded_count}")
    print(f"   - Destination: bronze/static/")
    print("\n💡 Tip: Check AWS S3 Console to verify uploads!")
    print("   Or run: python scripts/test_s3_download.py")


if __name__ == "__main__":
    main()