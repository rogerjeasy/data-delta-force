"""
Test S3 Download Script
========================

This script tests the connection to AWS S3 and downloads a file from the
crypto-macro-datalake-ddf bucket.

Usage:
    python scripts/s3_download.py
"""

import boto3
import os
import pandas as pd
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Load environment variables from .env
load_dotenv()

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

# S3 Bucket Configuration
BUCKET_NAME = 'crypto-macro-datalake-ddf'
BRONZE_STATIC_PREFIX = 'bronze/static/'


def create_s3_client():
    """
    Create and return an S3 client with credentials from .env

    Returns:
        boto3.client: Configured S3 client
    """
    print("🔑 Loading AWS credentials from .env file...")

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


def list_bucket_files(s3_client, prefix=''):
    """
    List all files in the S3 bucket with given prefix

    Args:
        s3_client: boto3 S3 client
        prefix: S3 prefix/folder to list

    Returns:
        list: List of file keys
    """
    print(f"\n📂 Listing files in s3://{BUCKET_NAME}/{prefix}")

    try:
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=prefix
        )

        if 'Contents' not in response:
            print(f"   No files found in {prefix}")
            return []

        files = [obj['Key'] for obj in response['Contents']]

        print(f"   Found {len(files)} file(s):")
        for file_key in files:
            # Get file size
            size = next(obj['Size'] for obj in response['Contents'] if obj['Key'] == file_key)
            size_mb = size / (1024 * 1024)
            print(f"   - {file_key} ({size_mb:.2f} MB)")

        return files

    except ClientError as e:
        print(f"❌ Error listing files: {e}")
        return []


def download_file(s3_client, s3_key, local_path):
    """
    Download a file from S3 to local path

    Args:
        s3_client: boto3 S3 client
        s3_key: S3 object key (path in bucket)
        local_path: Local file path to save to
    """
    print(f"\n⬇️  Downloading s3://{BUCKET_NAME}/{s3_key}")
    print(f"   Saving to: {local_path}")

    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # Download file
        s3_client.download_file(BUCKET_NAME, s3_key, local_path)

        file_size = os.path.getsize(local_path) / 1024
        print(f"✅ Downloaded successfully! ({file_size:.2f} KB)")

    except ClientError as e:
        print(f"❌ Error downloading file: {e}")
        raise


def preview_csv(file_path):
    """
    Preview first few rows of a CSV file

    Args:
        file_path: Path to CSV file
    """
    print(f"\n👀 Preview of {os.path.basename(file_path)}:")
    print("=" * 80)

    try:
        df = pd.read_csv(file_path)
        print(f"   Shape: {df.shape[0]} rows × {df.shape[1]} columns")
        print(f"   Columns: {', '.join(df.columns.tolist())}")
        print("\n   First 5 rows:")
        print(df.head().to_string(index=False))
        print("=" * 80)

    except Exception as e:
        print(f"❌ Error reading CSV: {e}")


def main():
    """
    Main function to test S3 download
    """
    print("=" * 80)
    print("🚀 Testing S3 Connection and Download")
    print("=" * 80)

    # Step 1: Create S3 client
    try:
        s3 = create_s3_client()
    except Exception as e:
        print(f"❌ Failed to create S3 client: {e}")
        return

    # Step 2: List files in bronze/static/
    files = list_bucket_files(s3, BRONZE_STATIC_PREFIX)

    if not files:
        print("\n❌ No files found in bronze/static/")
        print("   Make sure you uploaded the CSV files to S3!")
        return

    # Step 3: Download first CSV file
    csv_files = [f for f in files if f.endswith('.csv')]

    if not csv_files:
        print("\n❌ No CSV files found!")
        return

    # Download first CSV
    s3_key = csv_files[0]
    filename = os.path.basename(s3_key)
    local_path = f"data/downloaded/{filename}"

    download_file(s3, s3_key, local_path)

    # Step 4: Preview the downloaded file
    preview_csv(local_path)

    # Success!
    print("\n" + "=" * 80)
    print("✅ S3 Test Successful!")
    print("=" * 80)
    print("\n📋 Summary:")
    print(f"   - Bucket: {BUCKET_NAME}")
    print(f"   - Files found: {len(files)}")
    print(f"   - Downloaded: {filename}")
    print(f"   - Saved to: {local_path}")
    print("\n🎉 You can now use boto3 to interact with your S3 Data Lake!")


if __name__ == "__main__":
    main()