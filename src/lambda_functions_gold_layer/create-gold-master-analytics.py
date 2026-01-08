"""
AWS Lambda: Create Gold Master Analytics Dataset
Joins crypto, macro, markets, and FOMC data into unified analytics table
"""

import json
import boto3
import time
from datetime import datetime
from typing import Dict, List
import os

# Configuration
DATABASE = os.environ.get('DATABASE', 'crypto_macro_db')
S3_BUCKET = os.environ['S3_BUCKET']
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
S3_OUTPUT = f"s3://{S3_BUCKET}/athena-query-results/"
GOLD_OUTPUT_PATH = f"s3://{S3_BUCKET}/gold/master_analytics/"

# Initialize clients
athena_client = boto3.client('athena', region_name=AWS_REGION)
s3_client = boto3.client('s3', region_name=AWS_REGION)


def lambda_handler(event, context):
    """Create master analytics dataset"""
    
    print("=" * 80)
    print("📊 GOLD LAYER: Master Analytics Dataset Creation")
    print("=" * 80)
    
    timestamp = datetime.utcnow()
    
    try:
        # Step 1: Drop existing table if it exists
        print("\n🗑️  Step 1: Dropping existing table...")
        drop_query = f"DROP TABLE IF EXISTS {DATABASE}.gold_master_analytics"
        drop_execution_id = execute_athena_query(drop_query)
        wait_for_query_completion(drop_execution_id)
        
        # Step 2: Create table structure
        print("\n🔨 Step 2: Creating table structure...")
        create_query = build_create_table_query()
        create_execution_id = execute_athena_query(create_query)
        wait_for_query_completion(create_execution_id)
        
        # Step 3: Insert data
        print("\n📥 Step 3: Inserting data...")
        insert_query = build_insert_query()
        insert_execution_id = execute_athena_query(insert_query)
        wait_for_query_completion(insert_execution_id)
        
        # Step 4: Get row count
        print("\n🔢 Step 4: Counting records...")
        count_query = f"SELECT COUNT(*) as record_count FROM {DATABASE}.gold_master_analytics"
        count_execution_id = execute_athena_query(count_query)
        wait_for_query_completion(count_execution_id)
        
        row_count = get_result_count_value(count_execution_id)
        
        print("\n" + "=" * 80)
        print("✅ GOLD LAYER CREATION COMPLETE!")
        print(f"📊 Records created: {row_count}")
        print(f"📁 Output: {GOLD_OUTPUT_PATH}")
        print("=" * 80)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Master analytics dataset created successfully',
                'timestamp': timestamp.isoformat(),
                'records_created': row_count,
                'output_path': GOLD_OUTPUT_PATH
            })
        }
        
    except Exception as e:
        print(f"❌ Gold layer creation failed: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Master analytics creation failed',
                'error': str(e),
                'timestamp': timestamp.isoformat()
            })
        }


def build_create_table_query() -> str:
    """Build CREATE TABLE query"""
    
    query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.gold_master_analytics (
        date DATE,
        btc_price DOUBLE,
        btc_volume DOUBLE,
        btc_market_cap DOUBLE,
        eth_price DOUBLE,
        eth_volume DOUBLE,
        eth_market_cap DOUBLE,
        fed_funds_rate DOUBLE,
        treasury_10y DOUBLE,
        treasury_2y DOUBLE,
        cpi DOUBLE,
        core_cpi DOUBLE,
        pce DOUBLE,
        unemployment_rate DOUBLE,
        gdp DOUBLE,
        fomc_regime VARCHAR(50),
        fomc_sentiment_score DOUBLE,
        fomc_positive DOUBLE,
        fomc_negative DOUBLE,
        btc_return_pct DOUBLE,
        eth_return_pct DOUBLE,
        btc_volatility_30d DOUBLE,
        eth_volatility_30d DOUBLE
    )
    PARTITIONED BY (year STRING, month STRING)
    STORED AS PARQUET
    LOCATION '{GOLD_OUTPUT_PATH}'
    """
    
    return query


def build_insert_query() -> str:
    """Build INSERT query with actual data"""
    
    query = f"""
    INSERT INTO {DATABASE}.gold_master_analytics
    
    WITH crypto_daily AS (
        SELECT 
            DATE(from_iso8601_timestamp(timestamp)) as date,
            coin_id,
            symbol,
            AVG(current_price_usd) as avg_price,
            MAX(high_24h) as high_24h,
            MIN(low_24h) as low_24h,
            AVG(total_volume_usd) as avg_volume,
            AVG(market_cap_usd) as market_cap
        FROM {DATABASE}.market_data
        WHERE coin_id IN ('bitcoin', 'ethereum')
        GROUP BY DATE(from_iso8601_timestamp(timestamp)), coin_id, symbol
    ),
    
    crypto_pivoted AS (
        SELECT 
            date,
            MAX(CASE WHEN coin_id = 'bitcoin' THEN avg_price END) as btc_price,
            MAX(CASE WHEN coin_id = 'bitcoin' THEN avg_volume END) as btc_volume,
            MAX(CASE WHEN coin_id = 'bitcoin' THEN market_cap END) as btc_market_cap,
            MAX(CASE WHEN coin_id = 'ethereum' THEN avg_price END) as eth_price,
            MAX(CASE WHEN coin_id = 'ethereum' THEN avg_volume END) as eth_volume,
            MAX(CASE WHEN coin_id = 'ethereum' THEN market_cap END) as eth_market_cap
        FROM crypto_daily
        GROUP BY date
    ),
    
    macro_interest_rates AS (
        SELECT 
            CAST(date AS DATE) as date,
            MAX(CASE WHEN series_name = 'fed_funds_rate' THEN value END) as fed_funds_rate,
            MAX(CASE WHEN series_name = '10y_treasury' THEN value END) as treasury_10y,
            MAX(CASE WHEN series_name = '2y_treasury' THEN value END) as treasury_2y
        FROM {DATABASE}.interest_rates
        GROUP BY CAST(date AS DATE)
    ),
    
    macro_inflation AS (
        SELECT 
            CAST(date AS DATE) as date,
            MAX(CASE WHEN series_name = 'cpi' THEN value END) as cpi,
            MAX(CASE WHEN series_name = 'core_cpi' THEN value END) as core_cpi,
            MAX(CASE WHEN series_name = 'pce' THEN value END) as pce
        FROM {DATABASE}.inflation
        GROUP BY CAST(date AS DATE)
    ),
    
    macro_employment AS (
        SELECT 
            CAST(date AS DATE) as date,
            value as unemployment_rate
        FROM {DATABASE}.employment
        WHERE series_name = 'unemployment_rate'
    ),
    
    macro_gdp AS (
        SELECT 
            CAST(date AS DATE) as date,
            value as gdp
        FROM {DATABASE}.gdp
        WHERE series_name = 'gdp'
    ),
    
    fomc_sentiment_data AS (
        SELECT 
            CAST(date AS DATE) as date,
            regime as fomc_regime,
            sentiment_compound as fomc_sentiment_score,
            sentiment_positive as fomc_positive,
            sentiment_negative as fomc_negative
        FROM {DATABASE}.sentiment
    ),
    
    calendar AS (
        SELECT DISTINCT DATE(from_iso8601_timestamp(timestamp)) as date
        FROM {DATABASE}.market_data
        WHERE coin_id = 'bitcoin'
    )
    
    SELECT 
        c.date,
        cp.btc_price,
        cp.btc_volume,
        cp.btc_market_cap,
        cp.eth_price,
        cp.eth_volume,
        cp.eth_market_cap,
        mir.fed_funds_rate,
        mir.treasury_10y,
        mir.treasury_2y,
        mi.cpi,
        mi.core_cpi,
        mi.pce,
        me.unemployment_rate,
        mgdp.gdp,
        fs.fomc_regime,
        fs.fomc_sentiment_score,
        fs.fomc_positive,
        fs.fomc_negative,
        (cp.btc_price - LAG(cp.btc_price) OVER (ORDER BY c.date)) / 
            NULLIF(LAG(cp.btc_price) OVER (ORDER BY c.date), 0) * 100 as btc_return_pct,
        (cp.eth_price - LAG(cp.eth_price) OVER (ORDER BY c.date)) / 
            NULLIF(LAG(cp.eth_price) OVER (ORDER BY c.date), 0) * 100 as eth_return_pct,
        STDDEV(cp.btc_price) OVER (
            ORDER BY c.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as btc_volatility_30d,
        STDDEV(cp.eth_price) OVER (
            ORDER BY c.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as eth_volatility_30d,
        CAST(YEAR(c.date) AS VARCHAR) as year,
        CAST(LPAD(CAST(MONTH(c.date) AS VARCHAR), 2, '0') AS VARCHAR) as month
        
    FROM calendar c
    LEFT JOIN crypto_pivoted cp ON c.date = cp.date
    LEFT JOIN macro_interest_rates mir ON c.date = mir.date
    LEFT JOIN macro_inflation mi ON c.date = mi.date
    LEFT JOIN macro_employment me ON c.date = me.date
    LEFT JOIN macro_gdp mgdp ON c.date = mgdp.date
    LEFT JOIN fomc_sentiment_data fs ON DATE_TRUNC('month', c.date) = DATE_TRUNC('month', fs.date)
    
    WHERE c.date >= DATE('2024-01-01')
    ORDER BY c.date
    """
    
    return query


def execute_athena_query(query: str) -> str:
    """Execute Athena query and return execution ID"""
    
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT}
    )
    
    execution_id = response['QueryExecutionId']
    print(f"  🔄 Query submitted: {execution_id}")
    
    return execution_id


def wait_for_query_completion(execution_id: str, max_wait: int = 600) -> None:
    """Wait for Athena query to complete"""
    
    start_time = time.time()
    
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=execution_id)
        status = response['QueryExecution']['Status']['State']
        
        if status == 'SUCCEEDED':
            elapsed = time.time() - start_time
            print(f"  ✅ Query completed in {elapsed:.1f}s")
            return
        
        elif status in ['FAILED', 'CANCELLED']:
            reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
            raise Exception(f"Query {status}: {reason}")
        
        # Check timeout
        if time.time() - start_time > max_wait:
            raise Exception(f"Query timeout after {max_wait}s")
        
        time.sleep(5)


def get_result_count_value(execution_id: str) -> int:
    """Get the actual count value from query results"""
    try:
        response = athena_client.get_query_results(QueryExecutionId=execution_id)
        if len(response['ResultSet']['Rows']) > 1:
            count_value = response['ResultSet']['Rows'][1]['Data'][0].get('VarCharValue', '0')
            return int(count_value)
        return 0
    except Exception as e:
        print(f"  ⚠️ Could not get count value: {e}")
        return 0