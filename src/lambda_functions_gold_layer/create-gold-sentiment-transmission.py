"""
AWS Lambda: Create Gold Sentiment Transmission Dataset
Analyzes lead-lag relationships between FOMC sentiment and crypto markets
Includes Granger causality preparation and spillover effects
"""

import json
import boto3
import time
from datetime import datetime
import os

# Configuration
DATABASE = os.environ.get('DATABASE', 'crypto_macro_db')
S3_BUCKET = os.environ['S3_BUCKET']
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
S3_OUTPUT = f"s3://{S3_BUCKET}/athena-query-results/"
GOLD_OUTPUT_PATH = f"s3://{S3_BUCKET}/gold/sentiment_transmission/"

# Initialize clients
athena_client = boto3.client('athena', region_name=AWS_REGION)
s3_client = boto3.client('s3', region_name=AWS_REGION)


def lambda_handler(event, context):
    """Create sentiment transmission dataset"""
    
    print("=" * 80)
    print("💭 GOLD LAYER: Sentiment Transmission Dataset Creation")
    print("=" * 80)
    
    timestamp = datetime.utcnow()
    
    try:
        # Step 1: Drop existing table
        print("\n🗑️  Step 1: Dropping existing table...")
        drop_query = f"DROP TABLE IF EXISTS {DATABASE}.gold_sentiment_transmission"
        drop_execution_id = execute_athena_query(drop_query)
        wait_for_query_completion(drop_execution_id)
        
        # Step 2: Create table structure
        print("\n🔨 Step 2: Creating table structure...")
        create_query = build_create_table_query()
        create_execution_id = execute_athena_query(create_query)
        wait_for_query_completion(create_execution_id)
        
        # Step 3: Insert transmission analysis
        print("\n📥 Step 3: Analyzing sentiment transmission...")
        insert_query = build_insert_query()
        insert_execution_id = execute_athena_query(insert_query)
        wait_for_query_completion(insert_execution_id)
        
        # Step 4: Get row count
        print("\n🔢 Step 4: Counting records...")
        count_query = f"SELECT COUNT(*) as record_count FROM {DATABASE}.gold_sentiment_transmission"
        count_execution_id = execute_athena_query(count_query)
        wait_for_query_completion(count_execution_id)
        
        row_count = get_result_count_value(count_execution_id)
        
        print("\n" + "=" * 80)
        print("✅ SENTIMENT TRANSMISSION CREATION COMPLETE!")
        print(f"📊 Records created: {row_count}")
        print(f"📁 Output: {GOLD_OUTPUT_PATH}")
        print("=" * 80)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Sentiment transmission dataset created successfully',
                'timestamp': timestamp.isoformat(),
                'records_created': row_count,
                'output_path': GOLD_OUTPUT_PATH
            })
        }
        
    except Exception as e:
        print(f"❌ Sentiment transmission creation failed: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Sentiment transmission creation failed',
                'error': str(e),
                'timestamp': timestamp.isoformat()
            })
        }


def build_create_table_query() -> str:
    """Build CREATE TABLE query"""
    
    query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.gold_sentiment_transmission (
        date DATE,
        
        -- FOMC sentiment (contemporaneous and lags)
        fomc_sentiment_score DOUBLE,
        fomc_regime VARCHAR(20),
        fomc_positive DOUBLE,
        fomc_negative DOUBLE,
        fomc_sentiment_lag1 DOUBLE,
        fomc_sentiment_lag3 DOUBLE,
        fomc_sentiment_lag5 DOUBLE,
        fomc_sentiment_lag7 DOUBLE,
        fomc_regime_lag1 VARCHAR(20),
        
        -- Crypto market response
        btc_return_pct DOUBLE,
        btc_volatility_30d DOUBLE,
        eth_return_pct DOUBLE,
        eth_volatility_30d DOUBLE,
        
        -- Forward returns (for lead analysis)
        btc_return_forward1 DOUBLE,
        btc_return_forward3 DOUBLE,
        btc_return_forward5 DOUBLE,
        btc_return_forward7 DOUBLE,
        
        -- Volatility changes
        btc_vol_change_1d DOUBLE,
        btc_vol_change_3d DOUBLE,
        btc_vol_change_7d DOUBLE,
        
        -- Rolling correlations (sentiment vs returns)
        sentiment_btc_corr_30d DOUBLE,
        sentiment_btc_corr_60d DOUBLE,
        sentiment_eth_corr_30d DOUBLE,
        
        -- Transmission coefficients (rolling regression approximation)
        sentiment_to_btc_coef_30d DOUBLE,
        sentiment_to_eth_coef_30d DOUBLE,
        
        -- Regime-based transmission
        regime_volatility_impact DOUBLE,
        days_since_fomc_change INT,
        
        -- Granger causality preparation (X causes Y if past X predicts Y)
        granger_x_lag1 DOUBLE,
        granger_x_lag2 DOUBLE,
        granger_y_current DOUBLE,
        granger_y_lag1 DOUBLE,
        
        -- Spillover indicators
        volatility_spike_flag BOOLEAN,
        sentiment_reversal_flag BOOLEAN,
        transmission_strength VARCHAR(20)
    )
    PARTITIONED BY (year STRING, month STRING)
    STORED AS PARQUET
    LOCATION '{GOLD_OUTPUT_PATH}'
    """
    
    return query


def build_insert_query() -> str:
    """Build INSERT query with sentiment transmission analysis"""
    
    query = f"""
    INSERT INTO {DATABASE}.gold_sentiment_transmission
    
    WITH base_data AS (
        SELECT 
            date,
            fomc_sentiment_score,
            fomc_regime,
            fomc_positive,
            fomc_negative,
            btc_return_pct,
            btc_volatility_30d,
            eth_return_pct,
            eth_volatility_30d
        FROM {DATABASE}.gold_master_analytics
        WHERE date >= DATE('2024-01-01')
        ORDER BY date
    ),
    
    with_lags_leads AS (
        SELECT 
            date,
            fomc_sentiment_score,
            fomc_regime,
            fomc_positive,
            fomc_negative,
            btc_return_pct,
            btc_volatility_30d,
            eth_return_pct,
            eth_volatility_30d,
            
            -- Sentiment lags
            LAG(fomc_sentiment_score, 1) OVER (ORDER BY date) as fomc_sentiment_lag1,
            LAG(fomc_sentiment_score, 3) OVER (ORDER BY date) as fomc_sentiment_lag3,
            LAG(fomc_sentiment_score, 5) OVER (ORDER BY date) as fomc_sentiment_lag5,
            LAG(fomc_sentiment_score, 7) OVER (ORDER BY date) as fomc_sentiment_lag7,
            LAG(fomc_regime, 1) OVER (ORDER BY date) as fomc_regime_lag1,
            
            -- Forward returns
            LEAD(btc_return_pct, 1) OVER (ORDER BY date) as btc_return_forward1,
            LEAD(btc_return_pct, 3) OVER (ORDER BY date) as btc_return_forward3,
            LEAD(btc_return_pct, 5) OVER (ORDER BY date) as btc_return_forward5,
            LEAD(btc_return_pct, 7) OVER (ORDER BY date) as btc_return_forward7,
            
            -- Volatility changes
            btc_volatility_30d - LAG(btc_volatility_30d, 1) OVER (ORDER BY date) as btc_vol_change_1d,
            btc_volatility_30d - LAG(btc_volatility_30d, 3) OVER (ORDER BY date) as btc_vol_change_3d,
            btc_volatility_30d - LAG(btc_volatility_30d, 7) OVER (ORDER BY date) as btc_vol_change_7d,
            
            -- Granger causality variables
            LAG(fomc_sentiment_score, 1) OVER (ORDER BY date) as granger_x_lag1,
            LAG(fomc_sentiment_score, 2) OVER (ORDER BY date) as granger_x_lag2,
            btc_return_pct as granger_y_current,
            LAG(btc_return_pct, 1) OVER (ORDER BY date) as granger_y_lag1,
            
            -- Regime change flag
            CASE 
                WHEN fomc_regime != LAG(fomc_regime) OVER (ORDER BY date) THEN 1
                ELSE 0
            END as regime_change_flag
            
        FROM base_data
    ),
    
    with_correlations AS (
        SELECT 
            date,
            fomc_sentiment_score,
            fomc_regime,
            fomc_positive,
            fomc_negative,
            fomc_sentiment_lag1,
            fomc_sentiment_lag3,
            fomc_sentiment_lag5,
            fomc_sentiment_lag7,
            fomc_regime_lag1,
            btc_return_pct,
            btc_volatility_30d,
            eth_return_pct,
            eth_volatility_30d,
            btc_return_forward1,
            btc_return_forward3,
            btc_return_forward5,
            btc_return_forward7,
            btc_vol_change_1d,
            btc_vol_change_3d,
            btc_vol_change_7d,
            granger_x_lag1,
            granger_x_lag2,
            granger_y_current,
            granger_y_lag1,
            regime_change_flag,
            
            -- Rolling correlations (sentiment vs returns)
            CORR(fomc_sentiment_score, btc_return_pct) OVER (
                ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as sentiment_btc_corr_30d,
            
            CORR(fomc_sentiment_score, btc_return_pct) OVER (
                ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) as sentiment_btc_corr_60d,
            
            CORR(fomc_sentiment_score, eth_return_pct) OVER (
                ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as sentiment_eth_corr_30d,
            
            -- Transmission coefficients (covariance / variance = beta)
            COVAR_POP(fomc_sentiment_score, btc_return_pct) OVER (
                ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) / NULLIF(VAR_POP(fomc_sentiment_score) OVER (
                ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ), 0) as sentiment_to_btc_coef_30d,
            
            COVAR_POP(fomc_sentiment_score, eth_return_pct) OVER (
                ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) / NULLIF(VAR_POP(fomc_sentiment_score) OVER (
                ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ), 0) as sentiment_to_eth_coef_30d,
            
            -- Regime volatility impact (avg vol by regime)
            AVG(btc_volatility_30d) OVER (
                PARTITION BY fomc_regime ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) - AVG(btc_volatility_30d) OVER (
                ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as regime_volatility_impact
            
        FROM with_lags_leads
    ),
    
    with_flags AS (
        SELECT 
            date,
            fomc_sentiment_score,
            fomc_regime,
            fomc_positive,
            fomc_negative,
            fomc_sentiment_lag1,
            fomc_sentiment_lag3,
            fomc_sentiment_lag5,
            fomc_sentiment_lag7,
            fomc_regime_lag1,
            btc_return_pct,
            btc_volatility_30d,
            eth_return_pct,
            eth_volatility_30d,
            btc_return_forward1,
            btc_return_forward3,
            btc_return_forward5,
            btc_return_forward7,
            btc_vol_change_1d,
            btc_vol_change_3d,
            btc_vol_change_7d,
            sentiment_btc_corr_30d,
            sentiment_btc_corr_60d,
            sentiment_eth_corr_30d,
            sentiment_to_btc_coef_30d,
            sentiment_to_eth_coef_30d,
            regime_volatility_impact,
            granger_x_lag1,
            granger_x_lag2,
            granger_y_current,
            granger_y_lag1,
            regime_change_flag,
            
            -- Volatility spike flag (vol increased > 20% in 3 days)
            CASE 
                WHEN btc_vol_change_3d > 0.2 * LAG(btc_volatility_30d, 3) OVER (ORDER BY date) THEN true
                ELSE false
            END as volatility_spike_flag,
            
            -- Sentiment reversal flag (sentiment changed sign)
            CASE 
                WHEN (fomc_sentiment_score > 0 AND fomc_sentiment_lag1 < 0) 
                  OR (fomc_sentiment_score < 0 AND fomc_sentiment_lag1 > 0) THEN true
                ELSE false
            END as sentiment_reversal_flag,
            
            -- Transmission strength classification
            CASE 
                WHEN ABS(sentiment_btc_corr_30d) > 0.5 THEN 'strong'
                WHEN ABS(sentiment_btc_corr_30d) > 0.3 THEN 'moderate'
                WHEN ABS(sentiment_btc_corr_30d) > 0.1 THEN 'weak'
                ELSE 'negligible'
            END as transmission_strength
            
        FROM with_correlations
    )
    
    SELECT 
        date,
        fomc_sentiment_score,
        fomc_regime,
        fomc_positive,
        fomc_negative,
        fomc_sentiment_lag1,
        fomc_sentiment_lag3,
        fomc_sentiment_lag5,
        fomc_sentiment_lag7,
        fomc_regime_lag1,
        btc_return_pct,
        btc_volatility_30d,
        eth_return_pct,
        eth_volatility_30d,
        btc_return_forward1,
        btc_return_forward3,
        btc_return_forward5,
        btc_return_forward7,
        btc_vol_change_1d,
        btc_vol_change_3d,
        btc_vol_change_7d,
        sentiment_btc_corr_30d,
        sentiment_btc_corr_60d,
        sentiment_eth_corr_30d,
        sentiment_to_btc_coef_30d,
        sentiment_to_eth_coef_30d,
        regime_volatility_impact,
        NULL as days_since_fomc_change,
        granger_x_lag1,
        granger_x_lag2,
        granger_y_current,
        granger_y_lag1,
        volatility_spike_flag,
        sentiment_reversal_flag,
        transmission_strength,
        
        -- Partitioning
        CAST(YEAR(date) AS VARCHAR) as year,
        CAST(LPAD(CAST(MONTH(date) AS VARCHAR), 2, '0') AS VARCHAR) as month
        
    FROM with_flags
    WHERE date >= DATE('2024-02-01')  -- Start after lag period
    ORDER BY date
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