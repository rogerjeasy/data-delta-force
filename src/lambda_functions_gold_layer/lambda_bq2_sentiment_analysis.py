"""
AWS Lambda: BQ2 - Sentiment Transmission Analysis
"""

import json
import boto3
import time
import os

DATABASE = os.environ.get('DATABASE', 'crypto_macro_db')
S3_BUCKET = os.environ['S3_BUCKET']
S3_OUTPUT = f"s3://{S3_BUCKET}/athena-query-results/"
GOLD_OUTPUT = f"s3://{S3_BUCKET}/gold/bq2_sentiment_transmission/"

athena = boto3.client('athena')


def lambda_handler(event, context):
    print("🔬 BQ2: Sentiment Transmission Analysis")
    
    try:
        # Drop existing
        execute_query(f"DROP TABLE IF EXISTS {DATABASE}.bq2_sentiment_transmission")
        
        # Create table
        execute_query(build_create_table())
        
        # Insert data
        execute_query(build_insert_query())
        
        # Count
        count = get_count()
        print(f"✅ Created {count} records")
        
        return {'statusCode': 200, 'records': count}
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise


def build_create_table():
    return f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.bq2_sentiment_transmission (
        date DATE,
        
        -- Sentiment scores
        fomc_sentiment_score DOUBLE,
        sentiment_classification VARCHAR(20),
        sentiment_change_1d DOUBLE,
        sentiment_change_7d DOUBLE,
        
        -- Bitcoin returns
        btc_return_1d DOUBLE,
        btc_return_3d DOUBLE,
        btc_return_5d DOUBLE,
        btc_return_7d DOUBLE,
        
        -- Forward returns (for prediction testing)
        btc_return_fwd_1d DOUBLE,
        btc_return_fwd_3d DOUBLE,
        btc_return_fwd_7d DOUBLE,
        
        -- Volatility
        btc_volatility_30d DOUBLE,
        volatility_spike_flag BOOLEAN,
        
        -- Regime
        overall_regime VARCHAR(50),
        
        -- Granger causality prep
        sentiment_lag_1d DOUBLE,
        sentiment_lag_3d DOUBLE,
        sentiment_lag_7d DOUBLE
    )
    PARTITIONED BY (year STRING, month STRING)
    STORED AS PARQUET
    LOCATION '{GOLD_OUTPUT}'
    """


def build_insert_query():
    return f"""
    INSERT INTO {DATABASE}.bq2_sentiment_transmission
    
    WITH sentiment_data AS (
        SELECT 
            date,
            sentiment_score as fomc_sentiment_score,
            CASE 
                WHEN sentiment_score > 0.1 THEN 'hawkish'
                WHEN sentiment_score < -0.1 THEN 'dovish'
                ELSE 'neutral'
            END as sentiment_classification,
            sentiment_score - LAG(sentiment_score, 1) OVER (ORDER BY date) 
                as sentiment_change_1d,
            sentiment_score - LAG(sentiment_score, 7) OVER (ORDER BY date) 
                as sentiment_change_7d,
            LAG(sentiment_score, 1) OVER (ORDER BY date) as sentiment_lag_1d,
            LAG(sentiment_score, 3) OVER (ORDER BY date) as sentiment_lag_3d,
            LAG(sentiment_score, 7) OVER (ORDER BY date) as sentiment_lag_7d
        FROM {DATABASE}.gold_sentiment_transmission
    ),
    
    btc_data AS (
        SELECT 
            date,
            btc_price,
            overall_regime,
            btc_volatility_30d,
            
            -- Calculate returns
            (btc_price - LAG(btc_price, 1) OVER (ORDER BY date)) 
                / NULLIF(LAG(btc_price, 1) OVER (ORDER BY date), 0) * 100 
                as btc_return_1d,
            
            (btc_price - LAG(btc_price, 3) OVER (ORDER BY date)) 
                / NULLIF(LAG(btc_price, 3) OVER (ORDER BY date), 0) * 100 
                as btc_return_3d,
            
            (btc_price - LAG(btc_price, 5) OVER (ORDER BY date)) 
                / NULLIF(LAG(btc_price, 5) OVER (ORDER BY date), 0) * 100 
                as btc_return_5d,
            
            (btc_price - LAG(btc_price, 7) OVER (ORDER BY date)) 
                / NULLIF(LAG(btc_price, 7) OVER (ORDER BY date), 0) * 100 
                as btc_return_7d,
            
            -- Forward returns (for prediction)
            (LEAD(btc_price, 1) OVER (ORDER BY date) - btc_price) 
                / NULLIF(btc_price, 0) * 100 
                as btc_return_fwd_1d,
            
            (LEAD(btc_price, 3) OVER (ORDER BY date) - btc_price) 
                / NULLIF(btc_price, 0) * 100 
                as btc_return_fwd_3d,
            
            (LEAD(btc_price, 7) OVER (ORDER BY date) - btc_price) 
                / NULLIF(btc_price, 0) * 100 
                as btc_return_fwd_7d,
            
            -- Volatility spike detection
            CASE 
                WHEN btc_volatility_30d > LAG(btc_volatility_30d, 1) OVER (ORDER BY date) * 1.5 
                THEN true 
                ELSE false 
            END as volatility_spike_flag
            
        FROM {DATABASE}.gold_master_analytics
    )
    
    SELECT 
        b.date,
        s.fomc_sentiment_score,
        s.sentiment_classification,
        s.sentiment_change_1d,
        s.sentiment_change_7d,
        b.btc_return_1d,
        b.btc_return_3d,
        b.btc_return_5d,
        b.btc_return_7d,
        b.btc_return_fwd_1d,
        b.btc_return_fwd_3d,
        b.btc_return_fwd_7d,
        b.btc_volatility_30d,
        b.volatility_spike_flag,
        b.overall_regime,
        s.sentiment_lag_1d,
        s.sentiment_lag_3d,
        s.sentiment_lag_7d,
        
        CAST(YEAR(b.date) AS VARCHAR) as year,
        CAST(LPAD(CAST(MONTH(b.date) AS VARCHAR), 2, '0') AS VARCHAR) as month
        
    FROM btc_data b
    LEFT JOIN sentiment_data s ON b.date = s.date
    WHERE b.date >= DATE '2024-03-01'
    ORDER BY b.date
    """


def execute_query(query):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT}
    )
    
    execution_id = response['QueryExecutionId']
    
    while True:
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status['QueryExecution']['Status']['State']
        
        if state == 'SUCCEEDED':
            return
        elif state in ['FAILED', 'CANCELLED']:
            reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
            raise Exception(f"Query {state}: {reason}")
        
        time.sleep(3)


def get_count():
    query = f"SELECT COUNT(*) FROM {DATABASE}.bq2_sentiment_transmission"
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT}
    )
    time.sleep(5)
    result = athena.get_query_results(QueryExecutionId=response['QueryExecutionId'])
    if len(result['ResultSet']['Rows']) > 1:
        return int(result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
    return 0