"""
AWS Lambda: BQ1 - Regime Transition Analysis
Calculates transition probabilities and early warning indicators
"""

import json
import boto3
import time
from datetime import datetime
import os

DATABASE = os.environ.get('DATABASE', 'crypto_macro_db')
S3_BUCKET = os.environ['S3_BUCKET']
S3_OUTPUT = f"s3://{S3_BUCKET}/athena-query-results/"
GOLD_OUTPUT_PATH = f"s3://{S3_BUCKET}/gold/bq1_regime_transitions/"

athena_client = boto3.client('athena')


def lambda_handler(event, context):
    print("🔄 Creating regime transition analysis...")
    
    try:
        # Drop + Create + Insert
        print("Step 1: Dropping table...")
        execute_and_wait(f"DROP TABLE IF EXISTS {DATABASE}.bq1_regime_transitions")
        
        print("Step 2: Creating table...")
        execute_and_wait(build_create_table())
        
        print("Step 3: Inserting data...")
        execute_and_wait(build_insert_query())
        
        print("Step 4: Counting records...")
        count = get_row_count()
        print(f"✅ Created {count} transition records")
        
        return {'statusCode': 200, 'records': count}
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        raise


def build_create_table():
    return f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.bq1_regime_transitions (
        date DATE,
        current_regime VARCHAR(50),
        previous_regime VARCHAR(50),
        regime_changed BOOLEAN,
        days_in_current_regime INT,
        
        -- Next regime (for validation)
        next_regime_7d VARCHAR(50),
        next_regime_14d VARCHAR(50),
        
        -- Leading indicators
        fed_rate_change_30d DOUBLE,
        cpi_change_30d DOUBLE,
        unemployment_change_30d DOUBLE,
        btc_vol_change_30d DOUBLE,
        
        -- Transition probability scores (0-1)
        prob_stay_same DOUBLE,
        prob_to_risk_off DOUBLE,
        prob_to_risk_on DOUBLE,
        prob_to_neutral DOUBLE,
        
        -- Early warning flag
        transition_warning_flag BOOLEAN,
        warning_reason VARCHAR(200)
    )
    PARTITIONED BY (year STRING, month STRING)
    STORED AS PARQUET
    LOCATION '{GOLD_OUTPUT_PATH}'
    """


def build_insert_query():
    return f"""
    INSERT INTO {DATABASE}.bq1_regime_transitions
    
    WITH regime_sequence AS (
        SELECT 
            date,
            overall_regime as current_regime,
            LAG(overall_regime) OVER (ORDER BY date) as previous_regime,
            LEAD(overall_regime, 7) OVER (ORDER BY date) as next_regime_7d,
            LEAD(overall_regime, 14) OVER (ORDER BY date) as next_regime_14d
        FROM {DATABASE}.gold_macro_regimes
        WHERE overall_regime IS NOT NULL
    ),
    
    with_changes AS (
        SELECT 
            r.date,
            r.current_regime,
            r.previous_regime,
            r.next_regime_7d,
            r.next_regime_14d,
            
            CASE 
                WHEN r.current_regime != r.previous_regime THEN true 
                ELSE false 
            END as regime_changed,
            
            -- Days in current regime (fixed calculation)
            CASE 
                WHEN r.current_regime != r.previous_regime THEN 1
                ELSE COALESCE(
                    DATE_DIFF('day', 
                        LAG(r.date) OVER (
                            PARTITION BY r.current_regime 
                            ORDER BY r.date
                        ), 
                        r.date
                    ), 
                    1
                )
            END as days_in_current_regime,
            
            -- Leading indicators
            m.fed_funds_rate - LAG(m.fed_funds_rate, 30) OVER (ORDER BY r.date) 
                as fed_rate_change_30d,
            m.cpi - LAG(m.cpi, 30) OVER (ORDER BY r.date) 
                as cpi_change_30d,
            m.unemployment_rate - LAG(m.unemployment_rate, 30) OVER (ORDER BY r.date) 
                as unemployment_change_30d,
            m.btc_volatility_30d - LAG(m.btc_volatility_30d, 30) OVER (ORDER BY r.date) 
                as btc_vol_change_30d
            
        FROM regime_sequence r
        JOIN {DATABASE}.gold_master_analytics m ON r.date = m.date
    ),
    
    with_probabilities AS (
        SELECT 
            date,
            current_regime,
            previous_regime,
            regime_changed,
            days_in_current_regime,
            next_regime_7d,
            next_regime_14d,
            fed_rate_change_30d,
            cpi_change_30d,
            unemployment_change_30d,
            btc_vol_change_30d,
            
            -- Simplified probability scores (rule-based)
            CASE 
                WHEN days_in_current_regime > 60 THEN 0.3
                WHEN days_in_current_regime > 30 THEN 0.6
                ELSE 0.8
            END as prob_stay_same,
            
            CASE 
                WHEN COALESCE(fed_rate_change_30d, 0) > 0.2 
                     OR COALESCE(btc_vol_change_30d, 0) > 1000 THEN 0.4
                WHEN current_regime LIKE '%risk_off%' THEN 0.7
                ELSE 0.2
            END as prob_to_risk_off,
            
            CASE 
                WHEN COALESCE(fed_rate_change_30d, 0) < -0.2 
                     AND COALESCE(btc_vol_change_30d, 0) < -500 THEN 0.5
                WHEN current_regime LIKE '%risk_on%' THEN 0.6
                ELSE 0.1
            END as prob_to_risk_on,
            
            0.3 as prob_to_neutral,
            
            -- Early warning flags
            CASE 
                WHEN ABS(COALESCE(fed_rate_change_30d, 0)) > 0.25 THEN true
                WHEN COALESCE(btc_vol_change_30d, 0) > 2000 THEN true
                WHEN days_in_current_regime > 90 THEN true
                ELSE false
            END as transition_warning_flag,
            
            CASE 
                WHEN ABS(COALESCE(fed_rate_change_30d, 0)) > 0.25 THEN 'Large Fed rate change detected'
                WHEN COALESCE(btc_vol_change_30d, 0) > 2000 THEN 'Volatility spike detected'
                WHEN days_in_current_regime > 90 THEN 'Extended regime duration'
                ELSE NULL
            END as warning_reason
            
        FROM with_changes
    )
    
    SELECT 
        date,
        current_regime,
        previous_regime,
        regime_changed,
        days_in_current_regime,
        next_regime_7d,
        next_regime_14d,
        COALESCE(fed_rate_change_30d, 0.0) as fed_rate_change_30d,
        COALESCE(cpi_change_30d, 0.0) as cpi_change_30d,
        COALESCE(unemployment_change_30d, 0.0) as unemployment_change_30d,
        COALESCE(btc_vol_change_30d, 0.0) as btc_vol_change_30d,
        prob_stay_same,
        prob_to_risk_off,
        prob_to_risk_on,
        prob_to_neutral,
        transition_warning_flag,
        warning_reason,
        
        CAST(YEAR(date) AS VARCHAR) as year,
        CAST(LPAD(CAST(MONTH(date) AS VARCHAR), 2, '0') AS VARCHAR) as month
        
    FROM with_probabilities
    ORDER BY date
    """


def execute_and_wait(query):
    """Execute query with detailed error reporting"""
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT}
    )
    
    execution_id = response['QueryExecutionId']
    print(f"  Query ID: {execution_id}")
    
    while True:
        status_response = athena_client.get_query_execution(
            QueryExecutionId=execution_id
        )
        status = status_response['QueryExecution']['Status']['State']
        
        if status == 'SUCCEEDED':
            print(f"  ✅ Query succeeded")
            return
            
        elif status in ['FAILED', 'CANCELLED']:
            # Get detailed error
            reason = status_response['QueryExecution']['Status'].get(
                'StateChangeReason', 'Unknown error'
            )
            error_message = status_response['QueryExecution']['Status'].get(
                'AthenaError', {}
            ).get('ErrorMessage', reason)
            
            print(f"  ❌ Query {status}")
            print(f"  Error: {error_message}")
            raise Exception(f"Query {status}: {error_message}")
        
        time.sleep(3)


def get_row_count():
    """Get count with error handling"""
    query = f"SELECT COUNT(*) as cnt FROM {DATABASE}.bq1_regime_transitions"
    
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT}
    )
    
    execution_id = response['QueryExecutionId']
    time.sleep(5)
    
    try:
        result = athena_client.get_query_results(QueryExecutionId=execution_id)
        if len(result['ResultSet']['Rows']) > 1:
            return int(result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
    except Exception as e:
        print(f"Warning: Could not get count: {e}")
    
    return 0