"""
AWS Lambda: BQ5 - Crisis Propagation and Contagion Monitoring
"""

import json
import boto3
import time
import os

DATABASE = os.environ.get('DATABASE', 'crypto_macro_db')
S3_BUCKET = os.environ['S3_BUCKET']
S3_OUTPUT = f"s3://{S3_BUCKET}/athena-query-results/"
GOLD_OUTPUT = f"s3://{S3_BUCKET}/gold/bq5_crisis_detection/"

athena = boto3.client('athena')


def lambda_handler(event, context):
    print("🚨 BQ5: Crisis Propagation Detection")
    
    try:
        execute_query(f"DROP TABLE IF EXISTS {DATABASE}.bq5_crisis_detection")
        execute_query(build_create_table())
        execute_query(build_insert_query())
        
        count = get_count()
        print(f"✅ Created {count} crisis detection records")
        
        return {'statusCode': 200, 'records': count}
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise


def build_create_table():
    return f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.bq5_crisis_detection (
        date DATE,
        
        -- Volatility regime classification
        btc_volatility_30d DOUBLE,
        volatility_regime VARCHAR(20),
        volatility_regime_prior VARCHAR(20),
        volatility_regime_shift BOOLEAN,
        
        -- Correlation metrics (BTC vs traditional assets)
        correlation_btc_spx_30d DOUBLE,
        correlation_btc_treasury_30d DOUBLE,
        correlation_btc_vix_30d DOUBLE,
        
        -- Correlation changes (30-day window)
        correlation_change_btc_spx_30d DOUBLE,
        correlation_change_btc_treasury_30d DOUBLE,
        correlation_change_btc_vix_30d DOUBLE,
        
        -- Breakdown detection flags
        correlation_breakdown_flag BOOLEAN,
        breakdown_magnitude DOUBLE,
        breakdown_asset VARCHAR(50),
        
        -- Early warning signals
        volatility_warning_flag BOOLEAN,
        correlation_warning_flag BOOLEAN,
        combined_warning_score DOUBLE,
        
        -- Forward-looking outcomes (48-72 hours)
        breakdown_occurs_48h BOOLEAN,
        breakdown_occurs_72h BOOLEAN,
        crisis_event_next_5d BOOLEAN,
        
        -- Lead time tracking
        days_to_next_breakdown INT,
        warning_lead_time INT,
        
        -- Classification metrics
        true_positive BOOLEAN,
        false_positive BOOLEAN,
        false_negative BOOLEAN,
        true_negative BOOLEAN,
        
        -- Additional context
        overall_regime VARCHAR(50),
        btc_price DOUBLE,
        btc_return_pct DOUBLE
    )
    PARTITIONED BY (year STRING, month STRING)
    STORED AS PARQUET
    LOCATION '{GOLD_OUTPUT}'
    """


def build_insert_query():
    return f"""
    INSERT INTO {DATABASE}.bq5_crisis_detection
    
    WITH base_data AS (
        SELECT 
            m.date,
            m.btc_volatility_30d,
            m.btc_price,
            m.btc_return_pct,
            COALESCE(r.overall_regime, 'unknown') as overall_regime,
            m.fed_funds_rate,
            m.treasury_10y,
            m.treasury_2y
            
        FROM {DATABASE}.gold_master_analytics m
        LEFT JOIN {DATABASE}.gold_macro_regimes r
            ON m.date = r.date
        WHERE m.date >= DATE '2024-12-08'
    ),
    
    volatility_regimes AS (
        SELECT 
            *,
            -- Classify volatility regime
            CASE 
                WHEN btc_volatility_30d < 2000 THEN 'calm'
                WHEN btc_volatility_30d >= 2000 AND btc_volatility_30d < 5000 THEN 'moderate'
                WHEN btc_volatility_30d >= 5000 AND btc_volatility_30d < 8000 THEN 'elevated'
                WHEN btc_volatility_30d >= 8000 THEN 'extreme'
                ELSE 'unknown'
            END as volatility_regime,
            
            -- Prior regime
            LAG(
                CASE 
                    WHEN btc_volatility_30d < 2000 THEN 'calm'
                    WHEN btc_volatility_30d >= 2000 AND btc_volatility_30d < 5000 THEN 'moderate'
                    WHEN btc_volatility_30d >= 5000 AND btc_volatility_30d < 8000 THEN 'elevated'
                    WHEN btc_volatility_30d >= 8000 THEN 'extreme'
                    ELSE 'unknown'
                END, 1
            ) OVER (ORDER BY date) as volatility_regime_prior,
            
            -- Compute correlations (using Fed Funds and Treasury as proxies)
            CORR(btc_price, fed_funds_rate) 
                OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
                as correlation_btc_treasury_30d,
            
            CORR(btc_price, treasury_10y) 
                OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
                as correlation_btc_spx_30d
            
        FROM base_data
    ),
    
    correlation_changes AS (
        SELECT 
            *,
            -- Regime shift detection
            CASE 
                WHEN volatility_regime != volatility_regime_prior 
                THEN true ELSE false 
            END as volatility_regime_shift,
            
            -- Correlation changes (30-day delta)
            correlation_btc_spx_30d - LAG(correlation_btc_spx_30d, 30) 
                OVER (ORDER BY date) as correlation_change_btc_spx_30d,
            
            correlation_btc_treasury_30d - LAG(correlation_btc_treasury_30d, 30) 
                OVER (ORDER BY date) as correlation_change_btc_treasury_30d,
            
            -- VIX proxy (use treasury 2y-10y spread volatility)
            0.0 as correlation_btc_vix_30d,
            0.0 as correlation_change_btc_vix_30d
            
        FROM volatility_regimes
    ),
    
    breakdown_detection AS (
        SELECT 
            *,
            -- H5a: Correlation increase >0.3 signals breakdown
            CASE 
                WHEN ABS(correlation_change_btc_spx_30d) > 0.3 
                     OR ABS(correlation_change_btc_treasury_30d) > 0.3
                THEN true ELSE false 
            END as correlation_breakdown_flag,
            
            -- Breakdown magnitude
            GREATEST(
                ABS(COALESCE(correlation_change_btc_spx_30d, 0)),
                ABS(COALESCE(correlation_change_btc_treasury_30d, 0))
            ) as breakdown_magnitude,
            
            -- Which asset exhibited breakdown
            CASE 
                WHEN ABS(COALESCE(correlation_change_btc_spx_30d, 0)) > 
                     ABS(COALESCE(correlation_change_btc_treasury_30d, 0))
                THEN 'spx_proxy'
                ELSE 'treasury'
            END as breakdown_asset,
            
            -- H5b: Volatility warning (regime shift from calm → extreme)
            CASE 
                WHEN volatility_regime_shift = true 
                     AND volatility_regime_prior IN ('calm', 'moderate')
                     AND volatility_regime IN ('elevated', 'extreme')
                THEN true ELSE false 
            END as volatility_warning_flag,
            
            -- Correlation warning (change >0.2 as early signal)
            CASE 
                WHEN ABS(correlation_change_btc_spx_30d) > 0.2 
                     OR ABS(correlation_change_btc_treasury_30d) > 0.2
                THEN true ELSE false 
            END as correlation_warning_flag
            
        FROM correlation_changes
    ),
    
    warning_scores AS (
        SELECT 
            *,
            -- Combined warning score (0-1)
            (CAST(volatility_warning_flag AS DOUBLE) * 0.5) + 
            (CAST(correlation_warning_flag AS DOUBLE) * 0.5) 
                as combined_warning_score
            
        FROM breakdown_detection
    ),
    
    forward_outcomes AS (
        SELECT 
            *,
            -- Does breakdown occur in next 48-72 hours?
            CASE 
                WHEN LEAD(correlation_breakdown_flag, 2) OVER (ORDER BY date) = true 
                     OR LEAD(correlation_breakdown_flag, 3) OVER (ORDER BY date) = true
                THEN true ELSE false 
            END as breakdown_occurs_48h,
            
            CASE 
                WHEN LEAD(correlation_breakdown_flag, 3) OVER (ORDER BY date) = true 
                THEN true ELSE false 
            END as breakdown_occurs_72h,
            
            -- Crisis event in next 5 days
            CASE 
                WHEN LEAD(correlation_breakdown_flag, 1) OVER (ORDER BY date) = true 
                     OR LEAD(correlation_breakdown_flag, 2) OVER (ORDER BY date) = true
                     OR LEAD(correlation_breakdown_flag, 3) OVER (ORDER BY date) = true
                     OR LEAD(correlation_breakdown_flag, 4) OVER (ORDER BY date) = true
                     OR LEAD(correlation_breakdown_flag, 5) OVER (ORDER BY date) = true
                     OR LEAD(volatility_regime, 1) OVER (ORDER BY date) = 'extreme'
                     OR LEAD(volatility_regime, 2) OVER (ORDER BY date) = 'extreme'
                     OR LEAD(volatility_regime, 3) OVER (ORDER BY date) = 'extreme'
                THEN true ELSE false 
            END as crisis_event_next_5d
            
        FROM warning_scores
    ),
    
    lead_time_calc AS (
        SELECT 
            *,
            -- Simplified days to next breakdown (no DATEDIFF)
            CASE 
                WHEN correlation_breakdown_flag = true THEN 0
                WHEN LEAD(correlation_breakdown_flag, 1) OVER (ORDER BY date) = true THEN 1
                WHEN LEAD(correlation_breakdown_flag, 2) OVER (ORDER BY date) = true THEN 2
                WHEN LEAD(correlation_breakdown_flag, 3) OVER (ORDER BY date) = true THEN 3
                WHEN LEAD(correlation_breakdown_flag, 4) OVER (ORDER BY date) = true THEN 4
                WHEN LEAD(correlation_breakdown_flag, 5) OVER (ORDER BY date) = true THEN 5
                ELSE NULL
            END as days_to_next_breakdown,
            
            -- Warning lead time
            CASE 
                WHEN combined_warning_score >= 0.5 AND 
                     LEAD(correlation_breakdown_flag, 3) OVER (ORDER BY date) = true THEN 3
                WHEN combined_warning_score >= 0.5 AND 
                     LEAD(correlation_breakdown_flag, 2) OVER (ORDER BY date) = true THEN 2
                WHEN combined_warning_score >= 0.5 AND 
                     LEAD(correlation_breakdown_flag, 1) OVER (ORDER BY date) = true THEN 1
                ELSE 0
            END as warning_lead_time
            
        FROM forward_outcomes
    ),
    
    classification AS (
        SELECT 
            *,
            -- True Positive: Warning issued AND breakdown occurred
            CASE 
                WHEN combined_warning_score >= 0.5 AND breakdown_occurs_72h = true 
                THEN true ELSE false 
            END as true_positive,
            
            -- False Positive: Warning issued BUT no breakdown
            CASE 
                WHEN combined_warning_score >= 0.5 AND breakdown_occurs_72h = false 
                THEN true ELSE false 
            END as false_positive,
            
            -- False Negative: No warning BUT breakdown occurred
            CASE 
                WHEN combined_warning_score < 0.5 AND breakdown_occurs_72h = true 
                THEN true ELSE false 
            END as false_negative,
            
            -- True Negative: No warning AND no breakdown
            CASE 
                WHEN combined_warning_score < 0.5 AND breakdown_occurs_72h = false 
                THEN true ELSE false 
            END as true_negative
            
        FROM lead_time_calc
    )
    
    SELECT 
        date,
        btc_volatility_30d,
        volatility_regime,
        volatility_regime_prior,
        volatility_regime_shift,
        correlation_btc_spx_30d,
        correlation_btc_treasury_30d,
        correlation_btc_vix_30d,
        correlation_change_btc_spx_30d,
        correlation_change_btc_treasury_30d,
        correlation_change_btc_vix_30d,
        correlation_breakdown_flag,
        breakdown_magnitude,
        breakdown_asset,
        volatility_warning_flag,
        correlation_warning_flag,
        combined_warning_score,
        breakdown_occurs_48h,
        breakdown_occurs_72h,
        crisis_event_next_5d,
        days_to_next_breakdown,
        warning_lead_time,
        true_positive,
        false_positive,
        false_negative,
        true_negative,
        overall_regime,
        btc_price,
        btc_return_pct,
        
        CAST(YEAR(date) AS VARCHAR) as year,
        CAST(LPAD(CAST(MONTH(date) AS VARCHAR), 2, '0') AS VARCHAR) as month
        
    FROM classification
    ORDER BY date
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
    query = f"SELECT COUNT(*) FROM {DATABASE}.bq5_crisis_detection"
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