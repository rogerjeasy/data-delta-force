"""
AWS Lambda: Create Gold Risk Metrics Dataset
Calculates portfolio risk measures: VaR, CVaR, Sharpe, drawdowns, beta
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
GOLD_OUTPUT_PATH = f"s3://{S3_BUCKET}/gold/risk_metrics/"

# Initialize clients
athena_client = boto3.client('athena', region_name=AWS_REGION)
s3_client = boto3.client('s3', region_name=AWS_REGION)


def lambda_handler(event, context):
    """Create risk metrics dataset"""
    
    print("=" * 80)
    print("📊 GOLD LAYER: Risk Metrics Dataset Creation")
    print("=" * 80)
    
    timestamp = datetime.utcnow()
    
    try:
        # Step 1: Drop existing table
        print("\n🗑️  Step 1: Dropping existing table...")
        drop_query = f"DROP TABLE IF EXISTS {DATABASE}.gold_risk_metrics"
        drop_execution_id = execute_athena_query(drop_query)
        wait_for_query_completion(drop_execution_id)
        
        # Step 2: Create table structure
        print("\n🔨 Step 2: Creating table structure...")
        create_query = build_create_table_query()
        create_execution_id = execute_athena_query(create_query)
        wait_for_query_completion(create_execution_id)
        
        # Step 3: Insert risk metrics
        print("\n📥 Step 3: Calculating risk metrics...")
        insert_query = build_insert_query()
        insert_execution_id = execute_athena_query(insert_query)
        wait_for_query_completion(insert_execution_id)
        
        # Step 4: Get row count
        print("\n🔢 Step 4: Counting records...")
        count_query = f"SELECT COUNT(*) as record_count FROM {DATABASE}.gold_risk_metrics"
        count_execution_id = execute_athena_query(count_query)
        wait_for_query_completion(count_execution_id)
        
        row_count = get_result_count_value(count_execution_id)
        
        print("\n" + "=" * 80)
        print("✅ RISK METRICS CREATION COMPLETE!")
        print(f"📊 Records created: {row_count}")
        print(f"📁 Output: {GOLD_OUTPUT_PATH}")
        print("=" * 80)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Risk metrics dataset created successfully',
                'timestamp': timestamp.isoformat(),
                'records_created': row_count,
                'output_path': GOLD_OUTPUT_PATH
            })
        }
        
    except Exception as e:
        print(f"❌ Risk metrics creation failed: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Risk metrics creation failed',
                'error': str(e),
                'timestamp': timestamp.isoformat()
            })
        }


def build_create_table_query() -> str:
    """Build CREATE TABLE query"""
    
    query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.gold_risk_metrics (
        date DATE,
        asset VARCHAR(20),
        
        -- Price and return metrics
        price DOUBLE,
        return_1d DOUBLE,
        return_7d DOUBLE,
        return_30d DOUBLE,
        
        -- Volatility metrics (annualized)
        volatility_30d DOUBLE,
        volatility_60d DOUBLE,
        volatility_90d DOUBLE,
        
        -- Value at Risk (VaR)
        var_95_1d DOUBLE,
        var_99_1d DOUBLE,
        var_95_30d DOUBLE,
        
        -- Conditional Value at Risk (CVaR/Expected Shortfall)
        cvar_95_1d DOUBLE,
        cvar_99_1d DOUBLE,
        
        -- Risk-adjusted returns
        sharpe_ratio_30d DOUBLE,
        sharpe_ratio_60d DOUBLE,
        sharpe_ratio_90d DOUBLE,
        sortino_ratio_30d DOUBLE,
        
        -- Drawdown metrics
        peak_price DOUBLE,
        current_drawdown_pct DOUBLE,
        max_drawdown_30d DOUBLE,
        max_drawdown_90d DOUBLE,
        max_drawdown_ytd DOUBLE,
        days_since_peak INT,
        
        -- Beta and correlation (vs macro factors)
        beta_fedfunds_30d DOUBLE,
        beta_fedfunds_60d DOUBLE,
        correlation_to_rates_30d DOUBLE,
        correlation_to_inflation_30d DOUBLE,
        
        -- Risk classification
        risk_level VARCHAR(20),
        drawdown_severity VARCHAR(20)
    )
    PARTITIONED BY (year STRING, month STRING)
    STORED AS PARQUET
    LOCATION '{GOLD_OUTPUT_PATH}'
    """
    
    return query


def build_insert_query() -> str:
    """Build INSERT query with risk metric calculations"""
    
    query = f"""
    INSERT INTO {DATABASE}.gold_risk_metrics
    
    WITH base_data AS (
        SELECT 
            date,
            btc_price,
            eth_price,
            btc_return_pct,
            eth_return_pct,
            btc_volatility_30d,
            eth_volatility_30d,
            fed_funds_rate,
            cpi
        FROM {DATABASE}.gold_master_analytics
        WHERE date >= DATE('2024-01-01')
        ORDER BY date
    ),
    
    returns_calculated AS (
        SELECT 
            date,
            btc_price,
            eth_price,
            btc_return_pct,
            eth_return_pct,
            btc_volatility_30d,
            eth_volatility_30d,
            fed_funds_rate,
            cpi,
            
            -- Multi-period returns
            ((btc_price - LAG(btc_price, 7) OVER (ORDER BY date)) / 
                NULLIF(LAG(btc_price, 7) OVER (ORDER BY date), 0)) * 100 as btc_return_7d,
            ((btc_price - LAG(btc_price, 30) OVER (ORDER BY date)) / 
                NULLIF(LAG(btc_price, 30) OVER (ORDER BY date), 0)) * 100 as btc_return_30d,
                
            ((eth_price - LAG(eth_price, 7) OVER (ORDER BY date)) / 
                NULLIF(LAG(eth_price, 7) OVER (ORDER BY date), 0)) * 100 as eth_return_7d,
            ((eth_price - LAG(eth_price, 30) OVER (ORDER BY date)) / 
                NULLIF(LAG(eth_price, 30) OVER (ORDER BY date), 0)) * 100 as eth_return_30d,
            
            -- Rolling volatilities (annualized: daily_vol * sqrt(365))
            STDDEV(btc_return_pct) OVER (
                ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) * SQRT(365) as btc_vol_60d_annual,
            STDDEV(btc_return_pct) OVER (
                ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
            ) * SQRT(365) as btc_vol_90d_annual,
            
            STDDEV(eth_return_pct) OVER (
                ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) * SQRT(365) as eth_vol_60d_annual,
            STDDEV(eth_return_pct) OVER (
                ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
            ) * SQRT(365) as eth_vol_90d_annual,
            
            -- Peak tracking for drawdown
            MAX(btc_price) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as btc_peak,
            MAX(eth_price) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as eth_peak,
            
            -- 30-day and 90-day rolling peaks
            MAX(btc_price) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as btc_peak_30d,
            MAX(btc_price) OVER (ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) as btc_peak_90d,
            MAX(eth_price) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as eth_peak_30d,
            MAX(eth_price) OVER (ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) as eth_peak_90d,
            
            -- YTD peak (reset each year)
            MAX(btc_price) OVER (
                PARTITION BY YEAR(date) ORDER BY date
            ) as btc_peak_ytd,
            MAX(eth_price) OVER (
                PARTITION BY YEAR(date) ORDER BY date
            ) as eth_peak_ytd
            
        FROM base_data
    ),
    
    risk_metrics AS (
        SELECT 
            date,
            
            -- BTC metrics
            btc_price,
            btc_return_pct as btc_return_1d,
            btc_return_7d,
            btc_return_30d,
            
            -- Annualized volatilities
            btc_volatility_30d * SQRT(365) as btc_vol_30d_annual,
            btc_vol_60d_annual,
            btc_vol_90d_annual,
            
            -- VaR calculations (parametric method: -z * volatility)
            -- 1-day VaR at 95% confidence (z = 1.65)
            -1.65 * btc_volatility_30d as btc_var_95_1d,
            -- 1-day VaR at 99% confidence (z = 2.33)
            -2.33 * btc_volatility_30d as btc_var_99_1d,
            -- 30-day VaR at 95% (scale by sqrt(30))
            -1.65 * btc_volatility_30d * SQRT(30) as btc_var_95_30d,
            
            -- CVaR/Expected Shortfall (approximation: 1.4 * VaR for normal distribution)
            -1.65 * btc_volatility_30d * 1.4 as btc_cvar_95_1d,
            -2.33 * btc_volatility_30d * 1.4 as btc_cvar_99_1d,
            
            -- Sharpe Ratio (assuming risk-free rate ~ 4% annual = 0.011% daily)
            CASE 
                WHEN btc_volatility_30d > 0 THEN 
                    (AVG(btc_return_pct) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) - 0.011) 
                    / btc_volatility_30d * SQRT(365)
                ELSE NULL 
            END as btc_sharpe_30d,
            
            CASE 
                WHEN btc_vol_60d_annual > 0 THEN 
                    (AVG(btc_return_pct) OVER (ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) * 365 - 4.0) 
                    / btc_vol_60d_annual
                ELSE NULL 
            END as btc_sharpe_60d,
            
            CASE 
                WHEN btc_vol_90d_annual > 0 THEN 
                    (AVG(btc_return_pct) OVER (ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) * 365 - 4.0) 
                    / btc_vol_90d_annual
                ELSE NULL 
            END as btc_sharpe_90d,
            
            -- Sortino Ratio (downside deviation only)
            CASE 
                WHEN STDDEV(CASE WHEN btc_return_pct < 0 THEN btc_return_pct END) 
                    OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) > 0 THEN
                    (AVG(btc_return_pct) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) - 0.011) 
                    / STDDEV(CASE WHEN btc_return_pct < 0 THEN btc_return_pct END) 
                        OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) * SQRT(365)
                ELSE NULL 
            END as btc_sortino_30d,
            
            -- Drawdown calculations
            btc_peak,
            ((btc_price - btc_peak) / btc_peak) * 100 as btc_current_drawdown,
            ((btc_peak_30d - MIN(btc_price) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)) 
                / btc_peak_30d) * 100 as btc_max_drawdown_30d,
            ((btc_peak_90d - MIN(btc_price) OVER (ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)) 
                / btc_peak_90d) * 100 as btc_max_drawdown_90d,
            ((btc_peak_ytd - MIN(btc_price) OVER (PARTITION BY YEAR(date) ORDER BY date)) 
                / btc_peak_ytd) * 100 as btc_max_drawdown_ytd,
            
            -- Days since peak
            date - MAX(CASE WHEN btc_price = btc_peak THEN date END) 
                OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as btc_days_since_peak,
            
            -- Beta calculations (vs Fed Funds Rate changes)
            COALESCE(
                COVAR_POP(btc_return_pct, fed_funds_rate) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
                / NULLIF(VAR_POP(fed_funds_rate) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0),
                0
            ) as btc_beta_fedfunds_30d,
            
            COALESCE(
                COVAR_POP(btc_return_pct, fed_funds_rate) OVER (ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) 
                / NULLIF(VAR_POP(fed_funds_rate) OVER (ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW), 0),
                0
            ) as btc_beta_fedfunds_60d,
            
            -- Correlations to macro factors
            CORR(btc_return_pct, fed_funds_rate) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as btc_corr_rates_30d,
            CORR(btc_return_pct, cpi) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as btc_corr_inflation_30d,
            
            -- ETH metrics (same structure)
            eth_price,
            eth_return_pct as eth_return_1d,
            eth_return_7d,
            eth_return_30d,
            eth_volatility_30d * SQRT(365) as eth_vol_30d_annual,
            eth_vol_60d_annual,
            eth_vol_90d_annual,
            -1.65 * eth_volatility_30d as eth_var_95_1d,
            -2.33 * eth_volatility_30d as eth_var_99_1d,
            -1.65 * eth_volatility_30d * SQRT(30) as eth_var_95_30d,
            -1.65 * eth_volatility_30d * 1.4 as eth_cvar_95_1d,
            -2.33 * eth_volatility_30d * 1.4 as eth_cvar_99_1d,
            
            CASE 
                WHEN eth_volatility_30d > 0 THEN 
                    (AVG(eth_return_pct) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) - 0.011) 
                    / eth_volatility_30d * SQRT(365)
                ELSE NULL 
            END as eth_sharpe_30d,
            
            CASE 
                WHEN eth_vol_60d_annual > 0 THEN 
                    (AVG(eth_return_pct) OVER (ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) * 365 - 4.0) 
                    / eth_vol_60d_annual
                ELSE NULL 
            END as eth_sharpe_60d,
            
            CASE 
                WHEN eth_vol_90d_annual > 0 THEN 
                    (AVG(eth_return_pct) OVER (ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) * 365 - 4.0) 
                    / eth_vol_90d_annual
                ELSE NULL 
            END as eth_sharpe_90d,
            
            CASE 
                WHEN STDDEV(CASE WHEN eth_return_pct < 0 THEN eth_return_pct END) 
                    OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) > 0 THEN
                    (AVG(eth_return_pct) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) - 0.011) 
                    / STDDEV(CASE WHEN eth_return_pct < 0 THEN eth_return_pct END) 
                        OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) * SQRT(365)
                ELSE NULL 
            END as eth_sortino_30d,
            
            eth_peak,
            ((eth_price - eth_peak) / eth_peak) * 100 as eth_current_drawdown,
            ((eth_peak_30d - MIN(eth_price) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)) 
                / eth_peak_30d) * 100 as eth_max_drawdown_30d,
            ((eth_peak_90d - MIN(eth_price) OVER (ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)) 
                / eth_peak_90d) * 100 as eth_max_drawdown_90d,
            ((eth_peak_ytd - MIN(eth_price) OVER (PARTITION BY YEAR(date) ORDER BY date)) 
                / eth_peak_ytd) * 100 as eth_max_drawdown_ytd,
            
            date - MAX(CASE WHEN eth_price = eth_peak THEN date END) 
                OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as eth_days_since_peak,
            
            COALESCE(
                COVAR_POP(eth_return_pct, fed_funds_rate) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
                / NULLIF(VAR_POP(fed_funds_rate) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0),
                0
            ) as eth_beta_fedfunds_30d,
            
            COALESCE(
                COVAR_POP(eth_return_pct, fed_funds_rate) OVER (ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) 
                / NULLIF(VAR_POP(fed_funds_rate) OVER (ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW), 0),
                0
            ) as eth_beta_fedfunds_60d,
            
            CORR(eth_return_pct, fed_funds_rate) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as eth_corr_rates_30d,
            CORR(eth_return_pct, cpi) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as eth_corr_inflation_30d
            
        FROM returns_calculated
    ),
    
    unpivoted AS (
        -- BTC records
        SELECT 
            date,
            'BTC' as asset,
            btc_price as price,
            btc_return_1d as return_1d,
            btc_return_7d as return_7d,
            btc_return_30d as return_30d,
            btc_vol_30d_annual as volatility_30d,
            btc_vol_60d_annual as volatility_60d,
            btc_vol_90d_annual as volatility_90d,
            btc_var_95_1d as var_95_1d,
            btc_var_99_1d as var_99_1d,
            btc_var_95_30d as var_95_30d,
            btc_cvar_95_1d as cvar_95_1d,
            btc_cvar_99_1d as cvar_99_1d,
            btc_sharpe_30d as sharpe_ratio_30d,
            btc_sharpe_60d as sharpe_ratio_60d,
            btc_sharpe_90d as sharpe_ratio_90d,
            btc_sortino_30d as sortino_ratio_30d,
            btc_peak as peak_price,
            btc_current_drawdown as current_drawdown_pct,
            btc_max_drawdown_30d as max_drawdown_30d,
            btc_max_drawdown_90d as max_drawdown_90d,
            btc_max_drawdown_ytd as max_drawdown_ytd,
            DATE_DIFF('day', 
                MAX(CASE WHEN btc_price = btc_peak THEN date END) 
                    OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 
                date
            ) as days_since_peak,
            btc_beta_fedfunds_30d as beta_fedfunds_30d,
            btc_beta_fedfunds_60d as beta_fedfunds_60d,
            btc_corr_rates_30d as correlation_to_rates_30d,
            btc_corr_inflation_30d as correlation_to_inflation_30d
        FROM risk_metrics
        
        UNION ALL
        
        -- ETH records
        SELECT 
            date,
            'ETH' as asset,
            eth_price as price,
            eth_return_1d as return_1d,
            eth_return_7d as return_7d,
            eth_return_30d as return_30d,
            eth_vol_30d_annual as volatility_30d,
            eth_vol_60d_annual as volatility_60d,
            eth_vol_90d_annual as volatility_90d,
            eth_var_95_1d as var_95_1d,
            eth_var_99_1d as var_99_1d,
            eth_var_95_30d as var_95_30d,
            eth_cvar_95_1d as cvar_95_1d,
            eth_cvar_99_1d as cvar_99_1d,
            eth_sharpe_30d as sharpe_ratio_30d,
            eth_sharpe_60d as sharpe_ratio_60d,
            eth_sharpe_90d as sharpe_ratio_90d,
            eth_sortino_30d as sortino_ratio_30d,
            eth_peak as peak_price,
            eth_current_drawdown as current_drawdown_pct,
            eth_max_drawdown_30d as max_drawdown_30d,
            eth_max_drawdown_90d as max_drawdown_90d,
            eth_max_drawdown_ytd as max_drawdown_ytd,
            DATE_DIFF('day', 
                MAX(CASE WHEN eth_price = eth_peak THEN date END) 
                    OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 
                date
            ) as days_since_peak,
            eth_beta_fedfunds_30d as beta_fedfunds_30d,
            eth_beta_fedfunds_60d as beta_fedfunds_60d,
            eth_corr_rates_30d as correlation_to_rates_30d,
            eth_corr_inflation_30d as correlation_to_inflation_30d
        FROM risk_metrics
    )
    
    SELECT 
        date,
        asset,
        price,
        return_1d,
        return_7d,
        return_30d,
        volatility_30d,
        volatility_60d,
        volatility_90d,
        var_95_1d,
        var_99_1d,
        var_95_30d,
        cvar_95_1d,
        cvar_99_1d,
        sharpe_ratio_30d,
        sharpe_ratio_60d,
        sharpe_ratio_90d,
        sortino_ratio_30d,
        peak_price,
        current_drawdown_pct,
        max_drawdown_30d,
        max_drawdown_90d,
        max_drawdown_ytd,
        days_since_peak,
        beta_fedfunds_30d,
        beta_fedfunds_60d,
        correlation_to_rates_30d,
        correlation_to_inflation_30d,
        
        -- Risk level classification
        CASE 
            WHEN volatility_30d > 100 THEN 'extreme'
            WHEN volatility_30d > 75 THEN 'high'
            WHEN volatility_30d > 50 THEN 'moderate'
            ELSE 'low'
        END as risk_level,
        
        -- Drawdown severity
        CASE 
            WHEN current_drawdown_pct < -30 THEN 'severe'
            WHEN current_drawdown_pct < -20 THEN 'major'
            WHEN current_drawdown_pct < -10 THEN 'moderate'
            WHEN current_drawdown_pct < -5 THEN 'minor'
            ELSE 'none'
        END as drawdown_severity,
        
        -- Partitioning
        CAST(YEAR(date) AS VARCHAR) as year,
        CAST(LPAD(CAST(MONTH(date) AS VARCHAR), 2, '0') AS VARCHAR) as month
        
    FROM unpivoted
    WHERE date >= DATE('2024-03-01')  -- Start after 90-day window
    ORDER BY date, asset
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