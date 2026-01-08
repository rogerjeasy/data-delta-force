-- Drawdown comparison and reduction percentage
WITH drawdown_stats AS (
    SELECT 
        MIN(strategy_drawdown) as strategy_max_dd,
        MIN(benchmark_cumulative_return) - MAX(benchmark_cumulative_return) as benchmark_max_dd
    FROM crypto_macro_db.bq3_tactical_signals
)

SELECT 
    ROUND(strategy_max_dd, 2) as strategy_max_drawdown_pct,
    ROUND(benchmark_max_dd, 2) as benchmark_max_drawdown_pct,
    ROUND(ABS(strategy_max_dd), 2) as strategy_dd_absolute,
    ROUND(ABS(benchmark_max_dd), 2) as benchmark_dd_absolute,
    ROUND((ABS(benchmark_max_dd) - ABS(strategy_max_dd)) / NULLIF(ABS(benchmark_max_dd), 0) * 100, 2) 
        as drawdown_reduction_pct,
    CASE 
        WHEN (ABS(benchmark_max_dd) - ABS(strategy_max_dd)) / NULLIF(ABS(benchmark_max_dd), 0) * 100 > 30 
        THEN 'H3b: VALIDATED'
        ELSE 'H3b: NOT VALIDATED'
    END as hypothesis_status
FROM drawdown_stats;