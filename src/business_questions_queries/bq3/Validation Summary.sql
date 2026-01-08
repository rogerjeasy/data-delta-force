-- Complete hypothesis validation
SELECT 
    'H3a: Sharpe Ratio > 0.5' as hypothesis,
    CAST(ROUND(AVG(strategy_return_1d) / NULLIF(STDDEV(strategy_return_1d), 0) * SQRT(365), 4) AS VARCHAR) 
        as result_value,
    '> 0.5 (target)' as target,
    CASE 
        WHEN AVG(strategy_return_1d) / NULLIF(STDDEV(strategy_return_1d), 0) * SQRT(365) > 0.5 
        THEN 'VALIDATED'
        ELSE 'NOT VALIDATED'
    END as status
FROM crypto_macro_db.bq3_tactical_signals

UNION ALL

SELECT 
    'H3b: Drawdown Reduction > 30%',
    CAST(ROUND((ABS(MIN(benchmark_cumulative_return) - MAX(benchmark_cumulative_return)) - 
                ABS(MIN(strategy_drawdown))) / 
          NULLIF(ABS(MIN(benchmark_cumulative_return) - MAX(benchmark_cumulative_return)), 0) * 100, 2) AS VARCHAR) || '%',
    '> 30%',
    CASE 
        WHEN (ABS(MIN(benchmark_cumulative_return) - MAX(benchmark_cumulative_return)) - 
              ABS(MIN(strategy_drawdown))) / 
          NULLIF(ABS(MIN(benchmark_cumulative_return) - MAX(benchmark_cumulative_return)), 0) * 100 > 30 
        THEN 'VALIDATED'
        ELSE 'NOT VALIDATED'
    END
FROM crypto_macro_db.bq3_tactical_signals

UNION ALL

SELECT 
    'H3c: Signal Accuracy > 65%',
    CAST(ROUND(SUM(CASE WHEN signal_correct = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS VARCHAR) || '%',
    '> 65%',
    CASE 
        WHEN SUM(CASE WHEN signal_correct = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 65 
        THEN 'VALIDATED'
        ELSE 'NOT VALIDATED'
    END
FROM crypto_macro_db.bq3_tactical_signals;