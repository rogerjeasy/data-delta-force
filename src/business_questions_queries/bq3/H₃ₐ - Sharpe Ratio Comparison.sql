-- Compare strategy vs benchmark Sharpe ratios
SELECT 
    'Tactical Strategy' as portfolio,
    COUNT(*) as trading_days,
    ROUND(AVG(strategy_return_1d), 4) as avg_daily_return_pct,
    ROUND(STDDEV(strategy_return_1d), 4) as std_daily_return,
    ROUND(AVG(strategy_return_1d) / NULLIF(STDDEV(strategy_return_1d), 0) * SQRT(365), 4) 
        as annualized_sharpe,
    ROUND(MAX(strategy_cumulative_return), 2) as total_return_pct,
    ROUND(MIN(strategy_drawdown), 2) as max_drawdown_pct
FROM crypto_macro_db.bq3_tactical_signals

UNION ALL

SELECT 
    'Buy-and-Hold (10% BTC)',
    COUNT(*),
    ROUND(AVG(benchmark_return_1d), 4),
    ROUND(STDDEV(benchmark_return_1d), 4),
    ROUND(AVG(benchmark_return_1d) / NULLIF(STDDEV(benchmark_return_1d), 0) * SQRT(365), 4),
    ROUND(MAX(benchmark_cumulative_return), 2),
    ROUND((MIN(benchmark_cumulative_return) - MAX(benchmark_cumulative_return)), 2)
FROM crypto_macro_db.bq3_tactical_signals;