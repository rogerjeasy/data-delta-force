-- Time series of cumulative returns
SELECT 
    date,
    overall_regime,
    signal,
    ROUND(strategy_cumulative_return, 2) as strategy_cum_return_pct,
    ROUND(benchmark_cumulative_return, 2) as benchmark_cum_return_pct,
    ROUND(strategy_cumulative_return - benchmark_cumulative_return, 2) as outperformance_pct,
    ROUND(strategy_sharpe_rolling_30d, 4) as rolling_sharpe,
    ROUND(strategy_drawdown, 2) as current_drawdown_pct
FROM crypto_macro_db.bq3_tactical_signals
ORDER BY date;