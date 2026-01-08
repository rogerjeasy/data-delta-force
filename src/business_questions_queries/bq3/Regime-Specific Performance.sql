-- Performance breakdown by regime
SELECT 
    overall_regime,
    COUNT(*) as days_in_regime,
    ROUND(AVG(strategy_return_1d), 4) as avg_strategy_return,
    ROUND(AVG(benchmark_return_1d), 4) as avg_benchmark_return,
    ROUND(AVG(alpha_1d), 4) as avg_alpha,
    ROUND(SUM(alpha_1d), 2) as total_alpha_generated,
    ROUND(SUM(CASE WHEN signal_correct = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) 
        as signal_accuracy_pct
FROM crypto_macro_db.bq3_tactical_signals
WHERE overall_regime IS NOT NULL
GROUP BY overall_regime
ORDER BY total_alpha_generated DESC;