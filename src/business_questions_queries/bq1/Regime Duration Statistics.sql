-- Regime persistence and stability analysis
SELECT 
    current_regime,
    COUNT(*) as total_days_observed,
    ROUND(AVG(days_in_current_regime), 1) as avg_duration_days,
    ROUND(STDDEV(days_in_current_regime), 1) as std_duration,
    MIN(days_in_current_regime) as min_duration,
    MAX(days_in_current_regime) as max_duration,
    SUM(CASE WHEN regime_changed THEN 1 ELSE 0 END) as number_of_entries
FROM crypto_macro_db.bq1_regime_transitions
WHERE current_regime IS NOT NULL
GROUP BY current_regime
ORDER BY total_days_observed DESC;