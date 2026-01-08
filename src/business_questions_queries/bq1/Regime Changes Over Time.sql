-- Temporal distribution of regime changes
SELECT 
    DATE_TRUNC('month', date) as month,
    COUNT(*) as total_days,
    SUM(CASE WHEN regime_changed THEN 1 ELSE 0 END) as regime_changes,
    COUNT(DISTINCT current_regime) as unique_regimes_in_month
FROM crypto_macro_db.bq1_regime_transitions
GROUP BY DATE_TRUNC('month', date)
ORDER BY month;