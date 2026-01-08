-- Analyze volatility regime shift patterns
SELECT 
    volatility_regime_prior as from_regime,
    volatility_regime as to_regime,
    COUNT(*) as transition_count,
    SUM(CASE WHEN correlation_breakdown_flag = true THEN 1 ELSE 0 END) 
        as breakdowns_during_transition,
    ROUND(
        SUM(CASE WHEN correlation_breakdown_flag = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) as breakdown_rate_pct
FROM crypto_macro_db.bq5_crisis_detection
WHERE volatility_regime_shift = true
GROUP BY volatility_regime_prior, volatility_regime
ORDER BY transition_count DESC;