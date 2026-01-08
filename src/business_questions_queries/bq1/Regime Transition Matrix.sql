-- Calculate transition probability matrix (H1b validation)
SELECT 
    previous_regime,
    next_regime_7d as transitions_to,
    COUNT(*) as occurrences,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY previous_regime), 2) as probability_pct
FROM crypto_macro_db.bq1_regime_transitions
WHERE previous_regime IS NOT NULL 
    AND next_regime_7d IS NOT NULL
    AND previous_regime != 'null'
GROUP BY previous_regime, next_regime_7d
ORDER BY previous_regime, probability_pct DESC;