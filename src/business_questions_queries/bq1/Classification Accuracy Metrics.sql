-- Regime classification accuracy assessment
SELECT 
    current_regime,
    COUNT(*) as total_predictions,
    SUM(CASE WHEN next_regime_7d = current_regime THEN 1 ELSE 0 END) as correct_predictions,
    ROUND(SUM(CASE WHEN next_regime_7d = current_regime THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as accuracy_pct,
    SUM(CASE WHEN next_regime_7d != current_regime THEN 1 ELSE 0 END) as incorrect_predictions
FROM crypto_macro_db.bq1_regime_transitions
WHERE next_regime_7d IS NOT NULL
GROUP BY current_regime
ORDER BY accuracy_pct DESC;