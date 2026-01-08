-- Measure prediction lead time for regime changes (H1c)
SELECT 
    '7-Day Lead Time' as forecast_horizon,
    COUNT(*) as total_forecasts,
    SUM(CASE WHEN next_regime_7d != current_regime THEN 1 ELSE 0 END) as predicted_transitions,
    ROUND(SUM(CASE WHEN next_regime_7d != current_regime THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as transition_detection_rate,
    SUM(CASE WHEN transition_warning_flag = true AND next_regime_7d != current_regime THEN 1 ELSE 0 END) as correct_warnings,
    ROUND(SUM(CASE WHEN transition_warning_flag = true AND next_regime_7d != current_regime THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN next_regime_7d != current_regime THEN 1 ELSE 0 END), 0), 2) as warning_accuracy_pct
FROM crypto_macro_db.bq1_regime_transitions
WHERE next_regime_7d IS NOT NULL

UNION ALL

SELECT 
    '14-Day Lead Time',
    COUNT(*),
    SUM(CASE WHEN next_regime_14d != current_regime THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN next_regime_14d != current_regime THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
    SUM(CASE WHEN transition_warning_flag = true AND next_regime_14d != current_regime THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN transition_warning_flag = true AND next_regime_14d != current_regime THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN next_regime_14d != current_regime THEN 1 ELSE 0 END), 0), 2)
FROM crypto_macro_db.bq1_regime_transitions
WHERE next_regime_14d IS NOT NULL;