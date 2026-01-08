-- Current early warning status
SELECT 
    warning_reason,
    COUNT(*) as alert_count,
    ROUND(AVG(days_in_current_regime), 1) as avg_days_in_regime,
    ROUND(AVG(ABS(fed_rate_change_30d)), 3) as avg_fed_rate_change,
    ROUND(AVG(btc_vol_change_30d), 0) as avg_btc_vol_change
FROM crypto_macro_db.bq1_regime_transitions
WHERE transition_warning_flag = true
GROUP BY warning_reason
ORDER BY alert_count DESC;