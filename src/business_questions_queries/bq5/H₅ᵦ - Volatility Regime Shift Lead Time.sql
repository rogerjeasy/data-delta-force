-- Test if volatility shifts precede correlation breakdowns by 2-5 days
WITH regime_shifts AS (
    SELECT 
        date,
        volatility_regime_shift,
        volatility_warning_flag,
        correlation_breakdown_flag,
        breakdown_occurs_48h,
        breakdown_occurs_72h,
        days_to_next_breakdown
    FROM crypto_macro_db.bq5_crisis_detection
    WHERE volatility_regime_shift = true
)

SELECT 
    'Volatility Regime Shifts' as event_type,
    COUNT(*) as total_shifts,
    SUM(CASE WHEN breakdown_occurs_48h = true THEN 1 ELSE 0 END) as breakdowns_48h,
    SUM(CASE WHEN breakdown_occurs_72h = true THEN 1 ELSE 0 END) as breakdowns_72h,
    ROUND(
        SUM(CASE WHEN breakdown_occurs_72h = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) as lead_success_rate_pct,
    ROUND(AVG(CASE WHEN days_to_next_breakdown <= 5 THEN days_to_next_breakdown END), 2) 
        as avg_lead_time_days,
    CASE 
        WHEN SUM(CASE WHEN breakdown_occurs_72h = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 50
        THEN 'H5b: VALIDATED'
        ELSE 'H5b: NOT VALIDATED'
    END as hypothesis_status
FROM regime_shifts;