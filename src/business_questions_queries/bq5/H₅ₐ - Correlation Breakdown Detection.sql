-- Test if correlation increases >0.3 signal crisis propagation
SELECT 
    correlation_breakdown_flag as breakdown_detected,
    COUNT(*) as total_events,
    SUM(CASE WHEN crisis_event_next_5d = true THEN 1 ELSE 0 END) as crisis_events,
    ROUND(
        SUM(CASE WHEN crisis_event_next_5d = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) as crisis_propagation_rate_pct,
    ROUND(AVG(breakdown_magnitude), 4) as avg_breakdown_magnitude,
    CASE 
        WHEN SUM(CASE WHEN crisis_event_next_5d = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 50
        THEN 'H5a: VALIDATED'
        ELSE 'H5a: NOT VALIDATED'
    END as hypothesis_status
FROM crypto_macro_db.bq5_crisis_detection
WHERE correlation_breakdown_flag IS NOT NULL
GROUP BY correlation_breakdown_flag
ORDER BY correlation_breakdown_flag DESC;