SELECT 
    COUNT(CASE WHEN warning_lead_time > 0 THEN 1 END) as warnings_issued,
    ROUND(AVG(warning_lead_time), 2) as avg_lead_time_days,
    APPROX_PERCENTILE(warning_lead_time, 0.5) as median_lead_time_days,
    MIN(warning_lead_time) as min_lead_time,
    MAX(warning_lead_time) as max_lead_time,
    ROUND(STDDEV(warning_lead_time), 2) as std_dev_lead_time,
    CASE 
        WHEN APPROX_PERCENTILE(warning_lead_time, 0.5) >= 2 
             AND APPROX_PERCENTILE(warning_lead_time, 0.5) <= 3
        THEN 'TARGET MET (48-72h)'
        ELSE 'TARGET MISSED'
    END as lead_time_target_status
FROM crypto_macro_db.bq5_crisis_detection
WHERE warning_lead_time > 0;