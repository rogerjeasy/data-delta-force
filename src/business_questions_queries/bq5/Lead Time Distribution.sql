-- Distribution of warning lead times
SELECT 
    warning_lead_time as lead_time_days,
    COUNT(*) as occurrences,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    ROUND(AVG(breakdown_magnitude), 4) as avg_breakdown_magnitude
FROM crypto_macro_db.bq5_crisis_detection
WHERE warning_lead_time > 0
GROUP BY warning_lead_time
ORDER BY warning_lead_time;