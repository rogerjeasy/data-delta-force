-- Accuracy of sentiment reversals predicting volatility spikes
SELECT 
    sentiment_reversal_flag as sentiment_signal,
    COUNT(*) as total_observations,
    SUM(CASE WHEN volatility_spike_flag = true THEN 1 ELSE 0 END) as vol_spikes_detected,
    ROUND(SUM(CASE WHEN volatility_spike_flag = true THEN 1 ELSE 0 END) * 100.0 
        / COUNT(*), 2) as detection_accuracy_pct,
    CASE 
        WHEN SUM(CASE WHEN volatility_spike_flag = true THEN 1 ELSE 0 END) * 100.0 
             / COUNT(*) > 70 
        THEN 'PASS (>70%)'
        ELSE 'FAIL (<70%)'
    END as target_status
FROM crypto_macro_db.gold_sentiment_transmission
WHERE sentiment_reversal_flag IS NOT NULL
GROUP BY sentiment_reversal_flag
ORDER BY sentiment_reversal_flag DESC;