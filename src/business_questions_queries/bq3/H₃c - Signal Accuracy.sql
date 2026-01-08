SELECT 
    COUNT(*) as total_signals,
    SUM(CASE WHEN signal_correct = true THEN 1 ELSE 0 END) as correct_signals,
    ROUND(SUM(CASE WHEN signal_correct = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) 
        as overall_accuracy_pct,
    CASE 
        WHEN SUM(CASE WHEN signal_correct = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 65 
        THEN 'H3c: VALIDATED'
        ELSE 'H3c: NOT VALIDATED'
    END as hypothesis_status
FROM crypto_macro_db.bq3_tactical_signals;