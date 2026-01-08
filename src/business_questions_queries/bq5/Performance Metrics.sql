-- Detection rate, false positive rate, precision, recall
WITH metrics AS (
    SELECT 
        SUM(CASE WHEN true_positive = true THEN 1 ELSE 0 END) as tp,
        SUM(CASE WHEN false_positive = true THEN 1 ELSE 0 END) as fp,
        SUM(CASE WHEN false_negative = true THEN 1 ELSE 0 END) as fn,
        SUM(CASE WHEN true_negative = true THEN 1 ELSE 0 END) as tn
    FROM crypto_macro_db.bq5_crisis_detection
)

SELECT 
    tp as true_positives,
    fp as false_positives,
    fn as false_negatives,
    tn as true_negatives,
    ROUND(tp * 100.0 / NULLIF(tp + fn, 0), 2) as detection_rate_pct,
    ROUND(fp * 100.0 / NULLIF(fp + tn, 0), 2) as false_positive_rate_pct,
    ROUND(tp * 100.0 / NULLIF(tp + fp, 0), 2) as precision_pct,
    ROUND(tp * 100.0 / NULLIF(tp + fn, 0), 2) as recall_pct,
    ROUND((tp + tn) * 100.0 / NULLIF(tp + fp + fn + tn, 0), 2) as accuracy_pct,
    CASE 
        WHEN tp * 100.0 / NULLIF(tp + fn, 0) > 75 
             AND fp * 100.0 / NULLIF(fp + tn, 0) < 20
        THEN 'H5c: VALIDATED'
        ELSE 'H5c: NOT VALIDATED'
    END as hypothesis_status
FROM metrics;