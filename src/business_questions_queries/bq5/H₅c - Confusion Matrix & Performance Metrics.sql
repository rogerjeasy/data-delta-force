-- Calculate confusion matrix and detection metrics
SELECT 
    'True Positives' as classification,
    SUM(CASE WHEN true_positive = true THEN 1 ELSE 0 END) as count,
    ROUND(
        SUM(CASE WHEN true_positive = true THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 2
    ) as percentage
FROM crypto_macro_db.bq5_crisis_detection

UNION ALL

SELECT 
    'False Positives',
    SUM(CASE WHEN false_positive = true THEN 1 ELSE 0 END),
    ROUND(
        SUM(CASE WHEN false_positive = true THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 2
    )
FROM crypto_macro_db.bq5_crisis_detection

UNION ALL

SELECT 
    'False Negatives',
    SUM(CASE WHEN false_negative = true THEN 1 ELSE 0 END),
    ROUND(
        SUM(CASE WHEN false_negative = true THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 2
    )
FROM crypto_macro_db.bq5_crisis_detection

UNION ALL

SELECT 
    'True Negatives',
    SUM(CASE WHEN true_negative = true THEN 1 ELSE 0 END),
    ROUND(
        SUM(CASE WHEN true_negative = true THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 2
    )
FROM crypto_macro_db.bq5_crisis_detection;