-- Complete hypothesis validation
SELECT 
    'H5a: Correlation Breakdown Signals Crisis' as hypothesis,
    CAST(ROUND(
        SUM(CASE WHEN correlation_breakdown_flag = true AND crisis_event_next_5d = true THEN 1 ELSE 0 END) * 100.0 
        / NULLIF(SUM(CASE WHEN correlation_breakdown_flag = true THEN 1 ELSE 0 END), 0), 2
    ) AS VARCHAR) || '%' as result_value,
    '> 50% crisis propagation' as target,
    CASE 
        WHEN SUM(CASE WHEN correlation_breakdown_flag = true AND crisis_event_next_5d = true THEN 1 ELSE 0 END) * 100.0 
             / NULLIF(SUM(CASE WHEN correlation_breakdown_flag = true THEN 1 ELSE 0 END), 0) > 50
        THEN 'VALIDATED'
        ELSE 'NOT VALIDATED'
    END as status
FROM crypto_macro_db.bq5_crisis_detection

UNION ALL

SELECT 
    'H5b: Volatility Shifts Precede Breakdowns',
    CAST(ROUND(
        SUM(CASE WHEN volatility_regime_shift = true AND breakdown_occurs_72h = true THEN 1 ELSE 0 END) * 100.0 
        / NULLIF(SUM(CASE WHEN volatility_regime_shift = true THEN 1 ELSE 0 END), 0), 2
    ) AS VARCHAR) || '%',
    '> 50% within 2-5 days',
    CASE 
        WHEN SUM(CASE WHEN volatility_regime_shift = true AND breakdown_occurs_72h = true THEN 1 ELSE 0 END) * 100.0 
             / NULLIF(SUM(CASE WHEN volatility_regime_shift = true THEN 1 ELSE 0 END), 0) > 50
        THEN 'VALIDATED'
        ELSE 'NOT VALIDATED'
    END
FROM crypto_macro_db.bq5_crisis_detection

UNION ALL

SELECT 
    'H5c: Early Warning System Performance',
    CONCAT(
        CAST(ROUND(SUM(CASE WHEN true_positive = true THEN 1 ELSE 0 END) * 100.0 
            / NULLIF(SUM(CASE WHEN true_positive = true OR false_negative = true THEN 1 ELSE 0 END), 0), 2) AS VARCHAR), '% DR, ',
        CAST(ROUND(SUM(CASE WHEN false_positive = true THEN 1 ELSE 0 END) * 100.0 
            / NULLIF(SUM(CASE WHEN false_positive = true OR true_negative = true THEN 1 ELSE 0 END), 0), 2) AS VARCHAR), '% FPR'
    ),
    'DR >75%, FPR <20%',
    CASE 
        WHEN SUM(CASE WHEN true_positive = true THEN 1 ELSE 0 END) * 100.0 
             / NULLIF(SUM(CASE WHEN true_positive = true OR false_negative = true THEN 1 ELSE 0 END), 0) > 75
             AND SUM(CASE WHEN false_positive = true THEN 1 ELSE 0 END) * 100.0 
             / NULLIF(SUM(CASE WHEN false_positive = true OR true_negative = true THEN 1 ELSE 0 END), 0) < 20
        THEN 'VALIDATED'
        ELSE 'NOT VALIDATED'
    END
FROM crypto_macro_db.bq5_crisis_detection;