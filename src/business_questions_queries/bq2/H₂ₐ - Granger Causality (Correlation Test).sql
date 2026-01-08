-- Test if lagged sentiment predicts Bitcoin returns
SELECT 
    '1-Day Lag' as horizon,
    COUNT(*) as n,
    ROUND(CORR(fomc_sentiment_lag1, btc_return_pct), 4) as correlation,
    CASE 
        WHEN ABS(CORR(fomc_sentiment_lag1, btc_return_pct)) > 0.15 THEN 'Significant'
        ELSE 'Not Significant'
    END as significance_proxy,
    ROUND(POWER(CORR(fomc_sentiment_lag1, btc_return_pct), 2) * 100, 2) as variance_explained_pct
FROM crypto_macro_db.gold_sentiment_transmission
WHERE fomc_sentiment_lag1 IS NOT NULL

UNION ALL

SELECT 
    '3-Day Lag',
    COUNT(*),
    ROUND(CORR(fomc_sentiment_lag3, btc_return_pct), 4),
    CASE 
        WHEN ABS(CORR(fomc_sentiment_lag3, btc_return_pct)) > 0.15 THEN 'Significant'
        ELSE 'Not Significant'
    END,
    ROUND(POWER(CORR(fomc_sentiment_lag3, btc_return_pct), 2) * 100, 2)
FROM crypto_macro_db.gold_sentiment_transmission
WHERE fomc_sentiment_lag3 IS NOT NULL

UNION ALL

SELECT 
    '7-Day Lag',
    COUNT(*),
    ROUND(CORR(fomc_sentiment_lag7, btc_return_pct), 4),
    CASE 
        WHEN ABS(CORR(fomc_sentiment_lag7, btc_return_pct)) > 0.15 THEN 'Significant'
        ELSE 'Not Significant'
    END,
    ROUND(POWER(CORR(fomc_sentiment_lag7, btc_return_pct), 2) * 100, 2)
FROM crypto_macro_db.gold_sentiment_transmission
WHERE fomc_sentiment_lag7 IS NOT NULL;