-- BQ2 All Hypotheses Validation
SELECT 
    'H2a: Granger Causality (7-day)' as hypothesis,
    CAST(ROUND(CORR(fomc_sentiment_lag7, btc_return_pct), 4) AS VARCHAR) as result_value,
    '|r| > 0.15 or R² > 10%' as target,
    CASE 
        WHEN ABS(CORR(fomc_sentiment_lag7, btc_return_pct)) > 0.15 
             OR POWER(CORR(fomc_sentiment_lag7, btc_return_pct), 2) > 0.10 
        THEN 'VALIDATED'
        ELSE 'NOT VALIDATED'
    END as status
FROM crypto_macro_db.gold_sentiment_transmission
WHERE fomc_sentiment_lag7 IS NOT NULL

UNION ALL

SELECT 
    'H2b: Hawkish → Negative Returns',
    CAST(ROUND(AVG(btc_return_forward7), 3) AS VARCHAR),
    '< 0 (negative)',
    CASE 
        WHEN AVG(btc_return_forward7) < 0 THEN 'VALIDATED'
        ELSE 'NOT VALIDATED'
    END
FROM crypto_macro_db.gold_sentiment_transmission
WHERE fomc_regime = 'hawkish'

UNION ALL

SELECT 
    'H2c: Regime Differences (ANOVA proxy)',
    CAST(COUNT(DISTINCT fomc_regime) AS VARCHAR) || ' regimes analyzed',
    'Significant variance across regimes',
    CASE 
        WHEN STDDEV(sentiment_to_btc_coef_30d) > AVG(sentiment_to_btc_coef_30d) * 0.3 
        THEN 'VALIDATED'
        ELSE 'NOT VALIDATED'
    END
FROM crypto_macro_db.gold_sentiment_transmission
WHERE sentiment_to_btc_coef_30d IS NOT NULL

UNION ALL

SELECT 
    'Volatility Spike Prediction',
    CAST(ROUND(
        SUM(CASE WHEN volatility_spike_flag = true THEN 1 ELSE 0 END) * 100.0 
        / COUNT(*), 1) AS VARCHAR) || '%',
    '> 70%',
    CASE 
        WHEN SUM(CASE WHEN volatility_spike_flag = true THEN 1 ELSE 0 END) * 100.0 
             / COUNT(*) > 70 
        THEN 'PASS'
        ELSE 'FAIL'
    END
FROM crypto_macro_db.gold_sentiment_transmission
WHERE sentiment_reversal_flag = true;