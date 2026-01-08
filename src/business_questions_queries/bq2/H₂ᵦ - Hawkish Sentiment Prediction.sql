-- Test if hawkish sentiment predicts negative returns
SELECT 
    fomc_regime as sentiment_type,
    COUNT(*) as observations,
    ROUND(AVG(btc_return_forward1), 3) as avg_return_1d,
    ROUND(AVG(btc_return_forward3), 3) as avg_return_3d,
    ROUND(AVG(btc_return_forward7), 3) as avg_return_7d,
    ROUND(STDDEV(btc_return_forward7), 3) as std_return_7d,
    ROUND(SUM(CASE WHEN btc_return_forward7 < 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) 
        as pct_negative_returns_7d,
    CASE 
        WHEN fomc_regime = 'hawkish' AND AVG(btc_return_forward7) < 0 THEN 'H2b: VALIDATED'
        WHEN fomc_regime = 'hawkish' AND AVG(btc_return_forward7) >= 0 THEN 'H2b: NOT VALIDATED'
        ELSE 'N/A'
    END as hypothesis_status
FROM crypto_macro_db.gold_sentiment_transmission
WHERE fomc_regime IN ('hawkish', 'dovish', 'neutral')
GROUP BY fomc_regime
ORDER BY avg_return_7d;