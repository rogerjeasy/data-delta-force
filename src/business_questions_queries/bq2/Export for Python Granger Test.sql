-- Export data for formal Granger causality test in Python
SELECT 
    date,
    fomc_sentiment_score,
    fomc_sentiment_lag1,
    fomc_sentiment_lag3,
    fomc_sentiment_lag7,
    btc_return_pct,
    btc_return_forward7,
    fomc_regime
FROM crypto_macro_db.gold_sentiment_transmission
WHERE fomc_sentiment_score IS NOT NULL
  AND btc_return_pct IS NOT NULL
ORDER BY date;