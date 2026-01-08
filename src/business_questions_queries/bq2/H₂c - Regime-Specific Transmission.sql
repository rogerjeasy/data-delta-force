-- Sentiment transmission coefficients by regime
SELECT 
    fomc_regime,
    COUNT(*) as observations,
    ROUND(AVG(sentiment_to_btc_coef_30d), 4) as avg_transmission_coef,
    ROUND(STDDEV(sentiment_to_btc_coef_30d), 4) as std_transmission_coef,
    ROUND(AVG(sentiment_btc_corr_30d), 4) as avg_correlation_30d,
    ROUND(AVG(regime_volatility_impact), 4) as avg_volatility_impact,
    transmission_strength
FROM crypto_macro_db.gold_sentiment_transmission
WHERE fomc_regime IS NOT NULL
  AND sentiment_to_btc_coef_30d IS NOT NULL
GROUP BY fomc_regime, transmission_strength
ORDER BY avg_transmission_coef DESC;