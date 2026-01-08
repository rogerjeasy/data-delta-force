-- Comprehensive statistics for BQ2 report
SELECT 
    COUNT(*) as total_observations,
    COUNT(DISTINCT fomc_regime) as unique_sentiment_regimes,
    ROUND(AVG(fomc_sentiment_score), 4) as avg_sentiment,
    ROUND(STDDEV(fomc_sentiment_score), 4) as std_sentiment,
    ROUND(AVG(btc_return_pct), 4) as avg_btc_return,
    ROUND(STDDEV(btc_return_pct), 4) as std_btc_return,
    ROUND(AVG(sentiment_to_btc_coef_30d), 4) as avg_transmission_coef,
    ROUND(AVG(sentiment_btc_corr_30d), 4) as avg_30d_correlation,
    ROUND(AVG(sentiment_btc_corr_60d), 4) as avg_60d_correlation,
    SUM(CASE WHEN volatility_spike_flag = true THEN 1 ELSE 0 END) as total_vol_spikes,
    SUM(CASE WHEN sentiment_reversal_flag = true THEN 1 ELSE 0 END) as total_sentiment_reversals
FROM crypto_macro_db.gold_sentiment_transmission;