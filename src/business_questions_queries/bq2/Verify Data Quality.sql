-- Check data availability
SELECT 
    COUNT(*) as total_records,
    COUNT(fomc_sentiment_score) as sentiment_records,
    COUNT(btc_return_forward7) as forward_return_records,
    MIN(date) as start_date,
    MAX(date) as end_date,
    COUNT(DISTINCT fomc_regime) as unique_regimes
FROM crypto_macro_db.gold_sentiment_transmission;