-- Data coverage summary
SELECT 
    MIN(date) as start_date,
    MAX(date) as end_date,
    DATE_DIFF('day', MIN(date), MAX(date)) as total_days_covered,
    COUNT(*) as records_available,
    COUNT(DISTINCT overall_regime) as unique_regimes
FROM crypto_macro_db.bq1_portfolio_risk_attribution;