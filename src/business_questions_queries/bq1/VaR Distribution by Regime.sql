-- Statistical distribution of VaR across regimes
SELECT 
    overall_regime,
    COUNT(*) as n,
    ROUND(AVG(portfolio_var_99_1d), 2) as mean_var,
    ROUND(STDDEV(portfolio_var_99_1d), 2) as std_var,
    ROUND(APPROX_PERCENTILE(portfolio_var_99_1d, 0.25), 2) as q1_var,
    ROUND(APPROX_PERCENTILE(portfolio_var_99_1d, 0.50), 2) as median_var,
    ROUND(APPROX_PERCENTILE(portfolio_var_99_1d, 0.75), 2) as q3_var
FROM crypto_macro_db.bq1_portfolio_risk_attribution
WHERE overall_regime IS NOT NULL
GROUP BY overall_regime
ORDER BY mean_var DESC;