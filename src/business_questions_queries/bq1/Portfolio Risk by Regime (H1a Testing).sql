-- Test H1a: BTC VaR increases >50% during tightening vs easing
SELECT 
    overall_regime,
    risk_environment,
    COUNT(*) as observation_days,
    ROUND(AVG(portfolio_var_99_1d), 2) as avg_portfolio_var,
    ROUND(AVG(btc_var_99_1d), 2) as avg_btc_var,
    ROUND(AVG(crypto_var_contribution), 2) as avg_crypto_var_contribution,
    ROUND(AVG(crypto_pct_of_total_risk), 2) as avg_crypto_risk_pct,
    ROUND(STDDEV(portfolio_var_99_1d), 2) as std_portfolio_var,
    ROUND(MIN(portfolio_var_99_1d), 2) as min_var,
    ROUND(MAX(portfolio_var_99_1d), 2) as max_var
FROM crypto_macro_db.bq1_portfolio_risk_attribution
WHERE overall_regime IS NOT NULL
GROUP BY overall_regime, risk_environment
ORDER BY avg_portfolio_var DESC;