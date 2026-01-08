-- Detailed crypto risk contribution by regime
SELECT 
    overall_regime,
    COUNT(*) as days,
    ROUND(AVG(weight_btc) * 100, 1) as btc_weight_pct,
    ROUND(AVG(weight_eth) * 100, 1) as eth_weight_pct,
    ROUND(AVG(portfolio_var_no_crypto), 2) as avg_var_without_crypto,
    ROUND(AVG(portfolio_var_99_1d), 2) as avg_var_with_crypto,
    ROUND(AVG(risk_increase_from_crypto), 2) as avg_risk_increase,
    ROUND((AVG(portfolio_var_99_1d) - AVG(portfolio_var_no_crypto)) / AVG(portfolio_var_no_crypto) * 100, 1) as risk_increase_pct
FROM crypto_macro_db.bq1_portfolio_risk_attribution
WHERE overall_regime IS NOT NULL
GROUP BY overall_regime
ORDER BY risk_increase_pct DESC;