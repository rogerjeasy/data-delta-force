-- Risk-adjusted performance by regime
SELECT 
    overall_regime,
    COUNT(*) as observation_days,
    ROUND(AVG(btc_sharpe_30d), 4) as avg_btc_sharpe,
    ROUND(AVG(eth_sharpe_30d), 4) as avg_eth_sharpe,
    ROUND(AVG(portfolio_sharpe_30d), 4) as avg_portfolio_sharpe,
    ROUND(AVG(btc_current_drawdown), 2) as avg_btc_drawdown_pct,
    ROUND(MIN(btc_current_drawdown), 2) as max_btc_drawdown_pct
FROM crypto_macro_db.bq1_portfolio_risk_attribution
WHERE overall_regime IS NOT NULL
GROUP BY overall_regime
ORDER BY avg_portfolio_sharpe DESC;