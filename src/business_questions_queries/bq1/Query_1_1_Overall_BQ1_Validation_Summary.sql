-- BQ1 validation metrics for hypothesis testing
SELECT 
    '1. VaR Attribution Error' as metric_name,
    CAST(ROUND(AVG(var_attribution_error_pct), 4) AS VARCHAR) || '%' as result_value,
    '< 2%' as target,
    CASE WHEN AVG(var_attribution_error_pct) < 2.0 THEN 'PASS' ELSE 'FAIL' END as status
FROM crypto_macro_db.bq1_portfolio_risk_attribution

UNION ALL

SELECT 
    '2. Regime Classification Coverage',
    CAST(COUNT(DISTINCT overall_regime) AS VARCHAR) || ' regimes',
    '>= 4 regimes',
    CASE WHEN COUNT(DISTINCT overall_regime) >= 4 THEN 'PASS' ELSE 'FAIL' END
FROM crypto_macro_db.bq1_portfolio_risk_attribution

UNION ALL

SELECT 
    '3. Data Completeness',
    CAST(COUNT(*) AS VARCHAR) || ' records',
    '>= 250 records',
    CASE WHEN COUNT(*) >= 250 THEN 'PASS' ELSE 'FAIL' END
FROM crypto_macro_db.bq1_portfolio_risk_attribution

UNION ALL

SELECT 
    '4. Transition Predictions Available',
    CAST(COUNT(*) AS VARCHAR) || ' forecasts',
    '> 0 forecasts',
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END
FROM crypto_macro_db.bq1_regime_transitions
WHERE next_regime_7d IS NOT NULL;