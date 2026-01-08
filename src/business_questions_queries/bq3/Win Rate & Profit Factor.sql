-- Trading statistics
SELECT 
    COUNT(*) as total_trades,
    SUM(CASE WHEN alpha_1d > 0 THEN 1 ELSE 0 END) as winning_trades,
    SUM(CASE WHEN alpha_1d < 0 THEN 1 ELSE 0 END) as losing_trades,
    ROUND(SUM(CASE WHEN alpha_1d > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as win_rate_pct,
    ROUND(SUM(CASE WHEN alpha_1d > 0 THEN alpha_1d ELSE 0 END), 4) as total_gains,
    ROUND(ABS(SUM(CASE WHEN alpha_1d < 0 THEN alpha_1d ELSE 0 END)), 4) as total_losses,
    ROUND(
        SUM(CASE WHEN alpha_1d > 0 THEN alpha_1d ELSE 0 END) / 
        NULLIF(ABS(SUM(CASE WHEN alpha_1d < 0 THEN alpha_1d ELSE 0 END)), 0), 
        4
    ) as profit_factor,
    ROUND(AVG(CASE WHEN alpha_1d > 0 THEN alpha_1d END), 4) as avg_win_size,
    ROUND(AVG(CASE WHEN alpha_1d < 0 THEN alpha_1d END), 4) as avg_loss_size
FROM crypto_macro_db.bq3_tactical_signals
WHERE alpha_1d != 0;