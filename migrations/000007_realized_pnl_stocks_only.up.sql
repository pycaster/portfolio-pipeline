-- Make realized_pnl truly stocks-only.
-- Previously it returned STC/BTC rows (option closes) with est_pnl = t.amount,
-- which is wrong because Robinhood stores amount=0 for BTC and STC proceeds
-- are gross (not net of opening cost). Adding asset_type = 'STOCK' to the
-- WHERE clause prevents option rows from appearing at all.
-- Use portfolio.option_contract_pnl for option P&L.
CREATE OR REPLACE VIEW portfolio.realized_pnl AS
SELECT
    t.activity_date,
    t.broker,
    t.symbol,
    t.trans_code,
    t.quantity,
    t.price  AS close_price,
    t.amount AS proceeds,
    round(t.amount - (t.quantity * cb.avg_purchase_price), 2) AS est_pnl
FROM portfolio.transactions AS t FINAL
LEFT JOIN portfolio.stock_cost_basis AS cb
    ON t.symbol = cb.symbol AND t.broker = cb.broker
WHERE t.asset_type = 'STOCK'
  AND t.trans_code IN ('SELL', 'STC', 'STO', 'BTC')
ORDER BY t.activity_date DESC;
