-- stock_cost_basis: like stock_positions but includes fully exited positions.
-- stock_positions filters HAVING shares_held > 0; this view does not, so it
-- can be used to look up cost basis for symbols you've completely sold out of.
CREATE OR REPLACE VIEW portfolio.stock_cost_basis AS
SELECT
    broker,
    symbol,
    (
        sumIf(quantity * price, trans_code IN ('BUY', 'BTO'))
        - sumIf(amount, trans_code IN ('SELL', 'STC', 'STO', 'BTC'))
    ) / nullIf(
        sumIf(quantity, trans_code IN ('BUY', 'BTO'))
        - sumIf(quantity, trans_code IN ('SELL', 'STC', 'STO', 'BTC')),
        0
    ) AS avg_cost_basis
FROM portfolio.transactions FINAL
WHERE asset_type = 'STOCK'
GROUP BY broker, symbol;

-- realized_pnl: one row per closing transaction with an estimated P&L.
--
-- Stock sells (SELL/STC):
--   est_pnl = proceeds − (qty × avg_cost_basis)
--   This uses the all-time cost basis as a proxy. Accurate when the full
--   position is still open; slightly off for partially-sold positions where
--   the cost basis has been updated by prior sells.
--
-- Option closes (STC/BTC):
--   est_pnl = net amount received or paid (positive = cash in, negative = cash out)
--   Does NOT net against the original premium paid to open.
--
-- Option expirations (OEXP):
--   est_pnl = 0 (expired worthless). The real loss is the premium paid at open,
--   which requires matching to the original BTO transaction. Show all OEXP rows
--   to Venkat — they are always losses even when est_pnl = 0.
CREATE OR REPLACE VIEW portfolio.realized_pnl AS
SELECT
    t.activity_date,
    t.broker,
    t.symbol,
    t.asset_type,
    t.trans_code,
    t.instrument,
    t.quantity,
    t.price  AS close_price,
    t.amount AS proceeds,
    multiIf(
        t.asset_type = 'STOCK',
            round(t.amount - (t.quantity * cb.avg_cost_basis), 2),
        t.asset_type = 'OPTION',
            t.amount,
        NULL
    ) AS est_pnl
FROM portfolio.transactions AS t FINAL
LEFT JOIN portfolio.stock_cost_basis AS cb
    ON t.symbol = cb.symbol AND t.broker = cb.broker
WHERE t.trans_code IN ('SELL', 'STC', 'BTC', 'OEXP', 'OASGN')
ORDER BY t.activity_date DESC;
