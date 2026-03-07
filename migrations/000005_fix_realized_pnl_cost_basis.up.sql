-- Fix realized_pnl to work for fully exited positions.
--
-- The 000004 stock_cost_basis used the current-position cost basis formula:
--   (buy_cost - sell_proceeds) / shares_held
-- This returns NULL when shares_held = 0 (fully exited), producing NULL est_pnl
-- for every closed-out stock position.
--
-- For realized P&L we want the weighted average PURCHASE price across all buys,
-- independent of how many shares remain. This is stable: it doesn't go to zero
-- when you sell out, so P&L is computable for fully exited positions.
--
-- Note: stock_positions.avg_cost_basis is a different (correct) metric for
-- "what is the cost basis of my CURRENT holdings." These two views serve
-- different purposes.

CREATE OR REPLACE VIEW portfolio.stock_cost_basis AS
SELECT
    broker,
    symbol,
    sumIf(quantity * price, trans_code IN ('BUY', 'BTO'))
    / nullIf(sumIf(quantity, trans_code IN ('BUY', 'BTO')), 0) AS avg_purchase_price
FROM portfolio.transactions FINAL
WHERE asset_type = 'STOCK'
GROUP BY broker, symbol;

-- Recreate realized_pnl referencing the fixed column name.
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
            round(t.amount - (t.quantity * cb.avg_purchase_price), 2),
        t.asset_type = 'OPTION',
            t.amount,
        NULL
    ) AS est_pnl
FROM portfolio.transactions AS t FINAL
LEFT JOIN portfolio.stock_cost_basis AS cb
    ON t.symbol = cb.symbol AND t.broker = cb.broker
WHERE t.trans_code IN ('SELL', 'STC', 'BTC', 'OEXP', 'OASGN')
ORDER BY t.activity_date DESC;
