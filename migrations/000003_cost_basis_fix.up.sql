-- Fix avg_cost_basis to reflect cost of CURRENT holdings, not all-time buys.
--
-- Old formula: sum(qty*price for buys) / sum(qty for buys)
--   Problem:   divides by total shares ever bought, including shares already sold,
--              which underweights recent higher-priced purchases.
--
-- New formula: (total paid on buys - total proceeds from sells) / shares_held
--   This is the net capital deployed in the current position — the true
--   economic cost basis of the shares still held.
--
-- Note: Robinhood's Amount column is 0 for buys, so we derive buy cost from
--       quantity * price. Sell proceeds use the Amount column (already positive).

CREATE OR REPLACE VIEW portfolio.stock_positions AS
SELECT
    broker,
    symbol,
    sumIf(quantity, trans_code IN ('BUY', 'BTO'))
        - sumIf(quantity, trans_code IN ('SELL', 'STC', 'STO', 'BTC')) AS shares_held,
    (
        sumIf(quantity * price, trans_code IN ('BUY', 'BTO'))
        - sumIf(amount, trans_code IN ('SELL', 'STC', 'STO', 'BTC'))
    ) / nullIf(
        sumIf(quantity, trans_code IN ('BUY', 'BTO'))
        - sumIf(quantity, trans_code IN ('SELL', 'STC', 'STO', 'BTC')),
        0
    ) AS avg_cost_basis,
    min(activity_date) AS first_bought,
    max(activity_date) AS last_activity
FROM portfolio.transactions FINAL
WHERE asset_type = 'STOCK'
GROUP BY broker, symbol
HAVING shares_held > 0.0001;
