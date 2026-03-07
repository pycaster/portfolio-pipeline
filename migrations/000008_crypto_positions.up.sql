CREATE OR REPLACE VIEW portfolio.crypto_positions AS
SELECT
    broker,
    symbol,
    sumIf(quantity, trans_code IN ('BUY', 'BTO'))
        - sumIf(quantity, trans_code IN ('SELL', 'STC')) AS units_held,
    sumIf(quantity * price, trans_code IN ('BUY', 'BTO'))
        / nullIf(sumIf(quantity, trans_code IN ('BUY', 'BTO')), 0) AS avg_cost_basis,
    min(activity_date) AS first_bought,
    max(activity_date) AS last_activity
FROM portfolio.transactions FINAL
WHERE asset_type = 'CRYPTO'
GROUP BY broker, symbol
HAVING units_held > 0.000001;
