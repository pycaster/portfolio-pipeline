-- Robinhood uses BTO (Buy to Open) and STC (Sell to Close) for stock trades
-- in addition to the standard BUY/SELL codes.
-- STO (Sell to Open) and BTC (Buy to Close) cover short/covered positions.

CREATE OR REPLACE VIEW portfolio.stock_positions AS
SELECT
    broker,
    symbol,
    sumIf(quantity, trans_code IN ('BUY', 'BTO'))
        - sumIf(quantity, trans_code IN ('SELL', 'STC', 'STO', 'BTC')) AS shares_held,
    sumIf(quantity * price, trans_code IN ('BUY', 'BTO'))
        / nullIf(sumIf(quantity, trans_code IN ('BUY', 'BTO')), 0) AS avg_cost_basis,
    min(activity_date) AS first_bought,
    max(activity_date) AS last_activity
FROM portfolio.transactions FINAL
WHERE asset_type = 'STOCK'
GROUP BY broker, symbol
HAVING shares_held > 0.0001;

CREATE OR REPLACE VIEW portfolio.option_positions AS
SELECT
    broker,
    symbol,
    instrument,
    option_expiry,
    option_strike,
    option_type,
    sumIf(quantity, trans_code IN ('BTO', 'STO'))
        - sumIf(quantity, trans_code IN ('BTC', 'STC', 'OEXP', 'OASGN')) AS contracts_held,
    min(activity_date) AS opened_date
FROM portfolio.transactions FINAL
WHERE asset_type = 'OPTION'
GROUP BY broker, symbol, instrument, option_expiry, option_strike, option_type
HAVING contracts_held > 0.0001;
