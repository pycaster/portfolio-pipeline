CREATE DATABASE IF NOT EXISTS portfolio;

-- Immutable event log. One row per trade event from any broker.
-- ReplacingMergeTree(imported_at): re-importing the same CSV is idempotent —
-- rows with the same row_hash are collapsed, keeping the latest imported_at.
CREATE TABLE IF NOT EXISTS portfolio.transactions (
    row_hash      FixedString(32),
    broker        LowCardinality(String),
    activity_date Date,
    process_date  Date,
    settle_date   Date,
    instrument    String,
    symbol        LowCardinality(String),
    description   String,
    trans_code    LowCardinality(String),
    asset_type    LowCardinality(String),
    option_expiry Nullable(Date),
    option_strike Nullable(Float64),
    option_type   LowCardinality(String),
    quantity      Float64,
    price         Float64,
    amount        Float64,
    source_file   String,
    imported_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(imported_at)
ORDER BY row_hash
PARTITION BY toYYYYMM(activity_date);

-- Daily OHLCV per symbol.
-- source='transaction': trade price written as a side effect of ingest (close only).
-- source='yfinance':    full OHLCV from make ingest-prices (supersedes transaction rows).
CREATE TABLE IF NOT EXISTS portfolio.prices (
    symbol      LowCardinality(String),
    date        Date,
    open        Float64,
    high        Float64,
    low         Float64,
    close       Float64,
    volume      UInt64,
    source      LowCardinality(String),
    imported_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (symbol, date);

-- Current equity holdings derived from all transactions.
-- FINAL forces deduplication of the ReplacingMergeTree.
CREATE VIEW IF NOT EXISTS portfolio.stock_positions AS
SELECT
    broker,
    symbol,
    sumIf(quantity, trans_code = 'BUY')
        - sumIf(quantity, trans_code = 'SELL') AS shares_held,
    sumIf(quantity * price, trans_code = 'BUY')
        / nullIf(sumIf(quantity, trans_code = 'BUY'), 0) AS avg_cost_basis,
    min(activity_date) AS first_bought,
    max(activity_date) AS last_activity
FROM portfolio.transactions FINAL
WHERE asset_type = 'STOCK'
GROUP BY broker, symbol
HAVING shares_held > 0.0001;

-- Open option contracts derived from all transactions.
-- BTO/STO open contracts; BTC/STC/OEXP/OASGN close them.
CREATE VIEW IF NOT EXISTS portfolio.option_positions AS
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
