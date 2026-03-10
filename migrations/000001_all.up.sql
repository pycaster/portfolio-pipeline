-- Consolidated schema: single source of truth for all portfolio + signals tables/views.
-- Replaces migrations 000001–000017.
-- Run via: make migrate  (after full_reset.sh clears the tracker + data)

-- ─────────────────────────────────────────────
-- DATABASES
-- ─────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS portfolio;
CREATE DATABASE IF NOT EXISTS signals;

-- ─────────────────────────────────────────────
-- portfolio.transactions
-- ─────────────────────────────────────────────
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

-- ─────────────────────────────────────────────
-- portfolio.prices
-- ─────────────────────────────────────────────
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

-- ─────────────────────────────────────────────
-- portfolio.insights
-- ─────────────────────────────────────────────
-- LLM-generated trading insights, one row per generation run.
-- ReplacingMergeTree on (period, model): re-running gen_insights.py for the same
-- period+model replaces the previous insight.
CREATE TABLE IF NOT EXISTS portfolio.insights (
    generated_at DateTime DEFAULT now(),
    period       LowCardinality(String),   -- e.g. '2026-02', 'all-time'
    model        LowCardinality(String),   -- e.g. 'claude-opus-4-6'
    insight_text String,
    stats_json   String
) ENGINE = ReplacingMergeTree(generated_at)
ORDER BY (period, model)
PARTITION BY toYYYYMM(generated_at);

-- ─────────────────────────────────────────────
-- portfolio.stock_positions  (lot-reset cost basis)
-- ─────────────────────────────────────────────
-- Shows currently-held equity positions.
-- avg_cost_basis reflects the CURRENT LOT only: buys since the last date the
-- position was fully flat (cum_qty <= 0).  Positions never fully closed use all
-- historical buys, matching the naive approach.
CREATE OR REPLACE VIEW portfolio.stock_positions AS
WITH txn AS (
    SELECT broker, symbol, activity_date, trans_code, quantity, price
    FROM portfolio.transactions FINAL
    WHERE asset_type = 'STOCK'
      AND option_expiry IS NULL
      AND trans_code IN ('BUY', 'SELL', 'BTO', 'STC', 'STO', 'BTC')
),
running AS (
    SELECT broker, symbol, activity_date,
        sum(if(trans_code IN ('BUY', 'BTO'), quantity, -quantity))
            OVER (PARTITION BY broker, symbol ORDER BY activity_date
                  ROWS UNBOUNDED PRECEDING) AS cum_qty
    FROM txn
),
lot_reset AS (
    -- Last date the position was fully flat (cum_qty <= 0)
    SELECT broker, symbol, max(activity_date) AS reset_date
    FROM running
    WHERE cum_qty <= 0
    GROUP BY broker, symbol
)
SELECT
    t.broker,
    t.symbol,
    sumIf(t.quantity, t.trans_code IN ('BUY', 'BTO'))
        - sumIf(t.quantity, t.trans_code IN ('SELL', 'STC', 'STO', 'BTC')) AS shares_held,
    -- avg cost of current lot only (buys after last full exit, or all buys if never flat)
    sumIf(t.quantity * t.price,
          t.trans_code IN ('BUY', 'BTO')
          AND (lr.reset_date IS NULL OR t.activity_date > lr.reset_date))
    / nullIf(
        sumIf(t.quantity,
              t.trans_code IN ('BUY', 'BTO')
              AND (lr.reset_date IS NULL OR t.activity_date > lr.reset_date)),
        0) AS avg_cost_basis,
    min(t.activity_date) AS first_bought,
    max(t.activity_date) AS last_activity
FROM txn t
LEFT JOIN lot_reset lr ON t.broker = lr.broker AND t.symbol = lr.symbol
GROUP BY t.broker, t.symbol, lr.reset_date
HAVING shares_held > 0.0001;

-- ─────────────────────────────────────────────
-- portfolio.option_positions
-- ─────────────────────────────────────────────
-- Open option contracts.
-- instrument is computed deterministically from structured fields — never from
-- the raw CSV Instrument column (which can be inconsistent across exports).
CREATE OR REPLACE VIEW portfolio.option_positions AS
SELECT
    broker,
    symbol,
    concat(
        symbol, ' ',
        formatDateTime(option_expiry, '%m/%d/%Y'), ' ',
        toString(option_strike), option_type
    ) AS instrument,
    option_expiry,
    option_strike,
    option_type,
    sumIf(quantity, trans_code IN ('BTO', 'STO'))
        - sumIf(quantity, trans_code IN ('BTC', 'STC', 'OEXP', 'OASGN')) AS contracts_held,
    min(activity_date) AS opened_date
FROM portfolio.transactions FINAL
WHERE asset_type = 'OPTION'
GROUP BY broker, symbol, option_expiry, option_strike, option_type
HAVING contracts_held > 0.0001;

-- ─────────────────────────────────────────────
-- portfolio.crypto_positions
-- ─────────────────────────────────────────────
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

-- ─────────────────────────────────────────────
-- portfolio.stock_cost_basis  (smart lot cost — feeds realized_pnl)
-- ─────────────────────────────────────────────
-- Returns avg_purchase_price for the most relevant lot per (broker, symbol):
--   1. Never fully closed → lifetime weighted avg of all buys
--   2. Currently open (buys exist after last exit) → current lot cost only
--   3. Fully closed, 2+ resets → most recently completed lot
--      (buys between prev_reset and last_reset)
--   4. Fully closed, 1 reset → all buys up to the exit
-- This ensures round-trip trades (e.g. buy@$467 → flat → buy@$405 → sell@$406)
-- show correct P&L on the re-entered lot instead of a false -$13k loss.
CREATE OR REPLACE VIEW portfolio.stock_cost_basis AS
WITH txn AS (
    SELECT broker, symbol, activity_date, trans_code, quantity, price
    FROM portfolio.transactions FINAL
    WHERE asset_type = 'STOCK'
      AND option_expiry IS NULL
      AND trans_code IN ('BUY', 'SELL', 'BTO', 'STC', 'STO', 'BTC')
),
running AS (
    SELECT broker, symbol, activity_date,
        sum(if(trans_code IN ('BUY', 'BTO'), quantity, -quantity))
            OVER (PARTITION BY broker, symbol ORDER BY activity_date
                  ROWS UNBOUNDED PRECEDING) AS cum_qty
    FROM txn
),
all_resets AS (
    SELECT DISTINCT broker, symbol, activity_date AS reset_date
    FROM running WHERE cum_qty <= 0
),
numbered_resets AS (
    SELECT broker, symbol, reset_date,
        row_number() OVER (PARTITION BY broker, symbol ORDER BY reset_date DESC) AS rn
    FROM all_resets
),
lot_bounds AS (
    -- last_reset: most recent date position was flat
    -- prev_reset: second-most-recent flat date (start of the last closed lot)
    SELECT broker, symbol,
        maxIf(reset_date, rn = 1) AS last_reset,
        maxIf(reset_date, rn = 2) AS prev_reset
    FROM numbered_resets
    GROUP BY broker, symbol
)
SELECT
    t.broker,
    t.symbol,
    multiIf(
        -- Case 1: never fully closed — use lifetime avg
        lb.last_reset IS NULL,
        sumIf(t.quantity * t.price, t.trans_code IN ('BUY', 'BTO'))
            / nullIf(sumIf(t.quantity, t.trans_code IN ('BUY', 'BTO')), 0),

        -- Case 2: position currently open (buys exist after last exit) — current lot
        sumIf(t.quantity, t.trans_code IN ('BUY', 'BTO')
              AND t.activity_date > lb.last_reset) > 0,
        sumIf(t.quantity * t.price, t.trans_code IN ('BUY', 'BTO')
              AND t.activity_date > lb.last_reset)
            / sumIf(t.quantity, t.trans_code IN ('BUY', 'BTO')
              AND t.activity_date > lb.last_reset),

        -- Case 3: fully closed with 2+ resets — most recently completed lot
        lb.prev_reset IS NOT NULL,
        sumIf(t.quantity * t.price, t.trans_code IN ('BUY', 'BTO')
              AND t.activity_date > lb.prev_reset AND t.activity_date <= lb.last_reset)
            / nullIf(sumIf(t.quantity, t.trans_code IN ('BUY', 'BTO')
              AND t.activity_date > lb.prev_reset AND t.activity_date <= lb.last_reset), 0),

        -- Case 4: fully closed with 1 reset — all buys up to the exit
        sumIf(t.quantity * t.price, t.trans_code IN ('BUY', 'BTO')
              AND t.activity_date <= lb.last_reset)
            / nullIf(sumIf(t.quantity, t.trans_code IN ('BUY', 'BTO')
              AND t.activity_date <= lb.last_reset), 0)
    ) AS avg_purchase_price
FROM txn t
LEFT JOIN lot_bounds lb ON t.broker = lb.broker AND t.symbol = lb.symbol
GROUP BY t.broker, t.symbol, lb.last_reset, lb.prev_reset;

-- ─────────────────────────────────────────────
-- portfolio.realized_pnl
-- ─────────────────────────────────────────────
-- Stocks-only realized P&L with per-sell lot cost basis.
-- For each SELL, the cost basis is the weighted average of buys from the same
-- lot only: buys that occurred after the last time the position was flat
-- (cum_qty <= 0) and before or on this sell date.
-- This eliminates false P&L from round-trip trades at different price levels.
-- Use portfolio.option_contract_pnl for option P&L.
CREATE OR REPLACE VIEW portfolio.realized_pnl AS
WITH txn AS (
    SELECT row_hash, broker, symbol, activity_date, trans_code, quantity, price, amount
    FROM portfolio.transactions FINAL
    WHERE asset_type = 'STOCK'
      AND option_expiry IS NULL
      AND trans_code IN ('BUY', 'SELL', 'BTO', 'STC', 'STO', 'BTC')
),
running AS (
    SELECT *,
        sum(if(trans_code IN ('BUY', 'BTO'), quantity, -quantity))
            OVER (PARTITION BY broker, symbol ORDER BY activity_date
                  ROWS UNBOUNDED PRECEDING) AS cum_qty
    FROM txn
),
all_resets AS (
    SELECT DISTINCT broker, symbol, activity_date AS reset_date
    FROM running WHERE cum_qty <= 0
),
reset_arrays AS (
    SELECT broker, symbol, arraySort(groupArray(reset_date)) AS reset_dates
    FROM all_resets GROUP BY broker, symbol
),
sells AS (
    -- Each sell enriched with the start date of its lot
    -- lot_start = last date position was flat strictly before this sell date
    -- If no prior flat date: lot_start = 1970-01-01 (include all buys up to sell)
    SELECT s.row_hash, s.broker, s.symbol, s.activity_date, s.trans_code,
           s.quantity, s.price, s.amount,
        if(arrayLastIndex(x -> x < s.activity_date, coalesce(ra.reset_dates, [])) > 0,
           arrayElement(coalesce(ra.reset_dates, []), arrayLastIndex(x -> x < s.activity_date, coalesce(ra.reset_dates, []))),
           toDate('1970-01-01')) AS lot_start
    FROM running s
    LEFT JOIN reset_arrays ra ON s.broker = ra.broker AND s.symbol = ra.symbol
    WHERE s.trans_code IN ('SELL', 'STC', 'STO', 'BTC')
)
SELECT
    s.activity_date,
    s.broker,
    s.symbol,
    s.trans_code,
    s.quantity,
    s.price AS close_price,
    s.amount AS proceeds,
    round(
        s.amount - s.quantity *
        sumIf(b.quantity * b.price,
              b.trans_code IN ('BUY', 'BTO')
              AND b.activity_date > s.lot_start
              AND b.activity_date <= s.activity_date)
        / nullIf(
            sumIf(b.quantity,
                  b.trans_code IN ('BUY', 'BTO')
                  AND b.activity_date > s.lot_start
                  AND b.activity_date <= s.activity_date),
            0),
    2) AS est_pnl
FROM sells s
LEFT JOIN txn b ON s.broker = b.broker AND s.symbol = b.symbol
GROUP BY s.row_hash, s.broker, s.symbol, s.activity_date, s.trans_code,
         s.quantity, s.price, s.amount, s.lot_start
ORDER BY s.activity_date DESC;

-- ─────────────────────────────────────────────
-- portfolio.option_contract_pnl
-- ─────────────────────────────────────────────
-- Per-contract net P&L.
-- cash_received = sum(amount for STO + STC)        -- stored correctly in CH
-- cash_spent    = sum(qty × price × 100 for BTO + BTC) -- reconstructed
-- net_pnl       = cash_received − cash_spent
-- is_closed = true when total qty opened == total qty closed.
CREATE OR REPLACE VIEW portfolio.option_contract_pnl AS
SELECT
    broker,
    symbol,
    option_expiry,
    option_strike,
    option_type,
    sumIf(quantity, trans_code IN ('BTO', 'STO'))               AS qty_opened,
    sumIf(quantity, trans_code IN ('STC', 'BTC', 'OEXP', 'OASGN')) AS qty_closed,
    round(sumIf(amount,                 trans_code IN ('STO', 'STC')), 2) AS cash_received,
    round(sumIf(quantity * price * 100, trans_code IN ('BTO', 'BTC')), 2) AS cash_spent,
    round(
        sumIf(amount,                 trans_code IN ('STO', 'STC'))
      - sumIf(quantity * price * 100, trans_code IN ('BTO', 'BTC')),
    2) AS net_pnl,
    sumIf(quantity, trans_code IN ('BTO', 'STO'))
        = sumIf(quantity, trans_code IN ('STC', 'BTC', 'OEXP', 'OASGN')) AS is_closed,
    min(activity_date) AS opened_date,
    max(activity_date) AS closed_date
FROM portfolio.transactions FINAL
WHERE asset_type = 'OPTION'
GROUP BY broker, symbol, option_expiry, option_strike, option_type;

-- ─────────────────────────────────────────────
-- signals.indicators  (daily)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS signals.indicators (
    symbol          LowCardinality(String),
    date            Date,
    close           Float64,

    -- RSI(14)
    rsi_14          Nullable(Float64),
    rsi_zone        LowCardinality(String),   -- overbought | oversold | neutral

    -- Ichimoku Kinko Hyo (9, 26, 52)
    tenkan          Nullable(Float64),
    kijun           Nullable(Float64),
    senkou_a        Nullable(Float64),
    senkou_b        Nullable(Float64),
    cloud_color     LowCardinality(String),   -- green | red
    price_vs_cloud  LowCardinality(String),   -- above | inside | below
    tk_cross        LowCardinality(String),   -- bullish_cross | bearish_cross | bullish | bearish | neutral

    -- RSI divergence
    rsi_divergence  LowCardinality(String) DEFAULT '',

    -- Volume
    vol_ratio       Float32 DEFAULT 0,
    obv_trend       LowCardinality(String) DEFAULT '',
    vol_signal      LowCardinality(String) DEFAULT '',

    -- Composite signal
    signal          LowCardinality(String),   -- bullish | bearish | mixed
    signal_score    Int8,

    computed_at     DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (symbol, date)
PARTITION BY toYYYYMM(date);

-- ─────────────────────────────────────────────
-- signals.strategy  (daily decisions)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS signals.strategy (
    symbol        String,
    date          Date,
    decision      LowCardinality(String),   -- BUY | WATCH | HOLD | EXIT
    score         Int8,
    reasons       Array(String),
    prev_decision LowCardinality(String) DEFAULT ''
) ENGINE = ReplacingMergeTree()
ORDER BY (symbol, date);

-- ─────────────────────────────────────────────
-- signals.indicators_1h  (intraday)
-- ─────────────────────────────────────────────
-- Mirrors signals.indicators but uses DateTime for 1h bar timestamps (UTC).
CREATE TABLE IF NOT EXISTS signals.indicators_1h (
    symbol          LowCardinality(String),
    datetime        DateTime,
    close           Float64,

    rsi_14          Nullable(Float64),
    rsi_zone        LowCardinality(String),
    tenkan          Nullable(Float64),
    kijun           Nullable(Float64),
    senkou_a        Nullable(Float64),
    senkou_b        Nullable(Float64),
    cloud_color     LowCardinality(String),
    price_vs_cloud  LowCardinality(String),
    tk_cross        LowCardinality(String),
    rsi_divergence  LowCardinality(String) DEFAULT '',
    signal          LowCardinality(String),
    signal_score    Int8,
    vol_ratio       Float32 DEFAULT 0,
    obv_trend       LowCardinality(String) DEFAULT '',
    vol_signal      LowCardinality(String) DEFAULT '',

    computed_at     DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (symbol, datetime)
PARTITION BY toYYYYMM(datetime);

-- ─────────────────────────────────────────────
-- signals.strategy_1h
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS signals.strategy_1h (
    symbol        LowCardinality(String),
    datetime      DateTime,
    decision      LowCardinality(String),
    score         Int8,
    reasons       Array(String),
    prev_decision LowCardinality(String) DEFAULT ''
) ENGINE = ReplacingMergeTree()
ORDER BY (symbol, datetime);

-- ─────────────────────────────────────────────
-- signals.newsfeed_articles  (sector-based)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS signals.newsfeed_articles (
    article_id       String,
    source           LowCardinality(String),
    url              String,
    title            String,
    full_text        String,
    published_at     DateTime,
    fetched_at       DateTime DEFAULT now(),
    sectors          Array(LowCardinality(String)),
    sector_match     UInt8 DEFAULT 0,
    sentiment        LowCardinality(String),
    sentiment_score  Float32,
    sentiment_signed Float32,
    tickers          Array(String)
) ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (source, article_id)
PARTITION BY toYYYYMM(published_at);

-- ─────────────────────────────────────────────
-- signals.newsfeed_outcomes
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS signals.newsfeed_outcomes (
    article_id       String,
    ticker           String,
    sector           String,
    sentiment_signed Float32,
    published_at     DateTime,
    price_date       Date,
    price_change_1d  Nullable(Float32),
    price_change_2d  Nullable(Float32),
    outcome_at       DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(outcome_at)
ORDER BY (article_id, ticker, sector);

-- ─────────────────────────────────────────────
-- signals.newsfeed_ic
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS signals.newsfeed_ic (
    source        LowCardinality(String),
    sector        String,
    ticker        String,
    ic_1d         Float32,
    ic_2d         Float32,
    sample_count  UInt32,
    computed_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (source, sector, ticker);

-- ─────────────────────────────────────────────
-- signals.outcomes  (backtesting forward returns)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS signals.outcomes (
    symbol          String,
    signal_date     Date,
    decision        LowCardinality(String),
    score           Int8,
    rsi_divergence  LowCardinality(String),
    rsi_zone        LowCardinality(String),
    price_vs_cloud  LowCardinality(String),
    tk_cross        LowCardinality(String),
    vol_signal      LowCardinality(String),

    close_at_signal Float32,

    close_1d        Nullable(Float32),
    close_5d        Nullable(Float32),
    close_10d       Nullable(Float32),
    close_21d       Nullable(Float32),

    return_1d       Nullable(Float32),
    return_5d       Nullable(Float32),
    return_10d      Nullable(Float32),
    return_21d      Nullable(Float32),

    correct_1d      Nullable(UInt8),
    correct_5d      Nullable(UInt8),
    correct_10d     Nullable(UInt8),
    correct_21d     Nullable(UInt8),

    computed_at     DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (symbol, signal_date);
