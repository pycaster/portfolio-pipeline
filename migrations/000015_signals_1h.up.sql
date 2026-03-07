-- Intraday (1h) signals and strategy tables.
-- Mirrors signals.indicators and signals.strategy but uses DateTime for bar timestamps.
-- Bar timestamps are stored as UTC. Use toTimezone(datetime, 'America/New_York') in queries.

CREATE TABLE IF NOT EXISTS signals.indicators_1h (
    symbol          LowCardinality(String),
    datetime        DateTime,           -- UTC bar open time (e.g. 14:30 UTC = 09:30 ET)
    close           Float64,

    -- RSI(14)
    rsi_14          Nullable(Float64),
    rsi_zone        LowCardinality(String),  -- overbought | oversold | neutral

    -- Ichimoku (9, 26, 52 periods on 1h bars)
    tenkan          Nullable(Float64),
    kijun           Nullable(Float64),
    senkou_a        Nullable(Float64),
    senkou_b        Nullable(Float64),
    cloud_color     LowCardinality(String),
    price_vs_cloud  LowCardinality(String),
    tk_cross        LowCardinality(String),

    -- RSI divergence (context only)
    rsi_divergence  LowCardinality(String) DEFAULT '',

    -- Composite signal
    signal          LowCardinality(String),
    signal_score    Int8,

    -- Volume
    vol_ratio       Float32 DEFAULT 0,
    obv_trend       LowCardinality(String) DEFAULT '',
    vol_signal      LowCardinality(String) DEFAULT '',

    computed_at     DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (symbol, datetime)
PARTITION BY toYYYYMM(datetime);


-- Strategy decisions: one row per symbol per 1h bar.
CREATE TABLE IF NOT EXISTS signals.strategy_1h (
    symbol        LowCardinality(String),
    datetime      DateTime,
    decision      LowCardinality(String),  -- BUY | WATCH | HOLD | EXIT
    score         Int8,
    reasons       Array(String),
    prev_decision LowCardinality(String) DEFAULT ''
) ENGINE = ReplacingMergeTree()
ORDER BY (symbol, datetime);
