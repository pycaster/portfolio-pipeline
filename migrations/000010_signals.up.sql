-- Signals schema: computed technical indicators (separate from portfolio data).
-- Fully recomputable from price history — safe to truncate/re-run at any time.

CREATE DATABASE IF NOT EXISTS signals;

CREATE TABLE IF NOT EXISTS signals.indicators (
    symbol          LowCardinality(String),
    date            Date,
    close           Float64,

    -- RSI(14) — Wilder's smoothing
    rsi_14          Nullable(Float64),
    rsi_zone        LowCardinality(String),  -- overbought | oversold | neutral

    -- Ichimoku Kinko Hyo (9, 26, 52)
    tenkan          Nullable(Float64),       -- conversion line (9-period mid)
    kijun           Nullable(Float64),       -- base line (26-period mid)
    senkou_a        Nullable(Float64),       -- leading span A (plotted at current date, shift-adjusted)
    senkou_b        Nullable(Float64),       -- leading span B (plotted at current date, shift-adjusted)
    cloud_color     LowCardinality(String),  -- green | red | (empty if null)
    price_vs_cloud  LowCardinality(String),  -- above | inside | below | (empty)
    tk_cross        LowCardinality(String),  -- bullish_cross | bearish_cross | bullish | bearish | neutral

    -- Composite signal
    signal          LowCardinality(String),  -- bullish | bearish | mixed
    signal_score    Int8,                    -- raw score (-6 to +6)

    computed_at     DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (symbol, date)
PARTITION BY toYYYYMM(date);
