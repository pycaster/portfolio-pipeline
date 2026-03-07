-- Signal outcome tracking: forward price returns for every strategy decision.
-- Populated by compute_outcomes.py after each backfill or nightly run.
-- Use this to measure which signals (divergence types, score ranges, conditions)
-- actually predict price direction correctly.

CREATE TABLE IF NOT EXISTS signals.outcomes (
    symbol          String,
    signal_date     Date,
    decision        LowCardinality(String),  -- BUY | WATCH | HOLD | EXIT
    score           Int8,
    rsi_divergence  LowCardinality(String),  -- bullish_div | hidden_bull | bearish_div | hidden_bear | ''
    rsi_zone        LowCardinality(String),  -- oversold | neutral | overbought
    price_vs_cloud  LowCardinality(String),  -- above | inside | below
    tk_cross        LowCardinality(String),
    vol_signal      LowCardinality(String),

    close_at_signal Float32,

    -- Forward close prices (Nullable — future not yet available)
    close_1d        Nullable(Float32),
    close_5d        Nullable(Float32),
    close_10d       Nullable(Float32),
    close_21d       Nullable(Float32),

    -- % returns: (close_Nd - close_at_signal) / close_at_signal
    return_1d       Nullable(Float32),
    return_5d       Nullable(Float32),
    return_10d      Nullable(Float32),
    return_21d      Nullable(Float32),

    -- Directional correctness: 1=correct, 0=wrong
    -- BUY correct if return > 0, EXIT correct if return < 0
    correct_1d      Nullable(UInt8),
    correct_5d      Nullable(UInt8),
    correct_10d     Nullable(UInt8),
    correct_21d     Nullable(UInt8),

    computed_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (symbol, signal_date);
