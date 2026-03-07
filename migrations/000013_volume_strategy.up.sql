-- Volume signal columns on signals.indicators.
-- Populated by trader.py (or compute_signals.py) on next run.

ALTER TABLE signals.indicators
    ADD COLUMN IF NOT EXISTS vol_ratio   Float32                  DEFAULT 0,
    ADD COLUMN IF NOT EXISTS obv_trend   LowCardinality(String)   DEFAULT '',
    ADD COLUMN IF NOT EXISTS vol_signal  LowCardinality(String)   DEFAULT '';

-- Strategy decisions: one row per symbol per day.
-- decision: BUY | WATCH | HOLD | EXIT
-- prev_decision: previous trading day's decision (for transition detection)
CREATE TABLE IF NOT EXISTS signals.strategy (
    symbol        String,
    date          Date,
    decision      LowCardinality(String),
    score         Int8,
    reasons       Array(String),
    prev_decision LowCardinality(String) DEFAULT ''
)
ENGINE = ReplacingMergeTree()
ORDER BY (symbol, date);
