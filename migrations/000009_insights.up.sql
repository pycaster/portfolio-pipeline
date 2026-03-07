-- LLM-generated trading insights, one row per generation run.
-- ReplacingMergeTree on (period, model): re-running gen_insights.py for the same
-- period+model replaces the previous insight rather than accumulating duplicates.
-- Query with FINAL to see the latest row per (period, model).
CREATE TABLE IF NOT EXISTS portfolio.insights (
    generated_at DateTime DEFAULT now(),
    period       LowCardinality(String),   -- e.g. '2026-02', 'all-time'
    model        LowCardinality(String),   -- e.g. 'claude-opus-4-6'
    insight_text String,
    stats_json   String                    -- JSON snapshot of metrics sent to LLM
) ENGINE = ReplacingMergeTree(generated_at)
ORDER BY (period, model)
PARTITION BY toYYYYMM(generated_at);
