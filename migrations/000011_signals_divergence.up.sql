-- Add RSI divergence column to signals.indicators.
-- Populated by compute_signals.py on next run (safe to re-run for existing symbols).

ALTER TABLE signals.indicators
    ADD COLUMN IF NOT EXISTS rsi_divergence LowCardinality(String) DEFAULT '';
