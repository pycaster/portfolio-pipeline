-- News feed signal pipeline.
-- Articles from RSS sources → FinBERT sentiment → price outcome tracking → IC per source.
-- Safe to re-run (IF NOT EXISTS throughout).

-- signals DB already exists from migration 000010.

-- Raw article store. One row per article, deduped by article_id (SHA256 of URL).
CREATE TABLE IF NOT EXISTS signals.newsfeed_articles (
    article_id       String,                   -- SHA256(url), dedup key
    source           LowCardinality(String),   -- 'zerohedge'
    url              String,
    title            String,
    summary          String,                   -- first 500 chars of RSS description

    published_at     DateTime,
    fetched_at       DateTime DEFAULT now(),

    -- Tickers extracted from title+summary (matched against portfolio symbol list)
    tickers          Array(String),
    portfolio_match  UInt8 DEFAULT 0,          -- 1 if any ticker in current positions

    -- FinBERT sentiment
    sentiment        LowCardinality(String),   -- positive | negative | neutral
    sentiment_score  Float32,                  -- FinBERT confidence (0.5–1.0)
    sentiment_signed Float32                   -- +score if positive, -score if negative, 0 if neutral
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (source, article_id)
PARTITION BY toYYYYMM(published_at);

-- Price outcomes: filled in by newsfeed-outcomes after N trading days have elapsed.
-- Separate table so we can update without touching the article record.
CREATE TABLE IF NOT EXISTS signals.newsfeed_outcomes (
    article_id       String,
    ticker           String,
    sentiment_signed Float32,   -- copied from article at scoring time

    published_at     DateTime,
    price_date       Date,      -- closest trading day on/after published_at
    price_change_1d  Nullable(Float32),  -- % change: close(+1d) / close(price_date) - 1
    price_change_2d  Nullable(Float32),

    outcome_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(outcome_at)
ORDER BY (article_id, ticker);

-- Information Coefficient per source/ticker.
-- Recomputed periodically by newsfeed-ic. Stores most recent calculation.
CREATE TABLE IF NOT EXISTS signals.newsfeed_ic (
    source        LowCardinality(String),
    ticker        String,
    ic_1d         Float32,   -- corr(sentiment_signed, price_change_1d)
    ic_2d         Float32,
    sample_count  UInt32,
    computed_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (source, ticker);
