-- Rebuild newsfeed tables with sector-based signal design.
-- Drops and recreates all three newsfeed tables.
-- Safe: newsfeed tables contain no production data at this stage.

DROP TABLE IF EXISTS signals.newsfeed_articles;
DROP TABLE IF EXISTS signals.newsfeed_outcomes;
DROP TABLE IF EXISTS signals.newsfeed_ic;

-- Articles with sector classification (qwen3.5) + sentiment (FinBERT).
CREATE TABLE signals.newsfeed_articles (
    article_id       String,                   -- SHA256(url)[:32], dedup key
    source           LowCardinality(String),   -- 'zerohedge'
    url              String,
    title            String,
    full_text        String,                   -- full article text, HTML stripped, ~2000 chars

    published_at     DateTime,
    fetched_at       DateTime DEFAULT now(),

    -- qwen3.5 sector classification
    -- e.g. ['semiconductors', 'geopolitical']
    sectors          Array(LowCardinality(String)),
    sector_match     UInt8 DEFAULT 0,          -- 1 if any sector maps to a current holding

    -- FinBERT sentiment on title + first 512 tokens
    sentiment        LowCardinality(String),   -- positive | negative | neutral
    sentiment_score  Float32,                  -- confidence (0.5–1.0)
    sentiment_signed Float32,                  -- +score / -score / 0

    -- Ticker mentions kept as metadata (not used for signal routing)
    tickers          Array(String)
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (source, article_id)
PARTITION BY toYYYYMM(published_at);

-- Price outcomes per article × ticker (via sector → portfolio exposure map).
CREATE TABLE signals.newsfeed_outcomes (
    article_id       String,
    ticker           String,
    sector           String,                   -- which sector connected this article to this ticker
    sentiment_signed Float32,

    published_at     DateTime,
    price_date       Date,
    price_change_1d  Nullable(Float32),        -- % change close(+1d) / close(price_date) - 1
    price_change_2d  Nullable(Float32),

    outcome_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(outcome_at)
ORDER BY (article_id, ticker, sector);

-- Information Coefficient per source / sector / ticker.
CREATE TABLE signals.newsfeed_ic (
    source        LowCardinality(String),
    sector        String,
    ticker        String,
    ic_1d         Float32,
    ic_2d         Float32,
    sample_count  UInt32,
    computed_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (source, sector, ticker);
