# portfolio-pipeline

Personal portfolio analytics pipeline. Ingests broker trade history (Robinhood CSV exports), stores data in ClickHouse, and runs a sector-aware news sentiment pipeline to surface signals relevant to held positions.

## What it does

- **Trade ingestion** — Parses Robinhood CSV exports into a ClickHouse database. Tracks transactions, positions, cost basis, and realized P&L including options.
- **Price history** — Fetches OHLCV data via yfinance for held tickers.
- **News sentiment pipeline** — Fetches financial news articles, classifies them into sectors (semiconductors, defense_tech, energy, ai_cloud, macro_rates, geopolitical, fintech_saas), scores sentiment with FinBERT, and tracks which articles are relevant to current holdings.
- **Insider filing monitor** — Pulls SEC EDGAR Form 4 filings for held tickers to track insider buys/sells.
- **Ticker mention velocity** — Tracks how often tickers appear in news/social media over time; alerts on unusual acceleration vs 7-day baseline (pre-event detection).
- **Information Coefficient** — After accumulating enough articles with price outcomes, computes `corr(sentiment, price_change_Nd)` per source/sector/ticker to measure which news sources are actually predictive.
- **Grafana dashboards** — Visual dashboards for positions, P&L, signals, and newsfeed signals.

## Reddit API usage

This project reads **public posts from r/wallstreetbets** to track retail investor attention toward tickers in the portfolio (NVDA, PLTR, IREN). The purpose is purely analytical and personal:

- **Read-only** — only `GET /r/wallstreetbets/hot` and `GET /r/wallstreetbets/new` endpoints
- **No user data collected** — post author names are not stored; only post title and URL are used to extract ticker symbols (`$TICKER` mentions)
- **Personal use only** — data is stored locally in a private ClickHouse instance; nothing is redistributed or published
- **Low volume** — ingested approximately every 30 minutes during US market hours (~48 requests/day)
- **Goal** — detect when a ticker is getting unusually high retail attention before a price move (mention velocity spike as a pre-event signal)

## Stack

- **Go 1.22** — trade ingestion binary
- **Python 3** — newsfeed pipeline, price ingestion, signal computation
- **ClickHouse** — time-series storage for all data
- **LocalAI** — local LLM inference (Qwen2.5-3B for sector classification, Qwen3.5 for insights)
- **FinBERT** — financial sentiment scoring (CPU, local)
- **Grafana** — dashboards

## Project structure

```
cmd/ingest/         Go CLI for trade CSV ingestion
internal/broker/    Broker-specific CSV parsers (Robinhood)
internal/store/     ClickHouse store + migration runner
internal/pipeline/  File scanner + state tracking
migrations/         SQL migration files (up-only)
scripts/
  newsfeed.py       News + EDGAR + Reddit sentiment pipeline
  compute_signals.py  Ichimoku + RSI technical signal computation
  ingest_prices.py  OHLCV price fetcher (yfinance)
  gen_insights.py   LLM-generated portfolio commentary
```

## Configuration

Copy `config.env.example` to `config.env` and fill in your ClickHouse credentials.

## Makefile targets

```
make migrate          Apply pending DB migrations
make ingest           Ingest latest broker CSV
make ingest-prices    Fetch OHLCV price data
make newsfeed-ingest  Fetch + classify news articles
make newsfeed-mentions  Show ticker mention velocity
make newsfeed-alert   Print actionable signals
make newsfeed-status  Pipeline status summary
make signals SYMBOLS="NVDA PLTR"  Recompute technical signals
make status           Portfolio summary
```
