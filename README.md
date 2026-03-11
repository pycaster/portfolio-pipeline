# portfolio-pipeline

Personal portfolio analytics pipeline. Ingests broker trade history (Robinhood CSV exports + Gmail order emails), computes Ichimoku/RSI technical signals, evaluates trading strategy, and alerts via Slack. Backed by ClickHouse for time-series storage.

## What it does

- **Trade ingestion** — Parses Robinhood CSV exports and polls Gmail for order confirmation emails. Tracks transactions, positions, cost basis, and realized P&L including options and crypto.
- **Price history** — Fetches OHLCV data via yfinance for all held tickers + watchlist.
- **Technical signals** — Computes Ichimoku Cloud (tenkan, kijun, senkou A/B) + RSI(14) + volume indicators on both daily and 1-hour timeframes.
- **Strategy evaluation** — Scores each symbol into BUY / WATCH / HOLD / EXIT / SCALP decisions based on cloud position, TK cross, RSI zone, volume, and divergences. Alerts on state transitions only.
- **Trade logging** — Links human trade executions back to signals via `signal_id` (6-char SHA256). Sally (Slack agent) can log trades via natural language.
- **News sentiment pipeline** — Fetches financial news, classifies into sectors, scores sentiment with FinBERT, and tracks Information Coefficient per source.
- **Ticker mention velocity** — Tracks news/social mention frequency; alerts on unusual acceleration vs 7-day baseline.
- **Outcome tracking** — Computes forward returns (1d/5d/10d/21d) for every strategy decision to measure signal effectiveness.
- **Grafana dashboards** — Visual dashboards for positions, P&L, signals, and newsfeed.

## Stack

- **Go 1.22** — trade ingestion binary, email-ingest binary
- **Python 3** — signal computation, strategy evaluation, news pipeline, price ingestion
- **ClickHouse** — time-series storage (`portfolio` + `signals` databases)
- **Docker** — trader daemon runs as container with APScheduler
- **LocalAI** — local LLM inference (Qwen2.5-3B for sector classification, Qwen3.5 for insights)
- **FinBERT** — financial sentiment scoring (CPU, local)
- **Grafana** — dashboards
- **Sally (OpenClaw)** — Slack agent for alerts, heartbeat monitoring, and trade logging

## Project structure

```
cmd/ingest/              Go CLI for trade CSV ingestion
cmd/email-ingest/        Go CLI for Gmail IMAP order email polling
internal/broker/         Broker-specific CSV parsers (Robinhood)
internal/store/          ClickHouse store + migration runner
internal/pipeline/       File scanner + state tracking
migrations/              SQL migration files (up-only, single file)
scripts/
  trader.py              Signal daemon — scheduler, strategy eval, Slack alerts
  compute_signals.py     Ichimoku + RSI + volume indicators (daily & 1h)
  compute_outcomes.py    Forward return outcomes for strategy decisions
  ingest_prices.py       OHLCV price fetcher (yfinance)
  newsfeed.py            News + EDGAR + Reddit sentiment pipeline
  gen_insights.py        LLM-generated portfolio commentary
  log_trade.py           CLI to log human trades linked to signals
config.env               ClickHouse creds, Slack webhooks, watchlist (gitignored)
```

## Trader daemon

Runs in Docker container `trader` on a market-aware schedule (NYSE calendar):

| Job | Schedule (ET) | Pipeline |
|---|---|---|
| `job_news` | Every 30 min | Ingest news articles + mention velocity |
| `job_intraday` | :30 past hour, 09:30–15:30 | 1h signals → strategy → alert (stocks) |
| `job_crypto_intraday` | :30 past hour, 24/7 | 1h signals → strategy → alert (crypto) |
| `job_close` | 16:30 | Daily: prices → signals → strategy → alert → outcomes |
| `job_email_ingest` | Every 5 min | Poll Gmail for Robinhood order emails |

On startup, runs the intraday + crypto pipelines immediately so a deploy doesn't miss a window.

## Configuration

Copy `config.env.example` to `config.env` and fill in:
- ClickHouse credentials (`CH_HTTP`, `CH_ADDR`, `CH_USER`, `CH_PASS`, `CH_DB`)
- Slack webhook URLs (`SLACK_WEBHOOK_URL`, `SLACK_CRYPTO_WEBHOOK_URL`, `SLACK_OPTIONS_WEBHOOK_URL`)
- Watchlist symbols (`WATCHLIST=SPY,BTC`)

## Makefile targets

```
make migrate                          Apply pending DB migrations
make status                           Portfolio summary (positions, counts)
make ingest-csv                       Ingest latest broker CSV
make ingest-prices                    Fetch OHLCV price data
make signals SYMBOLS="NVDA PLTR"      Recompute technical signals

make trader-deploy                    Build + restart trader container
make trader-now                       Run post-close pipeline immediately
make trader-backfill START=... END=.. Backfill daily pipeline for date range
make trader-backfill-intraday         Backfill 60 days of 1h signals

make newsfeed-ingest                  Fetch + classify news articles
make newsfeed-mentions                Show ticker mention velocity
make newsfeed-alert                   Print actionable signals
make newsfeed-status                  Pipeline status summary
make outcomes FROM=... SYMBOL=...     Compute forward return outcomes
make gen-insights                     Generate LLM trading commentary
```

## Reddit API usage

This project reads **public posts from r/wallstreetbets** to track retail investor attention toward tickers in the portfolio. The purpose is purely analytical and personal:

- **Read-only** — only `GET /r/wallstreetbets/hot` and `GET /r/wallstreetbets/new` endpoints
- **No user data collected** — post author names are not stored; only post title and URL are used to extract ticker symbols
- **Personal use only** — data is stored locally in a private ClickHouse instance; nothing is redistributed
- **Low volume** — approximately every 30 minutes during US market hours
