# portfolio-pipeline

Go + Python pipeline: broker CSV ingestion, Ichimoku/RSI signal computation, strategy evaluation, and Slack alerting.

## Quick Reference

```
cd /home/vrmap/projects/portfolio-pipeline
make <target>                        # Makefile exports config.env automatically
make trader-deploy                   # rebuild & restart trader container
make trader-now                      # run post-close pipeline immediately
make trader-backfill START=YYYY-MM-DD END=YYYY-MM-DD
docker logs trader --tail 100        # check trader container logs
```

## ClickHouse

- **HTTP:** `localhost:18123` (config.env: `CH_HTTP`)
- **TCP:** `localhost:9000` (config.env: `CH_ADDR`)
- **Creds:** `CH_USER=default`, `CH_PASS` in `config.env`
- **Quick query:** `curl -s "http://localhost:18123/?user=default&password=$CH_PASS" -d "SELECT ... FORMAT Pretty"`
- All tables use `ReplacingMergeTree` — use `FINAL` for deduped reads
- **Note:** `signals.strategy` uses `date` (Date); `signals.strategy_1h` uses `datetime` (DateTime) — not `date`

### Database: `portfolio`

| Table | Type | Key Columns | Notes |
|---|---|---|---|
| `transactions` | RMT | row_hash, activity_date, symbol, trans_code, asset_type, quantity, price, amount | Source of truth for all trades |
| `prices` | RMT | symbol (LowCard), date, OHLCV | Daily prices from yfinance |
| `stock_positions` | VIEW | broker, symbol, shares_held, avg_cost_basis | Current holdings only |
| `option_positions` | VIEW | broker, symbol, option_expiry, option_strike, option_type, contracts_held | Open contracts |
| `crypto_positions` | VIEW | broker, symbol, units_held, avg_cost_basis | Crypto holdings |
| `stock_cost_basis` | VIEW | broker, symbol, avg_purchase_price | Includes exited positions |
| `realized_pnl` | VIEW | activity_date, symbol, trans_code, close_price, proceeds, est_pnl | Stock only, not options |
| `option_contract_pnl` | VIEW | symbol, option_expiry/strike/type, net_pnl, is_closed | Per-contract P&L |
| `insights` | RMT | generated_at, period, model, insight_text, stats_json | LLM-generated insights |

### Database: `signals`

| Table | Type | Key Columns | Notes |
|---|---|---|---|
| `indicators` | RMT | symbol, **date** (Date), close, rsi_14, rsi_zone, tenkan, kijun, senkou_a, senkou_b, cloud_color, price_vs_cloud, tk_cross, signal, signal_score, vol_ratio, obv_trend, vol_signal | Daily Ichimoku+RSI |
| `indicators_1h` | RMT | symbol, **datetime** (DateTime), same columns as indicators | Hourly Ichimoku+RSI |
| `strategy` | RMT | symbol, **date** (Date), decision, score, reasons (Array), prev_decision, signal_id | Daily BUY/WATCH/HOLD/EXIT |
| `strategy_1h` | RMT | symbol, **datetime** (DateTime), decision, score, reasons, prev_decision, signal_id | Hourly strategy |
| `trades` | RMT | signal_id, symbol, decision, signal_date, executed_at, shares, price, notes | Human trade log linked to signals |
| `outcomes` | RMT | symbol, signal_date, decision, return_1d/5d/10d/21d, correct_1d/5d/10d/21d | Forward return tracking |
| `newsfeed_articles` | RMT | article_id, source, title, sectors (Array), sentiment, sentiment_score, tickers (Array) | News articles |
| `newsfeed_outcomes` | RMT | article_id, ticker, sector, sentiment_signed, price_change_1d/2d | Article outcome tracking |
| `newsfeed_ic` | RMT | source, sector, ticker, ic_1d, ic_2d | Information coefficient |

## Strategy Decisions

`BUY` / `WATCH` / `HOLD` / `EXIT` / `SCALP_LONG_CAUTION` / `SCALP_SHORT_CAUTION`

Alerts fire only on **transitions** (decision != prev_decision). `signal_id` = 6-char SHA256 of `symbol:date:decision`.

## Trader Daemon (`scripts/trader.py`)

Runs in Docker container `trader`. APScheduler, NYSE calendar-aware.

| Job | Schedule (ET) | What |
|---|---|---|
| `job_news` | */30 | Ingest news + mentions |
| `job_intraday` | :30 (09:30-15:30) | 1h signals + strategy for stocks |
| `job_crypto_intraday` | :30 (24/7) | 1h signals + strategy for crypto |
| `job_close` | 16:30 | Full daily pipeline: prices → signals → strategy → alert |
| `job_email_ingest` | */5 | Poll Gmail for Robinhood order emails |

On startup, runs: `job_email_ingest` → `job_news` → `job_intraday` → `job_crypto_intraday`.

Pipeline: `_ingest_prices()` → `_compute_signals()` → `evaluate_strategy()` → `send_alert()`

## Key Python Scripts

| Script | Purpose |
|---|---|
| `scripts/trader.py` | Daemon + `--now` / `--backfill` modes |
| `scripts/compute_signals.py` | Ichimoku + RSI + volume indicators (daily & 1h) |
| `scripts/compute_outcomes.py` | Forward return outcomes for strategy decisions |
| `scripts/ingest_prices.py` | Fetch OHLCV from yfinance |
| `scripts/newsfeed.py` | News ingestion, sentiment, IC pipeline |
| `scripts/gen_insights.py` | LLM-generated trading insights |
| `scripts/log_trade.py` | CLI to log human trades to signals.trades |

## Makefile Targets

| Target | Description |
|---|---|
| `make migrate` | Apply pending SQL migrations |
| `make status` | Show positions, migrations, counts |
| `make signals SYMBOLS="X Y"` | Compute indicators for specific symbols |
| `make trader-deploy` | Rebuild + restart trader container |
| `make trader-now` | Run post-close pipeline immediately |
| `make trader-backfill START=... END=...` | Backfill date range |
| `make trader-backfill-intraday` | Backfill 60 days of 1h signals |
| `make outcomes FROM=... SYMBOL=...` | Compute forward return outcomes |
| `make newsfeed-ingest` | Fetch + score news articles |
| `make newsfeed-status` | Show newsfeed pipeline health |

## Go Structure

- `cmd/ingest/main.go` — CSV ingest CLI
- `cmd/email-ingest/` — Gmail IMAP poller for Robinhood order emails
- `internal/broker/robinhood/` — Robinhood CSV parser
- `internal/store/clickhouse.go` — ClickHouse store
- `internal/store/migrate.go` — Up-only migration runner
- `migrations/000001_all.up.sql` — All schema in single migration file

## Config

- `config.env` — ClickHouse creds, Slack webhooks, watchlist (gitignored)
- Key vars: `CH_HTTP`, `CH_ADDR`, `CH_USER`, `CH_PASS`, `CH_DB`, `WATCHLIST`, `SLACK_WEBHOOK_URL`, `SLACK_CRYPTO_WEBHOOK_URL`, `SLACK_OPTIONS_WEBHOOK_URL`
- Python venv: `.venv/` (use `make venv` to create)
- Trader container compose: `/home/vrmap/projects/dockage/stacks/trader/docker-compose.yaml`
