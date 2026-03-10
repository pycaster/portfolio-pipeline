BINARY       := bin/ingest
EMAIL_BINARY := bin/email-ingest

export PATH := $(PATH):/usr/local/go/bin

# Load config.env if present (cp config.env.example config.env to start)
-include config.env
export

TRADER_COMPOSE := /home/vrmap/projects/dockage/stacks/trader/docker-compose.yaml

.PHONY: build build-email ingest-csv email-ingest migrate migrate-status status venv ingest-prices grafana-start grafana-stop gen-insights signals newsfeed-setup newsfeed-ingest newsfeed-outcomes newsfeed-ic newsfeed-alert newsfeed-mentions newsfeed-status trader trader-deploy trader-now trader-setup trader-backfill outcomes check clean

build:
	@mkdir -p bin
	go build -o $(BINARY) ./cmd/ingest

build-email:
	@mkdir -p bin
	go build -o $(EMAIL_BINARY) ./cmd/email-ingest

## Apply all pending schema migrations.
## Safe to run repeatedly — already-applied versions are skipped.
migrate: build
	@$(BINARY) \
		-clickhouse-addr="$(CH_ADDR)" \
		-clickhouse-db="$(CH_DB)" \
		-clickhouse-user="$(CH_USER)" \
		-clickhouse-password="$(CH_PASS)" \
		-migrate

## Show which migrations have been applied vs are pending.
migrate-status: build
	@$(BINARY) \
		-clickhouse-addr="$(CH_ADDR)" \
		-clickhouse-db="$(CH_DB)" \
		-clickhouse-user="$(CH_USER)" \
		-clickhouse-password="$(CH_PASS)" \
		-migrate-status

## Scan uploads/ for new broker CSVs and ingest into ClickHouse.
## Idempotent — re-running with the same file is a no-op.
ingest-csv: build
	@$(BINARY) \
		-uploads-dir="$(UPLOADS)" \
		-clickhouse-addr="$(CH_ADDR)" \
		-clickhouse-db="$(CH_DB)" \
		-clickhouse-user="$(CH_USER)" \
		-clickhouse-password="$(CH_PASS)"

## Quick status: migrations, transaction counts, and all positions.
status:
	@bash scripts/status.sh

## Poll Gmail for unseen Robinhood order emails and ingest into ClickHouse.
## Idempotent — processed emails are marked seen and skipped on subsequent runs.
email-ingest: build-email
	@$(EMAIL_BINARY)

PYTHON := .venv/bin/python

## Set up Python venv and install dependencies (one-time).
venv:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

## Fetch current OHLCV prices for all held symbols via yfinance.
AITRADER_PYTHON := $(PYTHON)
ingest-prices:
	@$(AITRADER_PYTHON) scripts/ingest_prices.py

GRAFANA_DIR     := /home/vrmap/projects/dockage/stacks/grafana
GRAFANA_COMPOSE := $(GRAFANA_DIR)/compose.yaml

## Start Grafana dashboard at http://localhost:3001  (admin/admin).
grafana-start:
	@printf 'CH_USER=%s\nCH_PASS=%s\n' '$(CH_USER)' '$(CH_PASS)' > $(GRAFANA_DIR)/.env
	docker compose -f $(GRAFANA_COMPOSE) up -d
	@echo "  Grafana starting at http://localhost:3001  (admin/admin)"

## Stop Grafana.
grafana-stop:
	docker compose -f $(GRAFANA_COMPOSE) down

## Generate LLM trading insights via Claude and store in portfolio.insights.
## PERIOD defaults to current month. Override: make gen-insights PERIOD=all-time
gen-insights:
	@$(AITRADER_PYTHON) scripts/gen_insights.py $(PERIOD)

## Compute Ichimoku + RSI signals and store in signals.indicators.
## Pass specific symbols: make signals SYMBOLS="MSFT IREN"
## With no SYMBOLS arg, computes for all currently-held positions.
signals:
	@$(AITRADER_PYTHON) scripts/compute_signals.py $(SYMBOLS)

## Install Python deps for newsfeed pipeline (one-time setup).
newsfeed-setup:
	$(AITRADER_PYTHON) -m pip install transformers torch

## Fetch new ZeroHedge articles, score with FinBERT, store to ClickHouse.
newsfeed-ingest:
	@$(AITRADER_PYTHON) scripts/newsfeed.py --ingest

## Fill price outcomes for articles >= 1 day old (run nightly after ingest-prices).
newsfeed-outcomes:
	@$(AITRADER_PYTHON) scripts/newsfeed.py --outcomes

## Recompute Information Coefficient per source/ticker (run after newsfeed-outcomes).
newsfeed-ic:
	@$(AITRADER_PYTHON) scripts/newsfeed.py --ic

## Print actionable signals for Sally (portfolio-matched, high-confidence).
newsfeed-alert:
	@$(AITRADER_PYTHON) scripts/newsfeed.py --alert

## Show ticker mention velocity vs 7-day baseline. Highlights 2x+ spikes.
newsfeed-mentions:
	@$(AITRADER_PYTHON) scripts/newsfeed.py --mentions

## Show article counts, IC table, pipeline status.
newsfeed-status:
	@$(AITRADER_PYTHON) scripts/newsfeed.py --status

## Install Python deps for trader daemon (one-time setup).
trader-setup:
	$(AITRADER_PYTHON) -m pip install apscheduler pandas_market_calendars

## Run trader daemon (blocking — runs on market schedule).
trader:
	@$(AITRADER_PYTHON) scripts/trader.py

## Run post-close pipeline immediately (prices → signals → strategy → alert).
trader-now:
	@$(AITRADER_PYTHON) scripts/trader.py --now

## Backfill pipeline for a date range. START required, END optional (defaults to START).
## Example: make trader-backfill START=2026-02-24 END=2026-03-07
trader-backfill:
	@$(AITRADER_PYTHON) scripts/trader.py --backfill $(START) $(END)

## Backfill last 60 days of 1h intraday signals (indicators_1h + strategy_1h).
trader-backfill-intraday:
	@$(AITRADER_PYTHON) scripts/trader.py --backfill-intraday

## Build binaries and redeploy the trader Docker container.
## Run this after any code change or first-time setup.
trader-deploy: build-email
	cp config.env $(dir $(TRADER_COMPOSE))config.env
	docker compose -f $(TRADER_COMPOSE) up -d --force-recreate
	@echo "  trader redeployed — logs: docker logs trader -f"

## Compute forward return outcomes for all strategy decisions.
## Pass SYMBOL or FROM to filter: make outcomes FROM=2026-02-24
## Pass SYMBOL=PLTR to limit to one symbol: make outcomes SYMBOL=PLTR
outcomes:
	@$(PYTHON) scripts/compute_outcomes.py $(if $(SYMBOL),--symbol $(SYMBOL),) $(if $(FROM),--from $(FROM),)

## Compile check and go vet.
check:
	go vet ./...

clean:
	rm -rf bin/
