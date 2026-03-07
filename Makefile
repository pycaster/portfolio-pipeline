BINARY       := bin/ingest
EMAIL_BINARY := bin/email-ingest

export PATH := $(PATH):/usr/local/go/bin

# Load config.env if present (cp config.env.example config.env to start)
-include config.env
export

.PHONY: build build-email ingest-csv email-ingest migrate migrate-status status ingest-prices grafana-start grafana-stop gen-insights signals newsfeed-setup newsfeed-ingest newsfeed-outcomes newsfeed-ic newsfeed-alert newsfeed-mentions newsfeed-status check clean

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

## Fetch current OHLCV prices for all held symbols via yfinance (ai-trader venv).
## Writes into portfolio.prices with source='yfinance', superseding transaction-day prices.
AITRADER_PYTHON := /home/vrmap/projects/ai-trader/.venv/bin/python
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

## Compile check and go vet.
check:
	go vet ./...

clean:
	rm -rf bin/
