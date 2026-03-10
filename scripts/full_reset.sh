#!/usr/bin/env bash
# full_reset.sh — Drop all portfolio + signals data, apply consolidated migration,
# reingest all CSVs, backfill prices and signals from 2025-01-01 to today.
#
# Usage:
#   cd /home/vrmap/projects/portfolio-pipeline
#   bash scripts/full_reset.sh
#
# Pre-requisite: config.env must be present (see config.env.example).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

# Load config
if [[ -f config.env ]]; then
    set -a; source config.env; set +a
fi

CH_HTTP="${CH_HTTP:-localhost:18123}"
CH_USER="${CH_USER:-default}"
CH_PASS="${CH_PASS:-}"
AUTH="user=${CH_USER}&password=${CH_PASS}"
BASE="http://${CH_HTTP}"

q() {
    local sql="$1"
    curl -sf -d "${sql}" "${BASE}/?${AUTH}" \
        || { echo "  ERROR: ClickHouse unreachable or query failed"; exit 1; }
}

banner() { echo; echo "════════════════════════════════════════"; echo "  $1"; echo "════════════════════════════════════════"; }

# ─────────────────────────────────────────────
# 1. Drop all existing objects
# ─────────────────────────────────────────────
banner "1/7  Dropping existing views and tables"

for obj in \
    "VIEW IF EXISTS portfolio.stock_positions" \
    "VIEW IF EXISTS portfolio.option_positions" \
    "VIEW IF EXISTS portfolio.crypto_positions" \
    "VIEW IF EXISTS portfolio.stock_cost_basis" \
    "VIEW IF EXISTS portfolio.realized_pnl" \
    "VIEW IF EXISTS portfolio.option_contract_pnl" \
    "TABLE IF EXISTS portfolio.transactions" \
    "TABLE IF EXISTS portfolio.prices" \
    "TABLE IF EXISTS portfolio.insights" \
    "TABLE IF EXISTS portfolio._migrations" \
    "TABLE IF EXISTS signals.indicators" \
    "TABLE IF EXISTS signals.strategy" \
    "TABLE IF EXISTS signals.indicators_1h" \
    "TABLE IF EXISTS signals.strategy_1h" \
    "TABLE IF EXISTS signals.newsfeed_articles" \
    "TABLE IF EXISTS signals.newsfeed_outcomes" \
    "TABLE IF EXISTS signals.newsfeed_ic" \
    "TABLE IF EXISTS signals.outcomes"
do
    echo "  DROP $obj"
    q "DROP $obj"
done

echo "  Done."

# ─────────────────────────────────────────────
# 2. Apply consolidated migration
# ─────────────────────────────────────────────
banner "2/7  Applying consolidated migration"
make migrate
echo "  Done."

# ─────────────────────────────────────────────
# 3. Reset pipeline state (force full re-ingest)
# ─────────────────────────────────────────────
banner "3/7  Resetting pipeline state"
STATE_FILE="${UPLOADS:-uploads}/.pipeline-state.json"
echo '{"last_ingest":{}}' > "$STATE_FILE"
echo "  Written: $STATE_FILE"

# ─────────────────────────────────────────────
# 4. Reingest all CSVs
# ─────────────────────────────────────────────
banner "4/7  Reingesting all broker CSVs"
make ingest-csv
echo "  Done."

# ─────────────────────────────────────────────
# 5. Backfill prices (yfinance, 2025-01-01 → today)
# ─────────────────────────────────────────────
banner "5/7  Backfilling prices"
PYTHON="${PYTHON:-.venv/bin/python}"
"$PYTHON" scripts/ingest_prices.py
echo "  Done."

# ─────────────────────────────────────────────
# 6. Backfill signals + outcomes
# ─────────────────────────────────────────────
banner "6/7  Backfilling signals and outcomes"
TODAY="$(date +%Y-%m-%d)"
echo "  Running trader backfill 2025-01-02 → ${TODAY}"
"$PYTHON" scripts/trader.py --backfill 2025-01-02 "$TODAY"

echo "  Computing forward-return outcomes"
"$PYTHON" scripts/compute_outcomes.py

echo "  Backfilling 1h intraday signals (last 60 days)"
"$PYTHON" scripts/trader.py --backfill-intraday

echo "  Done."

banner "Reset complete"
echo "  Run 'make status' to verify positions."
