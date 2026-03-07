#!/usr/bin/env bash
set -euo pipefail

CH_HTTP="${CH_HTTP:-localhost:18123}"
CH_USER="${CH_USER:-default}"
CH_PASS="${CH_PASS:-}"

AUTH="user=${CH_USER}&password=${CH_PASS}"
BASE="http://${CH_HTTP}"

q() {
    curl -sf "${BASE}/?${AUTH}&query=${1}" || echo "  ClickHouse unreachable at ${CH_HTTP}"
}

echo "=== migrations ==="
q "SELECT+version,+name,+applied_at+FROM+portfolio._migrations+FINAL+ORDER+BY+version+FORMAT+PrettyCompact"

echo ""
echo "=== transactions by broker ==="
q "SELECT+broker,+count()+AS+rows,+min(activity_date)+AS+since,+max(activity_date)+AS+until+FROM+portfolio.transactions+FINAL+GROUP+BY+broker+ORDER+BY+broker+FORMAT+PrettyCompact"

echo ""
echo "=== current stock positions ==="
q "SELECT+symbol,+round(shares_held,4)+AS+shares,+round(avg_cost_basis,2)+AS+avg_cost,+broker+FROM+portfolio.stock_positions+ORDER+BY+shares+DESC+FORMAT+PrettyCompact"

echo ""
echo "=== open option contracts ==="
q "SELECT+symbol,+option_type,+option_strike,+option_expiry,+round(contracts_held,0)+AS+contracts+FROM+portfolio.option_positions+ORDER+BY+symbol+FORMAT+PrettyCompact"

echo ""
echo "=== crypto positions ==="
q "SELECT+symbol,+round(units_held,6)+AS+units,+round(avg_cost_basis,2)+AS+avg_cost,+broker+FROM+portfolio.crypto_positions+ORDER+BY+units+DESC+FORMAT+PrettyCompact"
