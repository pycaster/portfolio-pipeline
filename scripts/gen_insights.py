#!/usr/bin/env python3
"""
Generate LLM trading insights via Claude API and store in portfolio.insights.

Queries ~10 key stats from ClickHouse, builds a prompt, calls Claude, and
inserts the result back into ClickHouse for display in the Grafana dashboard.

Usage (via Makefile):
    make gen-insights                    # current month (default)
    make gen-insights PERIOD=all-time    # all-time summary
    make gen-insights PERIOD=2026-01     # specific month
"""

import os
import sys
import json
import urllib.request
import urllib.parse
from datetime import date



# ── ClickHouse helpers (same pattern as ingest_prices.py) ─────────────────

def ch_query(ch_http: str, auth: str, sql: str) -> list:
    url = (
        f"http://{ch_http}/?"
        f"{auth}&query={urllib.parse.quote(sql + ' FORMAT JSONCompact')}"
    )
    with urllib.request.urlopen(url, timeout=30) as resp:
        data = json.loads(resp.read())
    return data.get("data", [])


def ch_insert(ch_http: str, auth: str, table: str, columns: list, rows: list):
    if not rows:
        return
    col_str = ", ".join(columns)
    lines = "\n".join(json.dumps(dict(zip(columns, row))) for row in rows)
    url = (
        f"http://{ch_http}/?"
        f"{auth}&query={urllib.parse.quote(f'INSERT INTO {table} ({col_str}) FORMAT JSONEachRow')}"
    )
    req = urllib.request.Request(url, data=lines.encode(), method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        resp.read()


# ── Stat queries ───────────────────────────────────────────────────────────

def gather_stats(ch_http: str, auth: str, period: str) -> dict:
    """Run ~10 queries and return a dict of key portfolio metrics."""

    if period == "all-time":
        date_filter = "1=1"
        opt_filter = "1=1"
    else:
        year, month = period.split("-")
        ym = f"{year}{month}"
        date_filter = f"toYYYYMM(activity_date) = {ym}"
        opt_filter = f"toYYYYMM(closed_date) = {ym}"

    def scalar(sql):
        rows = ch_query(ch_http, auth, sql)
        return rows[0][0] if rows and rows[0] else None

    def rows(sql):
        return ch_query(ch_http, auth, sql)

    stats = {}

    # 1. Realized stock P&L
    stats["realized_stock_pnl"] = scalar(
        f"SELECT round(sum(est_pnl), 2) FROM portfolio.realized_pnl WHERE {date_filter}"
    )

    # 2. Option net P&L (closed contracts)
    stats["realized_option_pnl"] = scalar(
        f"SELECT round(sum(net_pnl), 2) FROM portfolio.option_contract_pnl"
        f" WHERE {opt_filter} AND is_closed = 1"
    )

    # 3. Total trade count
    stats["total_trades"] = scalar(
        f"SELECT count() FROM portfolio.transactions FINAL"
        f" WHERE {date_filter}"
        f" AND trans_code IN ('BUY','SELL','BTO','STC','STO','BTC','OEXP','OASGN')"
    )

    # 4. Win rate
    win_rows = rows(
        f"SELECT countIf(est_pnl > 0) AS wins, count() AS total"
        f" FROM portfolio.realized_pnl WHERE {date_filter}"
    )
    if win_rows:
        wins = int(win_rows[0][0] or 0)
        total_closed = int(win_rows[0][1] or 0)
        stats["win_rate_pct"] = round(100 * wins / total_closed, 1) if total_closed > 0 else None
        stats["total_closed_trades"] = total_closed
    else:
        stats["win_rate_pct"] = None
        stats["total_closed_trades"] = 0

    # 5. Top 3 winning symbols
    top_wins = rows(
        f"SELECT symbol, round(sum(est_pnl), 2) AS pnl"
        f" FROM portfolio.realized_pnl"
        f" WHERE est_pnl > 0 AND {date_filter}"
        f" GROUP BY symbol ORDER BY pnl DESC LIMIT 3"
    )
    stats["top_winners"] = [{"symbol": r[0], "pnl": r[1]} for r in top_wins]

    # 6. Top 3 losing symbols
    top_losses = rows(
        f"SELECT symbol, round(sum(est_pnl), 2) AS pnl"
        f" FROM portfolio.realized_pnl"
        f" WHERE est_pnl < 0 AND {date_filter}"
        f" GROUP BY symbol ORDER BY pnl ASC LIMIT 3"
    )
    stats["top_losers"] = [{"symbol": r[0], "pnl": r[1]} for r in top_losses]

    # 7. Open position counts
    stats["open_stock_positions"] = scalar("SELECT count() FROM portfolio.stock_positions")
    stats["open_option_contracts"] = scalar(
        "SELECT count() FROM portfolio.option_positions WHERE option_expiry >= today()"
    )
    stats["open_crypto_positions"] = scalar("SELECT count() FROM portfolio.crypto_positions")

    # 8. Most active symbols
    most_active = rows(
        f"SELECT symbol, count() AS trades"
        f" FROM portfolio.transactions FINAL"
        f" WHERE {date_filter} AND trans_code IN ('BUY','SELL','BTO','STC','STO','BTC')"
        f" GROUP BY symbol ORDER BY trades DESC LIMIT 5"
    )
    stats["most_active_symbols"] = [{"symbol": r[0], "trades": r[1]} for r in most_active]

    # 9. Best and worst option contract
    best_opt = rows(
        f"SELECT symbol, option_type, option_strike, option_expiry, round(net_pnl, 2)"
        f" FROM portfolio.option_contract_pnl WHERE {opt_filter}"
        f" ORDER BY net_pnl DESC LIMIT 1"
    )
    worst_opt = rows(
        f"SELECT symbol, option_type, option_strike, option_expiry, round(net_pnl, 2)"
        f" FROM portfolio.option_contract_pnl WHERE {opt_filter}"
        f" ORDER BY net_pnl ASC LIMIT 1"
    )
    stats["best_option"] = list(best_opt[0]) if best_opt else None
    stats["worst_option"] = list(worst_opt[0]) if worst_opt else None

    # 10. Unrealized P&L (current stock positions vs latest prices)
    portfolio_rows = rows(
        "SELECT"
        "  sp.symbol,"
        "  round(sp.shares_held * p.latest_close, 2) AS market_value,"
        "  round(sp.shares_held * sp.avg_cost_basis, 2) AS cost_basis"
        " FROM portfolio.stock_positions sp"
        " LEFT JOIN ("
        "   SELECT symbol, argMax(close, date) AS latest_close"
        "   FROM portfolio.prices FINAL GROUP BY symbol"
        " ) p ON sp.symbol = p.symbol"
    )
    total_market = sum(float(r[1] or 0) for r in portfolio_rows)
    total_cost   = sum(float(r[2] or 0) for r in portfolio_rows)
    stats["total_market_value"]  = round(total_market, 2)
    stats["total_cost_basis"]    = round(total_cost, 2)
    stats["unrealized_pnl"]      = round(total_market - total_cost, 2)

    return stats


# ── Prompt ─────────────────────────────────────────────────────────────────

PROMPT_TEMPLATE = """\
You are a quantitative trading analyst reviewing a personal trading portfolio.
Below are key metrics for the period: {period}.

PERFORMANCE:
- Realized stock P&L: ${realized_stock_pnl}
- Realized option P&L: ${realized_option_pnl}
- Unrealized P&L (stocks at market): ${unrealized_pnl}
- Total portfolio market value: ${total_market_value}
- Total cost basis: ${total_cost_basis}

TRADE ACTIVITY:
- Total trades executed: {total_trades}
- Closed stock trades with P&L data: {total_closed_trades}
- Stock win rate: {win_rate_pct}%
- Open stock positions: {open_stock_positions}
- Open option contracts: {open_option_contracts}
- Open crypto positions: {open_crypto_positions}

TOP WINNERS: {top_winners_str}
TOP LOSERS:  {top_losers_str}
MOST ACTIVE: {most_active_str}
BEST OPTION: {best_option_str}
WORST OPTION: {worst_option_str}

Write a concise analyst commentary (3–5 paragraphs) that:
1. Summarizes overall performance with specific dollar figures.
2. Calls out notable winners and losers.
3. Comments on option trading performance and risk profile.
4. Identifies one concrete trading pattern or risk worth watching \
(e.g. over-concentration, high churn, strategy-specific loss pattern).
5. Ends with a one-sentence forward-looking observation.

Be direct and specific. Use dollar amounts. No generic advice boilerplate. \
Write as if for the trader themselves reviewing their own account.
"""


def build_prompt(stats: dict, period: str) -> str:
    def fmt_list(items, sym_key="symbol", val_key="pnl"):
        if not items:
            return "none"
        return ", ".join(f"{i[sym_key]} (${i[val_key]})" for i in items)

    def fmt_opt(row):
        if not row:
            return "none"
        sym, otype, strike, expiry, pnl = row
        return f"{sym} {otype} ${strike} exp {expiry} → ${pnl}"

    return PROMPT_TEMPLATE.format(
        period=period,
        realized_stock_pnl=stats.get("realized_stock_pnl") or "N/A",
        realized_option_pnl=stats.get("realized_option_pnl") or "N/A",
        unrealized_pnl=stats.get("unrealized_pnl") or "N/A",
        total_market_value=stats.get("total_market_value") or "N/A",
        total_cost_basis=stats.get("total_cost_basis") or "N/A",
        total_trades=stats.get("total_trades") or 0,
        total_closed_trades=stats.get("total_closed_trades") or 0,
        win_rate_pct=stats.get("win_rate_pct") or "N/A",
        open_stock_positions=stats.get("open_stock_positions") or 0,
        open_option_contracts=stats.get("open_option_contracts") or 0,
        open_crypto_positions=stats.get("open_crypto_positions") or 0,
        top_winners_str=fmt_list(stats.get("top_winners", [])),
        top_losers_str=fmt_list(stats.get("top_losers", [])),
        most_active_str=fmt_list(stats.get("most_active_symbols", []), val_key="trades"),
        best_option_str=fmt_opt(stats.get("best_option")),
        worst_option_str=fmt_opt(stats.get("worst_option")),
    )


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    ch_http      = os.environ.get("CH_HTTP",       "localhost:18123")
    ch_user      = os.environ.get("CH_USER",       "default")
    ch_pass      = os.environ.get("CH_PASS",       "")
    ccr_base_url = os.environ.get("CCR_BASE_URL",  "http://127.0.0.1:3456/v1")
    ccr_api_key  = os.environ.get("CCR_API_KEY",   "sk-123456")
    model_id     = os.environ.get("CLAUDE_MODEL",  "claude-opus-4-6")

    # Period: CLI arg → env var → current month
    if len(sys.argv) > 1 and sys.argv[1]:
        period = sys.argv[1]
    else:
        period = os.environ.get("PERIOD") or date.today().strftime("%Y-%m")

    auth = f"user={urllib.parse.quote(ch_user)}&password={urllib.parse.quote(ch_pass)}"

    print(f"  period:  {period}")
    print(f"  model:   {model_id}")
    print("  gathering stats from ClickHouse...")

    stats = gather_stats(ch_http, auth, period)
    stats_json_str = json.dumps(stats, default=str)
    preview = stats_json_str[:120]
    print(f"  stats:   {preview}...")

    prompt = build_prompt(stats, period)

    print(f"  calling CCR ({ccr_base_url})...")
    payload = json.dumps({
        "model": model_id,
        "max_tokens": 16000,
        "messages": [{"role": "user", "content": prompt}],
    }).encode()
    req = urllib.request.Request(
        f"{ccr_base_url}/messages",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "x-api-key": ccr_api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read())
    insight_text = result["content"][0]["text"]
    print(f"  insight: {insight_text[:100]}...")

    print("  inserting into portfolio.insights...")
    ch_insert(
        ch_http, auth,
        "portfolio.insights",
        ["period", "model", "insight_text", "stats_json"],
        [[period, model_id, insight_text, stats_json_str]],
    )

    print(f"  done — run 'make grafana-start' and reload the AI Insights panel")


if __name__ == "__main__":
    main()
