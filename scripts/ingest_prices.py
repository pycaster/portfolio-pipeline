#!/usr/bin/env python3
"""
Fetch OHLCV price history for all held symbols and load into portfolio.prices.

Symbols are pulled from portfolio.stock_positions (stocks held) and
portfolio.option_positions (underlying symbols of open option contracts).

Prices come from yfinance — the same provider used by ai-trader.
Run this via ai-trader's venv which already has yfinance installed.

Usage (via Makefile):
    make ingest-prices
"""

import os
import sys
import json
import urllib.request
import urllib.parse
from datetime import date, timedelta

try:
    import yfinance as yf
except ImportError:
    print("ERR: yfinance not found — run from ai-trader venv")
    sys.exit(1)


def ch_query(ch_http: str, auth: str, sql: str) -> list:
    """Run a SELECT against ClickHouse HTTP interface, return rows as list of lists."""
    url = f"http://{ch_http}/?{auth}&query={urllib.parse.quote(sql + ' FORMAT JSONCompact')}"
    with urllib.request.urlopen(url) as resp:
        data = json.loads(resp.read())
    return data.get("data", [])


def ch_insert(ch_http: str, auth: str, table: str, columns: list, rows: list):
    """Batch-insert rows into ClickHouse via HTTP using JSONEachRow."""
    if not rows:
        return
    col_str = ", ".join(columns)
    lines = "\n".join(json.dumps(dict(zip(columns, row))) for row in rows)
    url = f"http://{ch_http}/?{auth}&query={urllib.parse.quote(f'INSERT INTO {table} ({col_str}) FORMAT JSONEachRow')}"
    data = lines.encode()
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req) as resp:
        resp.read()


def main():
    ch_http = os.environ.get("CH_HTTP", "localhost:18123")
    ch_user = os.environ.get("CH_USER", "default")
    ch_pass = os.environ.get("CH_PASS", "")
    auth = f"user={urllib.parse.quote(ch_user)}&password={urllib.parse.quote(ch_pass)}"

    # Collect symbols: stocks held + underlying of open option positions
    stock_rows = ch_query(ch_http, auth,
        "SELECT DISTINCT symbol FROM portfolio.stock_positions")
    option_rows = ch_query(ch_http, auth,
        "SELECT DISTINCT symbol FROM portfolio.option_positions WHERE option_expiry >= today()")
    crypto_rows = ch_query(ch_http, auth,
        "SELECT DISTINCT symbol FROM portfolio.crypto_positions")

    stock_symbols  = sorted({r[0] for r in stock_rows} | {r[0] for r in option_rows})
    crypto_symbols = sorted({r[0] for r in crypto_rows})
    all_symbols    = stock_symbols + crypto_symbols

    if not all_symbols:
        print("  no symbols held — nothing to fetch")
        return

    print(f"  stocks/options: {', '.join(stock_symbols) or '(none)'}")
    print(f"  crypto:         {', '.join(crypto_symbols) or '(none)'}")

    # Fetch from the earliest transaction date through today
    earliest_rows = ch_query(ch_http, auth,
        "SELECT min(activity_date) FROM portfolio.transactions FINAL")
    start_date = earliest_rows[0][0] if earliest_rows and earliest_rows[0][0] else str(date.today() - timedelta(days=365))
    end_date = str(date.today())

    print(f"  range:   {start_date} → {end_date}")

    rows = []
    for symbol in all_symbols:
        # Crypto tickers on Yahoo Finance use the SOL-USD format.
        yf_ticker = f"{symbol}-USD" if symbol in crypto_symbols else symbol
        try:
            df = yf.download(yf_ticker, start=start_date, end=end_date,
                             progress=False, auto_adjust=True)
            if df.empty:
                print(f"  WARN  {yf_ticker}: no data returned")
                continue

            # Flatten MultiIndex columns (yfinance quirk when downloading single ticker)
            if hasattr(df.columns, "get_level_values"):
                df.columns = df.columns.get_level_values(0)
            df.columns = [c.lower() for c in df.columns]
            df = df.dropna(subset=["open", "high", "low", "close", "volume"])

            for ts, row in df.iterrows():
                rows.append([
                    symbol,
                    ts.strftime("%Y-%m-%d"),
                    round(float(row["open"]),   6),
                    round(float(row["high"]),   6),
                    round(float(row["low"]),    6),
                    round(float(row["close"]),  6),
                    int(row["volume"]),
                    "yfinance",
                ])

            print(f"  ok    {yf_ticker}: {len(df)} days (last close: {df['close'].iloc[-1]:.2f})")

        except Exception as e:
            print(f"  ERR   {yf_ticker}: {e}", file=sys.stderr)

    if rows:
        ch_insert(ch_http, auth, "portfolio.prices",
                  ["symbol", "date", "open", "high", "low", "close", "volume", "source"],
                  rows)
        print(f"  inserted {len(rows)} price rows")
    else:
        print("  no price rows to insert")


if __name__ == "__main__":
    main()
