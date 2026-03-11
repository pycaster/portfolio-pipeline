#!/usr/bin/env python3
"""
log_trade.py — Record a human-confirmed trade execution against a signal.

Called by Sally when Venkat replies to a signal alert with:
  "executed NVDA 50sh @875.40 [sig:a3f9]"
  "executed NVDA 50sh @875.40"   (no sig: Sally resolves latest signal for symbol)

Usage:
    python3 scripts/log_trade.py --symbol NVDA --shares 50 --price 875.40 --signal-id a3f9
    python3 scripts/log_trade.py --symbol NVDA --shares 50 --price 875.40  # auto-resolve
    python3 scripts/log_trade.py --symbol NVDA --shares -25 --price 880.00 --notes "partial exit"

Exits 0 on success (prints confirmation line).
Exits 1 on error (prints error to stderr).
"""

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone


def ch_query(ch_http: str, auth: str, sql: str) -> list:
    url = f"http://{ch_http}/?{auth}&query={urllib.parse.quote(sql + ' FORMAT JSONCompact')}"
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.loads(resp.read()).get("data", [])


def ch_insert(ch_http: str, auth: str, table: str, columns: list, rows: list):
    col_str = ", ".join(columns)
    lines   = "\n".join(json.dumps(dict(zip(columns, row))) for row in rows)
    url     = (f"http://{ch_http}/?{auth}&query="
               f"{urllib.parse.quote(f'INSERT INTO {table} ({col_str}) FORMAT JSONEachRow')}")
    req = urllib.request.Request(url, data=lines.encode(), method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


def resolve_signal(ch_http: str, auth: str, symbol: str) -> tuple[str, str, str] | None:
    """Find the most recent BUY/EXIT signal for symbol. Returns (signal_id, decision, date)."""
    # Check daily signals first, then intraday
    for table, date_col in [("signals.strategy", "date"), ("signals.strategy_1h", "datetime")]:
        rows = ch_query(ch_http, auth, f"""
            SELECT signal_id, decision, {date_col}
            FROM {table} FINAL
            WHERE symbol = '{symbol}'
              AND decision IN ('BUY', 'EXIT', 'SCALP_LONG_CAUTION', 'SCALP_SHORT_CAUTION')
              AND signal_id != ''
            ORDER BY {date_col} DESC
            LIMIT 1
        """)
        if rows:
            return str(rows[0][0]), str(rows[0][1]), str(rows[0][2])
    return None


def main():
    parser = argparse.ArgumentParser(description="Log a trade execution against a signal")
    parser.add_argument("--symbol",    required=True,  help="Ticker symbol (e.g. NVDA)")
    parser.add_argument("--shares",    required=True,  type=float,
                        help="Number of shares (positive=buy, negative=sell/exit)")
    parser.add_argument("--price",     required=True,  type=float, help="Execution price per share")
    parser.add_argument("--signal-id", default="",     help="6-char sig ID from alert (e.g. a3f9ab)")
    parser.add_argument("--notes",     default="",     help="Optional notes")
    args = parser.parse_args()

    ch_http = os.environ.get("CH_HTTP", "localhost:18123")
    ch_user = os.environ.get("CH_USER", "default")
    ch_pass = os.environ.get("CH_PASS", "")
    auth    = f"user={urllib.parse.quote(ch_user)}&password={urllib.parse.quote(ch_pass)}"

    symbol    = args.symbol.upper()
    signal_id = args.signal_id.strip()
    decision  = ""
    date_str  = ""

    if signal_id:
        # Verify signal exists and get its metadata
        for table, date_col in [("signals.strategy", "date"), ("signals.strategy_1h", "datetime")]:
            rows = ch_query(ch_http, auth, f"""
                SELECT decision, {date_col}
                FROM {table} FINAL
                WHERE signal_id = '{signal_id}' AND symbol = '{symbol}'
                LIMIT 1
            """)
            if rows:
                decision = str(rows[0][0])
                date_str = str(rows[0][1])
                break
        if not decision:
            print(f"ERR: signal {signal_id} not found for {symbol}", file=sys.stderr)
            sys.exit(1)
    else:
        # Auto-resolve: find latest actionable signal for this symbol
        result = resolve_signal(ch_http, auth, symbol)
        if result:
            signal_id, decision, date_str = result
            print(f"  resolved to latest signal: sig:{signal_id} ({decision} on {date_str})",
                  file=sys.stderr)
        else:
            signal_id = "manual"
            decision  = "BUY" if args.shares > 0 else "EXIT"
            date_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            print(f"  no signal found — logging as manual {decision}", file=sys.stderr)

    executed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    ch_insert(ch_http, auth, "signals.trades",
              ["signal_id", "symbol", "decision", "signal_date",
               "executed_at", "shares", "price", "notes"],
              [[signal_id, symbol, decision, date_str,
                executed_at, args.shares, args.price, args.notes]])

    action = "BUY" if args.shares > 0 else "SELL"
    print(
        f"✅ Trade logged: {action} {abs(args.shares):.0f}sh {symbol} @ ${args.price:.2f}"
        f" | sig:{signal_id} ({decision} {date_str})"
    )


if __name__ == "__main__":
    main()
