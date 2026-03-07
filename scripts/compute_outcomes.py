#!/usr/bin/env python3
"""
compute_outcomes.py — Fill forward price returns for strategy decisions.

For every row in signals.strategy, look up closing prices 1, 5, 10, 21
trading days forward in portfolio.prices and compute % returns.

Marks each outcome as correct/wrong based on decision direction:
  BUY   → correct if return > 0 (price rose after signal)
  EXIT  → correct if return < 0 (price fell after signal, exit was right)
  WATCH → correct if return > 0 (was worth watching)
  HOLD  → no directional expectation, correctness not scored

Idempotent — re-running overwrites via ReplacingMergeTree.

Usage:
    python scripts/compute_outcomes.py           # all strategy rows with enough forward data
    python scripts/compute_outcomes.py --symbol PLTR
    python scripts/compute_outcomes.py --from 2026-01-01
"""

import os
import sys
import json
import argparse
import urllib.error
import urllib.request
import urllib.parse
from datetime import date, timedelta
from collections import defaultdict

CH_HTTP = os.environ.get("CH_HTTP", "localhost:18123")
CH_USER = os.environ.get("CH_USER", "default")
CH_PASS = os.environ.get("CH_PASS", "")
CH_AUTH = f"user={urllib.parse.quote(CH_USER)}&password={urllib.parse.quote(CH_PASS)}"

FORWARD_DAYS = [1, 5, 10, 21]
DIRECTIONAL  = {"BUY", "EXIT", "WATCH"}  # decisions where correctness is meaningful


def ch_query(sql: str) -> list:
    url = f"http://{CH_HTTP}/?{CH_AUTH}&query={urllib.parse.quote(sql + ' FORMAT JSONCompact')}"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            return json.loads(resp.read()).get("data", [])
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        raise RuntimeError(f"CH {e.code}: {body.strip()}") from None


def ch_insert(table: str, columns: list, rows: list):
    if not rows:
        return
    col_str = ", ".join(columns)
    lines   = "\n".join(json.dumps(dict(zip(columns, r))) for r in rows)
    url     = f"http://{CH_HTTP}/?{CH_AUTH}&query={urllib.parse.quote(f'INSERT INTO {table} ({col_str}) FORMAT JSONEachRow')}"
    req     = urllib.request.Request(url, data=lines.encode(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp.read()
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        raise RuntimeError(f"CH insert {table} {e.code}: {body.strip()}") from None


def load_prices(symbols: list[str]) -> dict[str, dict[str, float]]:
    """Load all price history as {symbol: {date_str: close}}."""
    sym_list = ", ".join(f"'{s}'" for s in symbols)
    rows = ch_query(f"""
        SELECT symbol, date, close
        FROM portfolio.prices FINAL
        WHERE symbol IN ({sym_list})
        ORDER BY symbol, date
    """)
    prices: dict[str, dict[str, float]] = defaultdict(dict)
    for sym, dt, close in rows:
        prices[sym][str(dt)[:10]] = float(close)
    return prices


def sorted_dates(price_map: dict[str, float]) -> list[str]:
    return sorted(price_map.keys())


def nth_trading_day_after(dates: list[str], signal_date: str, n: int) -> str | None:
    """Return the nth trading date after signal_date in the sorted date list."""
    try:
        idx = dates.index(signal_date)
    except ValueError:
        return None
    target = idx + n
    if target < len(dates):
        return dates[target]
    return None


def compute_correctness(decision: str, ret: float | None) -> int | None:
    if ret is None or decision not in DIRECTIONAL:
        return None
    if decision == "EXIT":
        return 1 if ret < 0 else 0
    else:  # BUY, WATCH
        return 1 if ret > 0 else 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", help="Limit to one symbol")
    parser.add_argument("--from", dest="from_date", help="Only process signal_date >= this (YYYY-MM-DD)")
    args = parser.parse_args()

    # Load strategy decisions
    where_clauses = ["1=1"]
    if args.symbol:
        where_clauses.append(f"symbol = '{args.symbol}'")
    if args.from_date:
        where_clauses.append(f"date >= '{args.from_date}'")
    where = " AND ".join(where_clauses)

    print("Loading strategy decisions...")
    strategy_rows = ch_query(f"""
        SELECT
            s.symbol, s.date, s.decision, s.score,
            i.rsi_divergence, i.rsi_zone, i.price_vs_cloud, i.tk_cross, i.vol_signal
        FROM signals.strategy s FINAL
        LEFT JOIN signals.indicators i FINAL
            ON s.symbol = i.symbol AND s.date = i.date
        WHERE {where}
        ORDER BY s.symbol, s.date
    """)

    if not strategy_rows:
        print("No strategy rows found.")
        return

    symbols = list({r[0] for r in strategy_rows})
    print(f"Loading prices for {len(symbols)} symbols...")
    prices = load_prices(symbols)

    # Pre-sort date lists per symbol for efficient nth-day lookup
    date_lists: dict[str, list[str]] = {s: sorted_dates(prices[s]) for s in symbols}

    output_columns = [
        "symbol", "signal_date", "decision", "score",
        "rsi_divergence", "rsi_zone", "price_vs_cloud", "tk_cross", "vol_signal",
        "close_at_signal",
        "close_1d", "close_5d", "close_10d", "close_21d",
        "return_1d", "return_5d", "return_10d", "return_21d",
        "correct_1d", "correct_5d", "correct_10d", "correct_21d",
    ]

    output_rows = []
    skipped = 0

    for r in strategy_rows:
        symbol, signal_date, decision, score, rsi_div, rsi_zone, pvc, tk, vol = r
        signal_date = str(signal_date)[:10]
        score       = int(score)

        sym_prices = prices.get(symbol, {})
        close_at   = sym_prices.get(signal_date)
        if close_at is None:
            skipped += 1
            continue

        dates = date_lists.get(symbol, [])
        forwards = {}
        returns  = {}
        correct  = {}

        for n in FORWARD_DAYS:
            fwd_date = nth_trading_day_after(dates, signal_date, n)
            fwd_close = sym_prices.get(fwd_date) if fwd_date else None
            forwards[n] = fwd_close
            if fwd_close is not None:
                ret = (fwd_close - close_at) / close_at
                returns[n] = round(ret, 6)
                correct[n] = compute_correctness(decision, ret)
            else:
                returns[n] = None
                correct[n] = None

        output_rows.append([
            symbol, signal_date, decision, score,
            rsi_div or "", rsi_zone or "", pvc or "", tk or "", vol or "",
            round(close_at, 4),
            forwards[1], forwards[5], forwards[10], forwards[21],
            returns[1],  returns[5],  returns[10],  returns[21],
            correct[1],  correct[5],  correct[10],  correct[21],
        ])

    ch_insert("signals.outcomes", output_columns, output_rows)
    print(f"Inserted {len(output_rows)} outcome rows ({skipped} skipped — no price on signal date)")

    # Print quick effectiveness summary
    if output_rows:
        _print_summary(output_rows, output_columns)


def _print_summary(rows: list, columns: list):
    """Print signal effectiveness by decision type and divergence."""
    idx = {c: i for i, c in enumerate(columns)}

    # Group by decision
    from collections import defaultdict
    groups: dict[str, list] = defaultdict(list)
    for r in rows:
        groups[r[idx["decision"]]].append(r)

    print("\n── Signal Effectiveness (5-day return) ──")
    print(f"{'Decision':<8}  {'Count':>5}  {'Avg 5d':>7}  {'Win%':>6}  {'Avg 10d':>8}  {'Win% 10d':>9}")
    print("─" * 55)

    for dec in ["BUY", "WATCH", "HOLD", "EXIT"]:
        grp = groups.get(dec, [])
        if not grp:
            continue
        rets5  = [r[idx["return_5d"]]  for r in grp if r[idx["return_5d"]]  is not None]
        rets10 = [r[idx["return_10d"]] for r in grp if r[idx["return_10d"]] is not None]
        cor5   = [r[idx["correct_5d"]] for r in grp if r[idx["correct_5d"]] is not None]
        cor10  = [r[idx["correct_10d"]] for r in grp if r[idx["correct_10d"]] is not None]
        avg5   = f"{sum(rets5)/len(rets5)*100:+.1f}%" if rets5 else "n/a"
        avg10  = f"{sum(rets10)/len(rets10)*100:+.1f}%" if rets10 else "n/a"
        win5   = f"{sum(cor5)/len(cor5)*100:.0f}%" if cor5 else "n/a"
        win10  = f"{sum(cor10)/len(cor10)*100:.0f}%" if cor10 else "n/a"
        print(f"{dec:<8}  {len(grp):>5}  {avg5:>7}  {win5:>6}  {avg10:>8}  {win10:>9}")

    # Divergence breakdown
    divs: dict[str, list] = defaultdict(list)
    for r in rows:
        d = r[idx["rsi_divergence"]]
        if d:
            divs[d].append(r)

    if divs:
        print("\n── By RSI Divergence (5-day return) ──")
        print(f"{'Divergence':<14}  {'Count':>5}  {'Avg 5d':>7}  {'Win%':>6}  {'Decisions'}")
        print("─" * 60)
        for div_type, grp in sorted(divs.items()):
            rets5 = [r[idx["return_5d"]] for r in grp if r[idx["return_5d"]] is not None]
            cor5  = [r[idx["correct_5d"]] for r in grp if r[idx["correct_5d"]] is not None]
            avg5  = f"{sum(rets5)/len(rets5)*100:+.1f}%" if rets5 else "n/a"
            win5  = f"{sum(cor5)/len(cor5)*100:.0f}%" if cor5 else "n/a"
            decs  = ", ".join(f"{d}({sum(1 for r in grp if r[idx['decision']]==d)})"
                              for d in ["BUY","WATCH","HOLD","EXIT"]
                              if any(r[idx["decision"]]==d for r in grp))
            print(f"{div_type:<14}  {len(grp):>5}  {avg5:>7}  {win5:>6}  {decs}")


def main_from_args(symbol: str | None = None, from_date: str | None = None):
    """Callable entry point for trader.py without argparse."""
    import sys as _sys
    old_argv = _sys.argv
    _sys.argv = ["compute_outcomes.py"]
    if symbol:
        _sys.argv += ["--symbol", symbol]
    if from_date:
        _sys.argv += ["--from", from_date]
    try:
        main()
    finally:
        _sys.argv = old_argv


if __name__ == "__main__":
    main()
