#!/usr/bin/env python3
"""
trader.py — Portfolio signal daemon.

Runs on a market-aware schedule (NYSE calendar):
  every 30min ET    →  ingest news + mentions (sentiment signals, daily)
  09:30–15:30 ET    →  intraday Ichimoku/RSI signals on 1h bars (every :30)
  every 30min ET    →  poll Gmail for Robinhood order emails (daily)
  16:30 ET          →  ingest prices → compute signals → evaluate strategy → alert
  Monday 07:00 ET   →  generate LLM trading insights via Claude API

Strategy evaluation combines Ichimoku + RSI + divergence + volume signals
into BUY / WATCH / HOLD / EXIT decisions, alerting only on transitions.

Usage:
    python scripts/trader.py                              # daemon
    python scripts/trader.py --now                        # run today's pipeline immediately
    python scripts/trader.py --backfill 2026-02-24        # single day
    python scripts/trader.py --backfill 2026-02-24 2026-03-07  # date range
"""

import hashlib
import os
import sys
import json
import logging
import argparse
import subprocess
import urllib.error
import urllib.request
import urllib.parse
from datetime import date, timedelta

try:
    import pandas_market_calendars as mcal
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger
except ImportError as e:
    print(f"ERR: missing dependency — {e}")
    print("     pip install apscheduler pandas_market_calendars")
    sys.exit(1)

# Ensure scripts/ dir is importable
sys.path.insert(0, os.path.dirname(__file__))
REPO_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_venv_py    = os.path.join(REPO_DIR, ".venv", "bin", "python")
VENV_PYTHON = _venv_py if os.path.exists(_venv_py) else sys.executable
import compute_signals
import compute_outcomes
import newsfeed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("trader")

# ---------------------------------------------------------------------------
# Config from env
# ---------------------------------------------------------------------------

CH_HTTP  = os.environ.get("CH_HTTP",  "localhost:18123")
CH_USER  = os.environ.get("CH_USER",  "default")
CH_PASS  = os.environ.get("CH_PASS",  "")
CH_AUTH  = f"user={urllib.parse.quote(CH_USER)}&password={urllib.parse.quote(CH_PASS)}"

WATCHLIST_ENV     = os.environ.get("WATCHLIST", "").strip()
WATCHLIST         = {s.strip().upper() for s in WATCHLIST_ENV.split(",") if s.strip()} if WATCHLIST_ENV else set()

SLACK_WEBHOOK_URL        = os.environ.get("SLACK_WEBHOOK_URL", "")
SLACK_INGEST_WEBHOOK_URL = os.environ.get("SLACK_INGEST_WEBHOOK_URL", "")
SLACK_CRYPTO_WEBHOOK_URL  = os.environ.get("SLACK_CRYPTO_WEBHOOK_URL", "")
SLACK_OPTIONS_WEBHOOK_URL = os.environ.get("SLACK_OPTIONS_WEBHOOK_URL", "")

# ---------------------------------------------------------------------------
# ClickHouse helpers
# ---------------------------------------------------------------------------

def ch_query(sql: str) -> list:
    url = f"http://{CH_HTTP}/?{CH_AUTH}&query={urllib.parse.quote(sql + ' FORMAT JSONCompact')}"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            return json.loads(resp.read()).get("data", [])
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        raise RuntimeError(f"CH query HTTP {e.code}: {body.strip()}") from None


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
        raise RuntimeError(f"CH insert {table} HTTP {e.code}: {body.strip()}") from None

# ---------------------------------------------------------------------------
# Market calendar helpers
# ---------------------------------------------------------------------------

NYSE = mcal.get_calendar("NYSE")


def is_trading_day(d: date | None = None) -> bool:
    d = d or date.today()
    return not NYSE.schedule(start_date=str(d), end_date=str(d)).empty


def trading_days_in_range(start: date, end: date) -> list[date]:
    """Return all NYSE trading days in [start, end] inclusive."""
    schedule = NYSE.schedule(start_date=str(start), end_date=str(end))
    return [d.date() for d in schedule.index]


def prev_trading_day(d: date) -> date | None:
    """Previous NYSE trading day before d."""
    look_back = d - timedelta(days=10)
    schedule  = NYSE.schedule(start_date=str(look_back), end_date=str(d - timedelta(days=1)))
    if schedule.empty:
        return None
    return schedule.index[-1].date()


def all_symbols() -> list[str]:
    """Union of held positions + watchlist."""
    stock_rows  = ch_query("SELECT DISTINCT symbol FROM portfolio.stock_positions")
    option_rows = ch_query("SELECT DISTINCT symbol FROM portfolio.option_positions WHERE option_expiry >= today()")
    crypto_rows = ch_query("SELECT DISTINCT symbol FROM portfolio.crypto_positions")
    held = {r[0] for r in stock_rows} | {r[0] for r in option_rows} | {r[0] for r in crypto_rows}
    return sorted(held | WATCHLIST)

# ---------------------------------------------------------------------------
# Strategy evaluation
# ---------------------------------------------------------------------------

VOL_SCORE = {"accumulation": 1, "distribution": -1, "neutral": 0}


def evaluate_strategy(symbols: list[str], target_date: date | None = None,
                      alert: bool = True) -> list[dict]:
    """
    Read signals.indicators for target_date, apply strategy rules,
    write decisions to signals.strategy.

    target_date  — date to evaluate (default: today). Pass historical date for backfill.
    alert        — if False, skip alerting (used during backfill).
    """
    if not symbols:
        return []

    today    = target_date or date.today()
    today_s  = str(today)
    sym_list = ", ".join(f"'{s}'" for s in symbols)

    rows = ch_query(f"""
        SELECT
            symbol, signal_score, rsi_zone, price_vs_cloud, tk_cross,
            vol_ratio, obv_trend, vol_signal, close,
            kijun, cloud_color, senkou_a, senkou_b
        FROM signals.indicators FINAL
        WHERE symbol IN ({sym_list}) AND date = '{today_s}'
    """)

    if not rows:
        log.warning("evaluate_strategy: no indicator rows for %s", today_s)
        return []

    # Previous decisions for transition detection
    prev_day = prev_trading_day(today)
    if prev_day:
        prev_rows = ch_query(f"""
            SELECT symbol, decision
            FROM signals.strategy FINAL
            WHERE symbol IN ({sym_list}) AND date = '{prev_day}'
        """)
        prev_map = {r[0]: r[1] for r in prev_rows}
    else:
        prev_map = {}

    strategy_rows = []
    transitions   = []

    for r in rows:
        (symbol, signal_score, rsi_zone, price_vs_cloud,
         tk_cross, vol_ratio, obv_trend, vol_signal, close,
         kijun, cloud_color, senkou_a, senkou_b) = r

        signal_score  = int(signal_score)
        vol_ratio     = float(vol_ratio)
        close_f       = float(close)
        kijun_f       = float(kijun)
        cloud_top_f   = max(float(senkou_a), float(senkou_b))
        prev_decision = prev_map.get(symbol, "")
        reasons       = []

        if price_vs_cloud == "above":
            reasons.append("above_cloud")
        elif price_vs_cloud == "below":
            reasons.append("below_cloud")

        if tk_cross in ("bullish_cross", "bullish"):
            reasons.append(f"tk_{tk_cross}")
        elif tk_cross in ("bearish_cross", "bearish"):
            reasons.append(f"tk_{tk_cross}")

        if vol_signal != "neutral":
            reasons.append(f"{vol_signal} (vol {vol_ratio:.1f}x avg)")

        if obv_trend == "rising":
            reasons.append("obv_rising")
        elif obv_trend == "falling":
            reasons.append("obv_falling")

        total = signal_score + VOL_SCORE.get(vol_signal, 0)

        # Cloud retest: price pulled back into/below cloud but Kijun held and
        # TK structure remains bullish in a green cloud — accumulation zone.
        cloud_retest = (
            price_vs_cloud in ("below", "inside")
            and cloud_color == "green"
            and tk_cross in ("bullish", "bullish_cross")
            and close_f >= kijun_f
            and total >= 1
        )

        # Cloud reclaim: price jumped back above cloud after a retest WATCH —
        # lower BUY threshold since Kijun support was already confirmed.
        cloud_reclaim = (
            price_vs_cloud == "above"
            and prev_decision == "WATCH"
            and tk_cross in ("bullish", "bullish_cross")
            and vol_signal == "accumulation"
            and total >= 3
            and rsi_zone != "overbought"
        )

        if cloud_retest:
            reasons.append("cloud_retest")
        if cloud_reclaim:
            reasons.append("cloud_reclaim")

        exit_cond = (
            (
                total <= -5
                or (total <= -3 and price_vs_cloud == "below" and vol_signal == "distribution")
            )
            and rsi_zone != "oversold"
        )
        buy_cond = (
            (
                total >= 5
                and vol_ratio > 1.2
                and rsi_zone != "overbought"
                and price_vs_cloud == "above"
            )
            or cloud_reclaim
        )

        if exit_cond:
            decision = "EXIT"
        elif buy_cond:
            decision = "BUY"
        elif (total >= 4 and price_vs_cloud == "above") or cloud_retest:
            decision = "WATCH"
        else:
            decision = "HOLD"

        signal_id = hashlib.sha256(
            f"{symbol}:{today_s}:{decision}".encode()
        ).hexdigest()[:6]
        strategy_rows.append([symbol, today_s, decision, total, reasons, prev_decision, signal_id])

        if decision != prev_decision and decision in ("BUY", "EXIT"):
            transitions.append({
                "symbol":    symbol,
                "decision":  decision,
                "prev":      prev_decision,
                "score":     total,
                "reasons":   reasons,
                "close":     float(close),
                "rsi_zone":  rsi_zone,
                "date":      today_s,
                "kijun":     kijun_f,
                "cloud_top": cloud_top_f,
                "signal_id": signal_id,
            })

    ch_insert("signals.strategy",
              ["symbol", "date", "decision", "score", "reasons", "prev_decision", "signal_id"],
              strategy_rows)

    # Only log per-symbol detail in live mode; backfill prints its own summary
    if alert:
        for row in strategy_rows:
            sym, _, dec, score, rsns, _, _sid = row
            log.info("  %-8s  %-5s  score=%+d  %s", sym, dec, score,
                     " | ".join(rsns[:3]))
        log.info("strategy: wrote %d rows for %s", len(strategy_rows), today_s)

    return transitions

def _slack_post(webhook_url: str, text: str):
    """POST a plain-text message to a Slack incoming webhook."""
    body = json.dumps({"text": text}).encode()
    req  = urllib.request.Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


def send_alert(transitions: list[dict], webhook_url: str = ""):
    """Post BUY/EXIT transition alerts to Slack via incoming webhook.

    webhook_url selects the channel:
      SLACK_WEBHOOK_URL         — daily/close signals (stocks, all)
      SLACK_CRYPTO_WEBHOOK_URL  — 1h crypto signals
      SLACK_OPTIONS_WEBHOOK_URL — 1h signals for symbols with open options

    Defaults to SLACK_WEBHOOK_URL if not specified.
    """
    if not transitions:
        return
    url = webhook_url or SLACK_WEBHOOK_URL
    if not url:
        log.warning("send_alert: no webhook URL — %d transition(s) suppressed", len(transitions))
        return

    for t in transitions:
        symbol   = t["symbol"]
        decision = t["decision"]
        score    = t["score"]
        reasons  = t["reasons"]
        close    = t["close"]
        date_str = t.get("date", "")

        emoji       = {"EXIT": "🚨", "BUY": "📈", "SCALP_LONG_CAUTION": "⚡↑", "SCALP_SHORT_CAUTION": "⚡↓"}.get(decision, "📈")
        reasons_str = " | ".join(reasons[:4]) if reasons else "—"
        signal_id   = t.get("signal_id", "")
        kijun       = t.get("kijun")
        cloud_top   = t.get("cloud_top")
        sig_footer  = f"`sig:{signal_id}`" if signal_id else ""

        # Levels line: stop and target derived from Ichimoku levels
        levels_line = ""
        if decision == "BUY" and kijun and cloud_top:
            stop_pct   = (close - kijun) / close * 100
            target_pct = (cloud_top - close) / close * 100
            levels_line = f"\nStop: ${kijun:.2f} ({stop_pct:+.1f}%)  Target: ${cloud_top:.2f} ({target_pct:+.1f}%)"
        elif decision in ("SCALP_LONG_CAUTION", "SCALP_SHORT_CAUTION") and kijun:
            direction  = "↑" if decision == "SCALP_LONG_CAUTION" else "↓"
            levels_line = f"\nTarget {direction} ${kijun:.2f} (Kijun) | RSI {t.get('rsi', 0):.0f}"
        elif decision == "EXIT" and kijun:
            levels_line = f"\nKijun: ${kijun:.2f}"

        # For EXIT, show how much is held so the urgency is clear
        held_note = ""
        if decision == "EXIT":
            try:
                rows = ch_query(
                    f"SELECT quantity FROM portfolio.stock_positions "
                    f"WHERE symbol = '{symbol}' LIMIT 1"
                )
                if rows:
                    held_note = f" | Held: {rows[0][0]} shares"
                else:
                    rows = ch_query(
                        f"SELECT quantity FROM portfolio.option_positions "
                        f"WHERE symbol = '{symbol}' AND option_expiry >= today() LIMIT 1"
                    )
                    if rows:
                        held_note = f" | Held: {rows[0][0]} contracts"
            except Exception:
                pass

        msg = (
            f"{emoji} *{decision} — {symbol}* @ ${close:.2f}{held_note}\n"
            f"Score: {score:+d} | {reasons_str}"
            f"{levels_line}\n"
            f"_{date_str}_ {sig_footer}"
        )
        try:
            _slack_post(url, msg)
            log.info("send_alert: posted %s %s to Slack", decision, symbol)
        except Exception as e:
            log.error("send_alert: Slack post failed — %s", e)


def _option_symbols() -> set[str]:
    """Return symbols with currently open option positions."""
    rows = ch_query(
        "SELECT DISTINCT symbol FROM portfolio.option_positions "
        "WHERE option_expiry >= today()"
    )
    return {r[0] for r in rows}

# ---------------------------------------------------------------------------
# Price + signal ingestion (date-aware)
# ---------------------------------------------------------------------------

def _ingest_prices(symbols: list[str], end_date: date | None = None,
                   start_date: date | None = None):
    """Fetch OHLCV up to end_date (default: today) and write to portfolio.prices."""
    import yfinance as yf

    crypto_rows    = ch_query("SELECT DISTINCT symbol FROM portfolio.crypto_positions")
    crypto_symbols = {r[0] for r in crypto_rows}

    end   = end_date or date.today()
    start = start_date or (end - timedelta(days=365))

    rows = []
    for symbol in symbols:
        yf_ticker = f"{symbol}-USD" if symbol in crypto_symbols else symbol
        try:
            df = yf.download(yf_ticker, start=str(start), end=str(end + timedelta(days=1)),
                             progress=False, auto_adjust=True)
            if df.empty:
                continue
            if hasattr(df.columns, "get_level_values"):
                df.columns = df.columns.get_level_values(0)
            df.columns = [c.lower() for c in df.columns]
            df = df.loc[:, ~df.columns.duplicated()]  # guard against multi-ticker column bleed
            df = df.dropna(subset=["open", "high", "low", "close", "volume"])
            # Trim to requested end_date
            df = df[df.index.date <= end]
            for ts, row in df.iterrows():
                rows.append([
                    symbol, ts.strftime("%Y-%m-%d"),
                    round(float(row["open"]),  6), round(float(row["high"]),  6),
                    round(float(row["low"]),   6), round(float(row["close"]), 6),
                    int(row["volume"]), "yfinance",
                ])
            log.debug("  prices  %-8s  %d days", yf_ticker, len(df))
        except Exception as e:
            log.error("  prices  %s: %s", yf_ticker, e)

    ch_insert("portfolio.prices",
              ["symbol", "date", "open", "high", "low", "close", "volume", "source"],
              rows)
    log.info("prices: inserted %d rows", len(rows))


def _compute_signals_intraday(symbols: list[str]):
    """Fetch 1h OHLCV and compute intraday Ichimoku+RSI+volume signals.

    Writes the last 60 days of 1h bars to signals.indicators_1h.
    Called by job_intraday() each hour during market hours.
    """
    crypto_rows    = ch_query("SELECT DISTINCT symbol FROM portfolio.crypto_positions")
    crypto_symbols = {r[0] for r in crypto_rows}

    columns = [
        "symbol", "datetime", "close",
        "rsi_14", "rsi_zone",
        "tenkan", "kijun", "senkou_a", "senkou_b",
        "cloud_color", "price_vs_cloud", "tk_cross",
        "signal", "signal_score",
        "vol_ratio", "obv_trend", "vol_signal",
    ]

    total = 0
    for symbol in symbols:
        yf_ticker = f"{symbol}-USD" if symbol in crypto_symbols else symbol
        try:
            records = compute_signals.compute_for_symbol_1h(symbol, yf_ticker)
        except Exception as e:
            log.error("  signals_1h %s: %s", symbol, e)
            continue
        if not records:
            continue
        rows = [[r[c] for c in columns] for r in records]
        ch_insert("signals.indicators_1h", columns, rows)
        total += len(rows)
        latest = records[-1]
        log.info("  intraday %-8s  %s ET  score=%+d  vol=%.1fx [%s]",
                 symbol, latest["datetime"][11:16],
                 latest["signal_score"], latest["vol_ratio"], latest["vol_signal"])

    log.info("signals_1h: inserted %d rows", total)


def evaluate_strategy_intraday(symbols: list[str], alert: bool = True,
                               target_dt: str | None = None) -> list[dict]:
    """
    Read the latest 1h indicator bar per symbol, apply strategy rules,
    write decisions to signals.strategy_1h.

    target_dt  — UTC datetime string ('2026-03-07 14:30:00'). When given, evaluate
                 that specific bar (used during backfill). Defaults to today's latest bar.
    Returns transitions (BUY/EXIT decision changes vs previous bar).
    """
    if not symbols:
        return []

    sym_list = ", ".join(f"'{s}'" for s in symbols)

    if target_dt:
        # Backfill mode: evaluate a specific historical bar
        indicator_filter = f"datetime = '{target_dt}'"
    else:
        # Live mode: latest completed bar for each symbol today
        indicator_filter = f"""(symbol, datetime) IN (
              SELECT symbol, max(datetime)
              FROM signals.indicators_1h FINAL
              WHERE symbol IN ({sym_list})
                AND toDate(datetime, 'America/New_York') = toDate(now(), 'America/New_York')
              GROUP BY symbol
          )"""

    rows = ch_query(f"""
        SELECT
            symbol, signal_score, rsi_zone, price_vs_cloud, tk_cross,
            vol_ratio, obv_trend, vol_signal, close, datetime,
            kijun, cloud_color, tenkan, rsi_14, senkou_a, senkou_b
        FROM signals.indicators_1h FINAL
        WHERE symbol IN ({sym_list})
          AND {indicator_filter}
    """)

    if not rows:
        log.warning("evaluate_strategy_intraday: no 1h indicator rows for today")
        return []

    # Kijun flatness: range of Kijun over last 3 bars as % of Kijun.
    # Flat Kijun (< 0.3%) is a price magnet — key filter for scalp setups.
    if target_dt:
        kijun_flat_filter = f"datetime <= '{target_dt}'"
    else:
        kijun_flat_filter = f"toDate(datetime, 'America/New_York') = toDate(now(), 'America/New_York')"
    kijun_flat_rows = ch_query(f"""
        SELECT symbol,
               (max(kijun) - min(kijun)) / nullIf(avg(kijun), 0) AS kijun_range_pct
        FROM (
            SELECT symbol, kijun,
                   row_number() OVER (PARTITION BY symbol ORDER BY datetime DESC) AS rn
            FROM signals.indicators_1h FINAL
            WHERE symbol IN ({sym_list}) AND {kijun_flat_filter}
        )
        WHERE rn <= 3
        GROUP BY symbol
    """)
    kijun_flat_map = {r[0]: float(r[1] or 1.0) for r in kijun_flat_rows}

    # Previous decision per symbol (most recent row before now)
    prev_rows = ch_query(f"""
        SELECT symbol, decision
        FROM signals.strategy_1h FINAL
        WHERE symbol IN ({sym_list})
        ORDER BY symbol ASC, datetime DESC
        LIMIT 1 BY symbol
    """)
    prev_map = {r[0]: r[1] for r in prev_rows}

    # Previous bar's RSI zone — needed for scalp detection (RSI bottoms in bar N,
    # tenkan reclaim happens in bar N+1 when RSI has already recovered to neutral).
    if target_dt:
        prev_rsi_filter = f"datetime < '{target_dt}'"
    else:
        prev_rsi_filter = f"toDate(datetime, 'America/New_York') = toDate(now(), 'America/New_York')"
    prev_rsi_rows = ch_query(f"""
        SELECT symbol, rsi_zone
        FROM signals.indicators_1h FINAL
        WHERE symbol IN ({sym_list}) AND {prev_rsi_filter}
        ORDER BY symbol ASC, datetime DESC
        LIMIT 1 BY symbol
    """)
    prev_rsi_map = {r[0]: r[1] for r in prev_rsi_rows}

    strategy_rows = []
    transitions   = []

    for r in rows:
        (symbol, signal_score, rsi_zone, price_vs_cloud,
         tk_cross, vol_ratio, obv_trend, vol_signal, close, dt_str,
         kijun, cloud_color, tenkan, rsi_14, senkou_a, senkou_b) = r

        signal_score   = int(signal_score)
        vol_ratio      = float(vol_ratio)
        close_f        = float(close)
        kijun_f        = float(kijun)
        cloud_top_f    = max(float(senkou_a), float(senkou_b))
        tenkan_f       = float(tenkan) if tenkan else close_f
        rsi_f          = float(rsi_14) if rsi_14 else 50.0
        prev_decision  = prev_map.get(symbol, "")
        prev_rsi_zone  = prev_rsi_map.get(symbol, "")
        rsi_was_oversold   = rsi_zone == "oversold" or prev_rsi_zone == "oversold"
        rsi_was_overbought = rsi_zone == "overbought" or prev_rsi_zone == "overbought"
        kijun_flat    = kijun_flat_map.get(symbol, 1.0) < 0.008
        reasons       = []

        if price_vs_cloud == "above":
            reasons.append("above_cloud")
        elif price_vs_cloud == "below":
            reasons.append("below_cloud")

        if tk_cross in ("bullish_cross", "bullish"):
            reasons.append(f"tk_{tk_cross}")
        elif tk_cross in ("bearish_cross", "bearish"):
            reasons.append(f"tk_{tk_cross}")

        if vol_signal != "neutral":
            reasons.append(f"{vol_signal} (vol {vol_ratio:.1f}x avg)")

        if obv_trend == "rising":
            reasons.append("obv_rising")
        elif obv_trend == "falling":
            reasons.append("obv_falling")

        total = signal_score + VOL_SCORE.get(vol_signal, 0)

        # Cloud retest: price pulled back into/below cloud but Kijun held and
        # TK structure remains bullish in a green cloud — accumulation zone.
        cloud_retest = (
            price_vs_cloud in ("below", "inside")
            and cloud_color == "green"
            and tk_cross in ("bullish", "bullish_cross")
            and close_f >= kijun_f
            and total >= 1
        )

        # Cloud reclaim: price jumped back above cloud after a retest WATCH —
        # lower BUY threshold since Kijun support was already confirmed.
        cloud_reclaim = (
            price_vs_cloud == "above"
            and prev_decision == "WATCH"
            and tk_cross in ("bullish", "bullish_cross")
            and vol_signal == "accumulation"
            and total >= 3
            and rsi_zone != "overbought"
        )

        if cloud_retest:
            reasons.append("cloud_retest")
        if cloud_reclaim:
            reasons.append("cloud_reclaim")

        kijun_proximity = abs(kijun_f - close_f) / close_f < 0.015
        scalp_long = (
            rsi_was_oversold
            and price_vs_cloud == "below"
            and close_f >= tenkan_f
            and kijun_f > close_f
            and kijun_proximity
            and kijun_flat
        )
        scalp_short = (
            rsi_was_overbought
            and price_vs_cloud == "above"
            and close_f <= tenkan_f
            and kijun_f < close_f
            and kijun_proximity
            and kijun_flat
        )

        if scalp_long:
            reasons.append(f"scalp_long (RSI {rsi_f:.0f}, kijun target {kijun_f:.2f})")
        if scalp_short:
            reasons.append(f"scalp_short (RSI {rsi_f:.0f}, kijun target {kijun_f:.2f})")

        exit_cond = (
            (
                total <= -5
                or (total <= -3 and price_vs_cloud == "below" and vol_signal == "distribution")
            )
            and rsi_zone != "oversold"
        )
        buy_cond = (
            (
                total >= 5
                and vol_ratio > 1.2
                and rsi_zone != "overbought"
                and price_vs_cloud == "above"
            )
            or cloud_reclaim
        )

        if scalp_long:
            decision = "SCALP_LONG_CAUTION"
        elif scalp_short:
            decision = "SCALP_SHORT_CAUTION"
        elif exit_cond:
            decision = "EXIT"
        elif buy_cond:
            decision = "BUY"
        elif (total >= 4 and price_vs_cloud == "above") or cloud_retest:
            decision = "WATCH"
        else:
            decision = "HOLD"
        signal_id = hashlib.sha256(
            f"{symbol}:{dt_str}:{decision}".encode()
        ).hexdigest()[:6]
        strategy_rows.append([symbol, dt_str, decision, total, reasons, prev_decision, signal_id])

        if decision != prev_decision and decision in ("BUY", "EXIT", "SCALP_LONG_CAUTION", "SCALP_SHORT_CAUTION"):
            transitions.append({
                "symbol":    symbol,
                "decision":  decision,
                "prev":      prev_decision,
                "score":     total,
                "reasons":   reasons,
                "close":     float(close),
                "rsi_zone":  rsi_zone,
                "date":      dt_str,
                "kijun":     kijun_f,
                "cloud_top": cloud_top_f,
                "rsi":       rsi_f,
                "signal_id": signal_id,
            })

    ch_insert("signals.strategy_1h",
              ["symbol", "datetime", "decision", "score", "reasons", "prev_decision", "signal_id"],
              strategy_rows)

    if alert:
        for row in strategy_rows:
            sym, dt, dec, score, rsns, _, _sid = row
            log.info("  %-8s  %-5s  score=%+d  %s  [%s ET]",
                     sym, dec, score, " | ".join(rsns[:3]), dt[11:16])
        log.info("strategy_1h: wrote %d rows", len(strategy_rows))

    return transitions


def _compute_signals(symbols: list[str], end_date: date | None = None,
                     start_date: date | None = None):
    """Compute Ichimoku + RSI + volume signals, optionally scoped to a date range."""
    crypto_rows    = ch_query("SELECT DISTINCT symbol FROM portfolio.crypto_positions")
    crypto_symbols = {r[0] for r in crypto_rows}

    columns = [
        "symbol", "date", "close",
        "rsi_14", "rsi_zone",
        "tenkan", "kijun", "senkou_a", "senkou_b",
        "cloud_color", "price_vs_cloud", "tk_cross",
        "signal", "signal_score",
        "vol_ratio", "obv_trend", "vol_signal",
    ]

    total = 0
    for symbol in symbols:
        yf_ticker = f"{symbol}-USD" if symbol in crypto_symbols else symbol
        records   = compute_signals.compute_for_symbol(symbol, yf_ticker,
                                                       end_date=end_date,
                                                       start_date=start_date)
        if not records:
            continue

        # Sanity-check: reject any record whose close deviates > 20% from the
        # prices table for that date. Catches yfinance data contamination (e.g.
        # AMD showing BTC-level prices) before it corrupts the indicators table.
        price_rows = ch_query(f"""
            SELECT date, close FROM portfolio.prices FINAL
            WHERE symbol = '{symbol}'
              AND date >= '{records[0]["date"]}' AND date <= '{records[-1]["date"]}'
        """)
        price_map = {str(r[0]): float(r[1]) for r in price_rows}
        clean = []
        for rec in records:
            ref = price_map.get(str(rec["date"]))
            if ref and ref > 0:
                ratio = abs(float(rec["close"]) - ref) / ref
                if ratio > 0.20:
                    log.warning("signals sanity: %s %s close=%.2f vs prices=%.2f (%.0f%%) — skipped",
                                symbol, rec["date"], rec["close"], ref, ratio * 100)
                    continue
            clean.append(rec)
        if not clean:
            continue
        rows = [[r[c] for c in columns] for r in clean]
        ch_insert("signals.indicators", columns, rows)
        total += len(rows)
        latest = records[-1]
        log.debug("  signals %-8s  %s  score=%+d  vol=%.1fx [%s]",
                  symbol, latest["signal"].upper(), latest["signal_score"],
                  latest["vol_ratio"], latest["vol_signal"])

    log.info("signals: inserted %d rows", total)

# ---------------------------------------------------------------------------
# Scheduled jobs
# ---------------------------------------------------------------------------

def _format_ingest_slack(output: str) -> str:
    """Format email-ingest slog output into a clean Slack notification."""
    import re
    lines = []
    skipped = 0
    for line in output.splitlines():
        m = re.search(
            r"INFO parsed.*?code=(\S+)\s+symbol=(\S+)\s+qty=(\S+)\s+price=(\S+)", line
        )
        if m:
            code, symbol, qty, price = m.groups()
            lines.append(f"• {code} {symbol} ×{qty} @ ${float(price):.2f}")
        elif "skipped as non-trade" in line:
            skipped += 1
    header = f"📥 *{len(lines)} order{'s' if len(lines) != 1 else ''} ingested*"
    parts = [header] + lines
    if skipped:
        parts.append(f"_{skipped} non-trade email{'s' if skipped != 1 else ''} skipped_")
    return "\n".join(parts)


def job_email_ingest():
    """Poll Gmail for unseen Robinhood order emails and ingest into ClickHouse."""
    binary = os.path.join(REPO_DIR, "bin", "email-ingest")
    if not os.path.exists(binary):
        log.error("job_email_ingest: binary not found at %s — run 'make build-email'", binary)
        return
    log.info("=== job_email_ingest start ===")
    try:
        result = subprocess.run([binary], capture_output=True, text=True, timeout=120)
        stdout = result.stdout.strip()
        stderr = result.stderr.strip()
        output = "\n".join(filter(None, [stdout, stderr]))
        log.info("email-ingest exit=%d", result.returncode)
        if stdout:
            log.info("email-ingest stdout: %s", stdout)
        if stderr:
            log.info("email-ingest stderr: %s", stderr)
        if result.returncode != 0:
            log.error("email-ingest failed (exit %d)", result.returncode)
        elif "inserted" in output:
            # New transaction landed — fetch prices immediately so the dashboard is current.
            log.info("email-ingest: new transaction(s) inserted — refreshing prices")
            try:
                _ingest_prices(all_symbols())
            except Exception as e:
                log.error("email-ingest: price refresh failed — %s", e)
            if SLACK_INGEST_WEBHOOK_URL:
                try:
                    _slack_post(SLACK_INGEST_WEBHOOK_URL,
                                _format_ingest_slack(output))
                except Exception as e:
                    log.error("job_email_ingest: Slack post failed — %s", e)
    except Exception as e:
        log.error("job_email_ingest exception: %s", e)
    log.info("=== job_email_ingest done ===")


def job_gen_insights():
    """Generate LLM trading insights for the current month via Claude API."""
    script = os.path.join(REPO_DIR, "scripts", "gen_insights.py")
    log.info("=== job_gen_insights start ===")
    try:
        result = subprocess.run([VENV_PYTHON, script], capture_output=True, text=True, timeout=180)
        if result.stdout.strip():
            log.info("gen_insights: %s", result.stdout.strip())
        if result.returncode != 0:
            log.error("gen_insights exited %d: %s", result.returncode, result.stderr.strip())
    except Exception as e:
        log.error("job_gen_insights: %s", e)
    log.info("=== job_gen_insights done ===")


def job_backfill_intraday():
    """Populate signals.indicators_1h and signals.strategy_1h for the last 60 days.

    yfinance provides up to 60 calendar days of 1h data. Evaluates strategy for
    every bar in chronological order so prev_decision tracking is accurate.
    """
    symbols = all_symbols()
    log.info("=== backfill_intraday — %d symbols ===", len(symbols))

    log.info("step 1: compute 1h signals (last 60 days)")
    try:
        _compute_signals_intraday(symbols)
    except Exception as e:
        log.error("compute_signals_intraday failed — %s", e)
        return

    # All distinct bar timestamps in chronological order
    sym_list = ", ".join(f"'{s}'" for s in symbols)
    dt_rows = ch_query(f"""
        SELECT DISTINCT datetime FROM signals.indicators_1h FINAL
        WHERE symbol IN ({sym_list})
        ORDER BY datetime ASC
    """)

    if not dt_rows:
        log.warning("backfill_intraday: no 1h bars found after compute step")
        return

    log.info("step 2: evaluate strategy for %d bars", len(dt_rows))
    total_transitions = 0
    for (dt_str,) in dt_rows:
        try:
            t = evaluate_strategy_intraday(symbols, alert=False, target_dt=dt_str)
            total_transitions += len(t or [])
        except Exception as e:
            log.error("evaluate_strategy_intraday %s failed — %s", dt_str, e)

    log.info("=== backfill_intraday done — %d transitions across %d bars ===",
             total_transitions, len(dt_rows))


def job_intraday():
    """Run the intraday signal pipeline for the current 1h bar.

    Scheduled at :30 past each hour from 09:30 to 15:30 ET — right after each NYSE hourly
    bar closes. Computes Ichimoku/RSI/volume on 1h candles and evaluates the same
    BUY/WATCH/HOLD/EXIT strategy. Alerts only on transitions.

    Also refreshes daily prices + signals so the daily dashboard stays current
    throughout the trading day, not just after the 16:30 close run.
    """
    if not is_trading_day():
        log.info("job_intraday: skipping — not a trading day")
        return

    log.info("=== job_intraday start ===")
    symbols = all_symbols()

    # Refresh daily prices + signals with latest available bars
    try:
        _ingest_prices(symbols, end_date=date.today())
        _compute_signals(symbols, end_date=date.today(), start_date=date.today())
    except Exception as e:
        log.error("job_intraday: daily refresh failed — %s", e)

    try:
        _compute_signals_intraday(symbols)
    except Exception as e:
        log.error("job_intraday: compute_signals_intraday failed — %s", e)
        return

    try:
        transitions = evaluate_strategy_intraday(symbols, alert=True)
    except Exception as e:
        log.error("job_intraday: evaluate_strategy_intraday failed — %s", e)
        transitions = []

    if transitions:
        # Alert on: open options positions (time-sensitive) + explicit watchlist symbols.
        # Crypto is handled separately by job_crypto_intraday.
        opt_syms    = _option_symbols()
        crypto_rows = ch_query("SELECT DISTINCT symbol FROM portfolio.crypto_positions")
        crypto_syms = {r[0] for r in crypto_rows}
        alert_syms  = opt_syms | WATCHLIST
        opt_transitions = [t for t in transitions
                           if t["symbol"] in alert_syms and t["symbol"] not in crypto_syms]
        if opt_transitions:
            send_alert(opt_transitions, webhook_url=SLACK_OPTIONS_WEBHOOK_URL or SLACK_WEBHOOK_URL)

    log.info("=== job_intraday done ===")


def job_crypto_intraday():
    """Run the 1h signal pipeline for crypto symbols only — no NYSE gate, runs 24/7.

    Scheduled at :30 every hour so crypto positions get evaluated outside market hours
    and on weekends when job_intraday is skipped.
    """
    crypto_rows = ch_query("SELECT DISTINCT symbol FROM portfolio.crypto_positions")
    crypto_symbols = [r[0] for r in crypto_rows]
    if not crypto_symbols:
        log.info("job_crypto_intraday: no crypto positions — skipping")
        return

    log.info("=== job_crypto_intraday start === symbols=%s", crypto_symbols)

    try:
        _compute_signals_intraday(crypto_symbols)
    except Exception as e:
        log.error("job_crypto_intraday: compute_signals_intraday failed — %s", e)
        return

    try:
        transitions = evaluate_strategy_intraday(crypto_symbols, alert=True)
    except Exception as e:
        log.error("job_crypto_intraday: evaluate_strategy_intraday failed — %s", e)
        transitions = []

    if transitions:
        send_alert(transitions, webhook_url=SLACK_CRYPTO_WEBHOOK_URL or SLACK_WEBHOOK_URL)

    log.info("=== job_crypto_intraday done ===")


def job_news():
    if not is_trading_day():
        log.info("job_news: skipping — not a trading day")
        return
    log.info("=== job_news start ===")
    try:
        newsfeed.cmd_ingest()
    except Exception as e:
        log.error("job_news: ingest failed — %s", e)
    try:
        newsfeed.cmd_mentions()
    except Exception as e:
        log.error("job_news: mentions failed — %s", e)
    log.info("=== job_news done ===")


def job_close(target_date: date | None = None, alert: bool = True):
    """Run the full post-close pipeline for target_date (default: today)."""
    td = target_date or date.today()
    if not is_trading_day(td):
        log.info("job_close: skipping %s — not a trading day", td)
        return

    log.info("=== job_close %s ===", td)
    symbols = all_symbols()
    log.info("symbols (%d): %s", len(symbols), ", ".join(symbols))

    log.info("step 1/4: ingest prices")
    try:
        _ingest_prices(symbols, end_date=td)
    except Exception as e:
        log.error("ingest_prices failed — %s", e)

    log.info("step 2/4: compute signals")
    try:
        _compute_signals(symbols, end_date=td, start_date=td)
    except Exception as e:
        log.error("compute_signals failed — %s", e)

    log.info("step 3/4: evaluate strategy")
    try:
        transitions = evaluate_strategy(symbols, target_date=td, alert=alert)
    except Exception as e:
        log.error("evaluate_strategy failed — %s", e)
        transitions = []

    if alert:
        # Daily close signals go to the main channel — authoritative for all position types
        send_alert(transitions or [], webhook_url=SLACK_WEBHOOK_URL)
        log.info("step 4/4: transitions=%d", len(transitions or []))
    else:
        log.info("step 4/4: alert suppressed (backfill)")
        if transitions:
            for t in transitions:
                log.info("  [%s] %s score=%+d  %s", t["decision"], t["symbol"],
                         t["score"], ", ".join(t["reasons"][:3]))

    # Compute outcomes for any past decisions that now have enough forward data
    log.info("step 5: compute outcomes")
    try:
        compute_outcomes.main_from_args(symbol=None, from_date=str(td - timedelta(days=30)))
    except Exception as e:
        log.error("compute_outcomes failed — %s", e)

    log.info("=== job_close %s done ===", td)


def job_backfill(start: date, end: date):
    """Run the full pipeline for every NYSE trading day in [start, end]."""
    days = trading_days_in_range(start, end)
    if not days:
        log.warning("backfill: no trading days in %s → %s", start, end)
        return

    log.info("=== backfill %s → %s (%d trading days) ===", start, end, len(days))
    symbols = all_symbols()
    log.info("symbols (%d): %s", len(symbols), ", ".join(symbols))

    # Ingest prices once for the full range (efficient — one yfinance call per symbol)
    log.info("step 1: ingest prices up to %s", end)
    try:
        _ingest_prices(symbols, end_date=end)
    except Exception as e:
        log.error("ingest_prices failed — %s", e)

    # Compute signals once per symbol for the full range
    log.info("step 2: compute signals for %s → %s", start, end)
    try:
        # start_date intentionally omitted — Ichimoku needs 52+ bars of history to
        # compute correctly. Passing a narrow start_date starves the calculation.
        # end_date caps the output to the requested range.
        _compute_signals(symbols, end_date=end)
    except Exception as e:
        log.error("compute_signals failed — %s", e)

    # Evaluate strategy for each trading day (no alerts)
    log.info("step 3: evaluate strategy for each day")
    all_transitions = []

    for d in days:
        try:
            t = evaluate_strategy(symbols, target_date=d, alert=False)
            all_transitions.extend(t or [])
        except Exception as e:
            log.error("evaluate_strategy %s failed — %s", d, e)

    # Compute outcomes for all backfilled decisions
    log.info("step 4: compute outcomes")
    try:
        compute_outcomes.main_from_args(from_date=str(start))
    except Exception as e:
        log.error("compute_outcomes failed — %s", e)

    log.info("=== backfill done — %d transitions across %d days ===",
             len(all_transitions), len(days))

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def _parse_date(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid date '{s}' — use YYYY-MM-DD")


def main():
    parser = argparse.ArgumentParser(description="Portfolio signal daemon")
    group  = parser.add_mutually_exclusive_group()
    group.add_argument("--now", action="store_true",
                       help="Run today's post-close pipeline immediately then exit")
    group.add_argument("--backfill", nargs="+", metavar="DATE",
                       help="Backfill: --backfill START [END]  (YYYY-MM-DD)")
    group.add_argument("--backfill-intraday", action="store_true",
                       help="Backfill 60 days of 1h intraday signals into signals.indicators_1h / strategy_1h")
    args = parser.parse_args()

    if args.now:
        log.info("--now: running job_close for today")
        job_close()
        return

    if args.backfill_intraday:
        job_backfill_intraday()
        return

    if args.backfill:
        if len(args.backfill) == 1:
            start = end = _parse_date(args.backfill[0])
        elif len(args.backfill) == 2:
            start = _parse_date(args.backfill[0])
            end   = _parse_date(args.backfill[1])
        else:
            parser.error("--backfill takes 1 or 2 dates")
        job_backfill(start, end)
        return

    # Daemon mode — run time-sensitive jobs immediately on startup so a deploy
    # doesn't cause a missed window while waiting for the first scheduled fire.
    log.info("startup: running initial jobs before scheduler")
    job_email_ingest()
    job_news()
    job_intraday()
    job_crypto_intraday()

    scheduler = BlockingScheduler(timezone="America/New_York")

    scheduler.add_job(job_news, CronTrigger(
        minute="*/30",
        timezone="America/New_York",
    ), id="job_news", name="News ingest")

    # Intraday: runs at :30 each hour from 09:30 to 15:30 ET.
    # Covers bar closes: 09:30, 10:30, 11:30, 12:30, 13:30, 14:30, 15:30.
    scheduler.add_job(job_intraday, CronTrigger(
        day_of_week="mon-fri", hour="9-15", minute=30,
        timezone="America/New_York",
    ), id="job_intraday", name="Intraday signals")

    # Crypto intraday: runs at :30 every hour, 24/7 — no NYSE gate.
    # job_intraday already covers crypto during market hours; this catches
    # evenings, nights, weekends, and holidays.
    scheduler.add_job(job_crypto_intraday, CronTrigger(
        minute=30,
    ), id="job_crypto_intraday", name="Crypto 24/7 intraday")

    scheduler.add_job(job_close, CronTrigger(
        day_of_week="mon-fri", hour=16, minute=30,
        timezone="America/New_York",
    ), id="job_close", name="Post-close pipeline")

    # Email ingest: poll Gmail every 5min, every day
    scheduler.add_job(job_email_ingest, CronTrigger(
        minute="*/5",
        timezone="America/New_York",
    ), id="job_email_ingest", name="Email ingest")

    # gen_insights disabled — requires CCR_BASE_URL (Claude API proxy) not available in container
    # scheduler.add_job(job_gen_insights, CronTrigger(
    #     day_of_week="mon", hour=7, minute=0,
    #     timezone="America/New_York",
    # ), id="job_gen_insights", name="Generate insights")

    log.info("trader daemon starting — job_news@*/30, job_intraday@:30(09:30-15:30), "
             "job_close@16:30, job_email_ingest@*/5, "
             "job_crypto_intraday@:30(24/7)")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("trader daemon stopped")


if __name__ == "__main__":
    main()
