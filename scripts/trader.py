#!/usr/bin/env python3
"""
trader.py — Portfolio signal daemon.

Runs on a market-aware schedule (NYSE calendar):
  09:00 ET        →  ingest news + mentions (sentiment signals)
  09:30–15:30 ET  →  intraday Ichimoku/RSI signals on 1h bars (every :30)
  16:30 ET        →  ingest prices → compute signals → evaluate strategy → alert

Strategy evaluation combines Ichimoku + RSI + divergence + volume signals
into BUY / WATCH / HOLD / EXIT decisions, alerting only on transitions.

Usage:
    python scripts/trader.py                              # daemon
    python scripts/trader.py --now                        # run today's pipeline immediately
    python scripts/trader.py --backfill 2026-02-24        # single day
    python scripts/trader.py --backfill 2026-02-24 2026-03-07  # date range
"""

import os
import sys
import json
import smtplib
import logging
import argparse
import urllib.error
import urllib.request
import urllib.parse
from datetime import date, timedelta
from email.mime.text import MIMEText

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

WATCHLIST_ENV = os.environ.get("WATCHLIST", "").strip()
WATCHLIST     = {s.strip().upper() for s in WATCHLIST_ENV.split(",") if s.strip()} if WATCHLIST_ENV else set()

EMAIL_SMTP = os.environ.get("EMAIL_SMTP_ADDR", "smtp.gmail.com:587")
EMAIL_FROM = os.environ.get("EMAIL_USER", "")
EMAIL_PASS = os.environ.get("MAIL_APP_PASSWORD", "")
EMAIL_TO   = os.environ.get("ALERT_EMAIL", EMAIL_FROM)

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
            rsi_divergence, vol_ratio, obv_trend, vol_signal, close
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
         tk_cross, rsi_div, vol_ratio, obv_trend, vol_signal, close) = r

        signal_score = int(signal_score)
        vol_ratio    = float(vol_ratio)
        reasons      = []

        if price_vs_cloud == "above":
            reasons.append("above_cloud")
        elif price_vs_cloud == "below":
            reasons.append("below_cloud")

        if tk_cross in ("bullish_cross", "bullish"):
            reasons.append(f"tk_{tk_cross}")
        elif tk_cross in ("bearish_cross", "bearish"):
            reasons.append(f"tk_{tk_cross}")

        if rsi_div:
            reasons.append(rsi_div)

        if vol_signal != "neutral":
            reasons.append(f"{vol_signal} (vol {vol_ratio:.1f}x avg)")

        if obv_trend == "rising":
            reasons.append("obv_rising")
        elif obv_trend == "falling":
            reasons.append("obv_falling")

        total = signal_score + VOL_SCORE.get(vol_signal, 0)

        # Ichimoku score drives EXIT/BUY thresholds.
        # RSI zone and divergence type are context guards — they don't affect the score,
        # but they gate conditions: oversold = don't exit (bottom risk), overbought+bearish_div = exit.
        exit_cond = (
            (
                # Ichimoku fully bearish — but not during a bounce (hidden_bear = near-term up)
                (total <= -5 and rsi_div != "hidden_bear")
                # Exhaustion at a confirmed top: overbought + reversal divergence
                or (rsi_zone == "overbought" and rsi_div == "bearish_div")
                # Confirmed distribution below cloud — requires at least partial bearish structure
                or (total <= -3 and price_vs_cloud == "below" and vol_signal == "distribution")
            )
            and rsi_zone != "oversold"  # never exit when already oversold — highest bounce risk
        )
        buy_cond = (
            total >= 5
            and vol_ratio > 1.2
            and rsi_zone != "overbought"
            and price_vs_cloud == "above"
        )

        if exit_cond:
            decision = "EXIT"
        elif buy_cond:
            decision = "BUY"
        elif total >= 4 and price_vs_cloud == "above":  # score 4+ above cloud = genuine momentum
            decision = "WATCH"
        else:
            decision = "HOLD"

        prev_decision = prev_map.get(symbol, "")

        strategy_rows.append([symbol, today_s, decision, total, reasons, prev_decision])

        if decision != prev_decision and decision in ("BUY", "EXIT"):
            transitions.append({
                "symbol":   symbol,
                "decision": decision,
                "prev":     prev_decision,
                "score":    total,
                "reasons":  reasons,
                "close":    float(close),
                "rsi_zone": rsi_zone,
                "rsi_div":  rsi_div,
                "date":     today_s,
            })

    ch_insert("signals.strategy",
              ["symbol", "date", "decision", "score", "reasons", "prev_decision"],
              strategy_rows)

    # Only log per-symbol detail in live mode; backfill prints its own summary
    if alert:
        for row in strategy_rows:
            sym, _, dec, score, rsns, _ = row
            log.info("  %-8s  %-5s  score=%+d  %s", sym, dec, score,
                     " | ".join(rsns[:3]))
        log.info("strategy: wrote %d rows for %s", len(strategy_rows), today_s)

    return transitions

# ---------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------

def send_alert(transitions: list[dict]):
    if not transitions or not EMAIL_FROM or not EMAIL_PASS:
        if transitions:
            log.warning("alert: EMAIL_USER / MAIL_APP_PASSWORD not set — skipping email")
        return

    lines = []
    for t in transitions:
        label = t["decision"]
        lines.append(
            f"[{label}] {t['symbol']}  ${t['close']:.2f}  score={t['score']:+d}  ({t['date']})\n"
            f"  Why:   {', '.join(t['reasons'])}\n"
            f"  RSI:   {t['rsi_zone']}  div={t['rsi_div'] or 'none'}\n"
            f"  Prev:  {t['prev'] or 'none'}\n"
        )

    subject = " | ".join(f"[{t['decision']}] {t['symbol']}" for t in transitions)
    body    = "\n".join(lines)

    msg            = MIMEText(body)
    msg["From"]    = EMAIL_FROM
    msg["To"]      = EMAIL_TO
    msg["Subject"] = subject

    host, port = EMAIL_SMTP.rsplit(":", 1)
    try:
        with smtplib.SMTP(host, int(port), timeout=15) as s:
            s.starttls()
            s.login(EMAIL_FROM, EMAIL_PASS)
            s.sendmail(EMAIL_FROM, [EMAIL_TO], msg.as_string())
        log.info("alert: sent — %s", subject)
    except Exception as e:
        log.error("alert: email failed — %s", e)

# ---------------------------------------------------------------------------
# Price + signal ingestion (date-aware)
# ---------------------------------------------------------------------------

def _ingest_prices(symbols: list[str], end_date: date | None = None):
    """Fetch OHLCV up to end_date (default: today) and write to portfolio.prices."""
    import yfinance as yf

    crypto_rows    = ch_query("SELECT DISTINCT symbol FROM portfolio.crypto_positions")
    crypto_symbols = {r[0] for r in crypto_rows}

    end   = end_date or date.today()
    start = end - timedelta(days=365)

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
        "rsi_divergence", "signal", "signal_score",
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
                AND toDate(datetime, 'America/New_York') = today()
              GROUP BY symbol
          )"""

    rows = ch_query(f"""
        SELECT
            symbol, signal_score, rsi_zone, price_vs_cloud, tk_cross,
            rsi_divergence, vol_ratio, obv_trend, vol_signal, close, datetime
        FROM signals.indicators_1h FINAL
        WHERE symbol IN ({sym_list})
          AND {indicator_filter}
    """)

    if not rows:
        log.warning("evaluate_strategy_intraday: no 1h indicator rows for today")
        return []

    # Previous decision per symbol (most recent row before now)
    prev_rows = ch_query(f"""
        SELECT symbol, decision
        FROM signals.strategy_1h FINAL
        WHERE symbol IN ({sym_list})
        ORDER BY symbol ASC, datetime DESC
        LIMIT 1 BY symbol
    """)
    prev_map = {r[0]: r[1] for r in prev_rows}

    strategy_rows = []
    transitions   = []

    for r in rows:
        (symbol, signal_score, rsi_zone, price_vs_cloud,
         tk_cross, rsi_div, vol_ratio, obv_trend, vol_signal, close, dt_str) = r

        signal_score = int(signal_score)
        vol_ratio    = float(vol_ratio)
        reasons      = []

        if price_vs_cloud == "above":
            reasons.append("above_cloud")
        elif price_vs_cloud == "below":
            reasons.append("below_cloud")

        if tk_cross in ("bullish_cross", "bullish"):
            reasons.append(f"tk_{tk_cross}")
        elif tk_cross in ("bearish_cross", "bearish"):
            reasons.append(f"tk_{tk_cross}")

        if rsi_div:
            reasons.append(rsi_div)

        if vol_signal != "neutral":
            reasons.append(f"{vol_signal} (vol {vol_ratio:.1f}x avg)")

        if obv_trend == "rising":
            reasons.append("obv_rising")
        elif obv_trend == "falling":
            reasons.append("obv_falling")

        total = signal_score + VOL_SCORE.get(vol_signal, 0)

        # Same strategy logic as daily — Ichimoku drives score, RSI is context guard
        exit_cond = (
            (
                (total <= -5 and rsi_div != "hidden_bear")
                or (rsi_zone == "overbought" and rsi_div == "bearish_div")
                or (total <= -3 and price_vs_cloud == "below" and vol_signal == "distribution")
            )
            and rsi_zone != "oversold"
        )
        buy_cond = (
            total >= 5
            and vol_ratio > 1.2
            and rsi_zone != "overbought"
            and price_vs_cloud == "above"
        )

        if exit_cond:
            decision = "EXIT"
        elif buy_cond:
            decision = "BUY"
        elif total >= 4 and price_vs_cloud == "above":
            decision = "WATCH"
        else:
            decision = "HOLD"

        prev_decision = prev_map.get(symbol, "")
        strategy_rows.append([symbol, dt_str, decision, total, reasons, prev_decision])

        if decision != prev_decision and decision in ("BUY", "EXIT"):
            transitions.append({
                "symbol":   symbol,
                "decision": decision,
                "prev":     prev_decision,
                "score":    total,
                "reasons":  reasons,
                "close":    float(close),
                "rsi_zone": rsi_zone,
                "rsi_div":  rsi_div,
                "date":     dt_str,
            })

    ch_insert("signals.strategy_1h",
              ["symbol", "datetime", "decision", "score", "reasons", "prev_decision"],
              strategy_rows)

    if alert:
        for row in strategy_rows:
            sym, dt, dec, score, rsns, _ = row
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
        "rsi_divergence", "signal", "signal_score",
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
        rows = [[r[c] for c in columns] for r in records]
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
    """
    if not is_trading_day():
        log.info("job_intraday: skipping — not a trading day")
        return

    log.info("=== job_intraday start ===")
    symbols = all_symbols()

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
        send_alert(transitions)

    log.info("=== job_intraday done ===")


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
        log.info("step 4/4: alert (%d transitions)", len(transitions or []))
        send_alert(transitions or [])
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
        _compute_signals(symbols, end_date=end, start_date=start)
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

    # Daemon mode
    scheduler = BlockingScheduler(timezone="America/New_York")

    scheduler.add_job(job_news, CronTrigger(
        day_of_week="mon-fri", hour=9, minute=0,
        timezone="America/New_York",
    ), id="job_news", name="News ingest")

    # Intraday: runs at :30 each hour from 09:30 to 15:30 ET.
    # Covers bar closes: 09:30, 10:30, 11:30, 12:30, 13:30, 14:30, 15:30.
    scheduler.add_job(job_intraday, CronTrigger(
        day_of_week="mon-fri", hour="9-15", minute=30,
        timezone="America/New_York",
    ), id="job_intraday", name="Intraday signals")

    scheduler.add_job(job_close, CronTrigger(
        day_of_week="mon-fri", hour=16, minute=30,
        timezone="America/New_York",
    ), id="job_close", name="Post-close pipeline")

    log.info("trader daemon starting — job_news@09:00, job_intraday@:30(09:30-15:30), job_close@16:30 ET")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("trader daemon stopped")


if __name__ == "__main__":
    main()
