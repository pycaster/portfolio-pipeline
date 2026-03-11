#!/usr/bin/env python3
"""
Compute Ichimoku Cloud + RSI(14) signals for one or more symbols and store in signals.indicators.

Fetches 120 days of OHLCV from yfinance (enough for 52+26 Ichimoku warmup).
After warmup, writes ~42 valid rows per symbol. The table uses ReplacingMergeTree
so re-running for the same symbol overwrites existing rows.

Usage:
    make signals SYMBOLS="MSFT IREN"         # specific symbols
    make signals                              # all currently-held positions
    python scripts/compute_signals.py MSFT IREN
"""

import os
import sys
import json
import math
import urllib.request
import urllib.parse
from datetime import date, timedelta

try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
except ImportError as e:
    print(f"ERR: missing dependency — {e}. Run from ai-trader venv.")
    sys.exit(1)

LOOKBACK_DAYS   = 150  # fetch window — gives enough warmup for Ichimoku(52)+shift(26)
TENKAN_PERIOD   = 9
KIJUN_PERIOD    = 26
SENKOU_B_PERIOD = 52
DISPLACEMENT    = 26  # periods Senkou spans are plotted ahead
DIV_WINDOW      = 14  # bars to look back for RSI divergence (matches RSI period)


# ---------------------------------------------------------------------------
# ClickHouse helpers (same pattern as ingest_prices.py)
# ---------------------------------------------------------------------------

def ch_query(ch_http: str, auth: str, sql: str) -> list:
    url = f"http://{ch_http}/?{auth}&query={urllib.parse.quote(sql + ' FORMAT JSONCompact')}"
    with urllib.request.urlopen(url) as resp:
        data = json.loads(resp.read())
    return data.get("data", [])


def ch_insert(ch_http: str, auth: str, table: str, columns: list, rows: list):
    if not rows:
        return
    col_str = ", ".join(columns)
    lines = "\n".join(json.dumps(dict(zip(columns, row))) for row in rows)
    url = f"http://{ch_http}/?{auth}&query={urllib.parse.quote(f'INSERT INTO {table} ({col_str}) FORMAT JSONEachRow')}"
    data = lines.encode()
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req) as resp:
        resp.read()


# ---------------------------------------------------------------------------
# Indicators
# ---------------------------------------------------------------------------

def compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """RSI using Wilder's smoothing (exponential, alpha=1/period, adjust=False)."""
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def donchian_mid(high: pd.Series, low: pd.Series, period: int) -> pd.Series:
    """(Highest high + lowest low) / 2 over a rolling window."""
    return (high.rolling(period).max() + low.rolling(period).min()) / 2


def compute_ichimoku(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.DataFrame:
    """
    Returns a DataFrame with columns:
        tenkan, kijun, senkou_a, senkou_b, chikou
    senkou_a and senkou_b are already shift-adjusted back to current date index
    so iloc[-1] gives the cloud level around today.
    """
    tenkan = donchian_mid(high, low, TENKAN_PERIOD)
    kijun  = donchian_mid(high, low, KIJUN_PERIOD)

    # Senkou spans are computed D periods ago and plotted DISPLACEMENT ahead.
    # To read the cloud value AT date D, use .shift(-DISPLACEMENT) (shift back in time).
    senkou_a_raw = ((tenkan + kijun) / 2).shift(DISPLACEMENT)
    senkou_b_raw = donchian_mid(high, low, SENKOU_B_PERIOD).shift(DISPLACEMENT)

    return pd.DataFrame({
        "tenkan":   tenkan,
        "kijun":    kijun,
        "senkou_a": senkou_a_raw,
        "senkou_b": senkou_b_raw,
    }, index=close.index)


# ---------------------------------------------------------------------------
# Signal scoring
# ---------------------------------------------------------------------------

def score_signal(row: pd.Series, prev_row: pd.Series | None) -> tuple[int, str]:
    """
    Score = sum of component scores.  Range roughly -6 to +6.
    Returns (score, signal_str).
    """
    score = 0
    close     = row["close"]
    tenkan    = row["tenkan"]
    kijun     = row["kijun"]
    senkou_a  = row["senkou_a"]
    senkou_b  = row["senkou_b"]
    rsi       = row["rsi_14"]

    any_null = any(math.isnan(v) if v is not None else True
                   for v in [tenkan, kijun, senkou_a, senkou_b])

    if not any_null:
        cloud_top    = max(senkou_a, senkou_b)
        cloud_bottom = min(senkou_a, senkou_b)

        # Price vs cloud (±2)
        if close > cloud_top:
            score += 2
        elif close < cloud_bottom:
            score -= 2

        # Cloud color (±1)
        if senkou_a > senkou_b:
            score += 1
        elif senkou_a < senkou_b:
            score -= 1

        # Price vs kijun (±1)
        if close > kijun:
            score += 1
        elif close < kijun:
            score -= 1

        # TK cross / alignment (±1 or ±2)
        if prev_row is not None and not (
            math.isnan(float(prev_row["tenkan"])) if prev_row["tenkan"] is not None else True
        ):
            prev_t = float(prev_row["tenkan"])
            prev_k = float(prev_row["kijun"])
            bullish_cross = (prev_t <= prev_k) and (tenkan > kijun)
            bearish_cross = (prev_t >= prev_k) and (tenkan < kijun)
            if bullish_cross:
                score += 2
            elif bearish_cross:
                score -= 2
            elif tenkan > kijun:
                score += 1
            elif tenkan < kijun:
                score -= 1

    # RSI dampening at extremes
    if rsi is not None and not math.isnan(rsi):
        if rsi > 70:
            score -= 1   # overbought dampens bullish signal
        elif rsi < 30:
            score += 1   # oversold dampens bearish signal

    if score >= 4:
        signal = "bullish"
    elif score <= -4:
        signal = "bearish"
    else:
        signal = "mixed"

    return score, signal


def compute_divergence(close: pd.Series, rsi: pd.Series, window: int = DIV_WINDOW) -> pd.Series:
    """
    Detect RSI divergence for each bar by comparing current close/RSI to the
    price extremes in the prior `window` bars.

    Returns a Series of strings:
        'bullish_div'  — regular bullish: price lower low + RSI higher low  (reversal)
        'hidden_bull'  — hidden bullish:  price higher low + RSI lower low  (uptrend continuation)
        'bearish_div'  — regular bearish: price higher high + RSI lower high (reversal)
        'hidden_bear'  — hidden bearish:  price lower high + RSI higher high (downtrend continuation)
        ''             — no divergence detected
    """
    result = pd.Series('', index=close.index, dtype=object)
    PRICE_THRESH = 0.005  # minimum 0.5% price gap (filters micro-noise)
    RSI_THRESH   = 3.0    # minimum 3 RSI-point gap

    for i in range(window, len(close)):
        c_now = float(close.iloc[i])
        r_now = rsi.iloc[i]
        if pd.isna(r_now):
            continue
        r_now = float(r_now)

        c_win = close.iloc[i - window:i]
        r_win = rsi.iloc[i - window:i]

        # ---- Bullish divergence (compare vs price low in window) ----
        min_pos  = int(c_win.values.argmin())
        c_low    = float(c_win.iloc[min_pos])
        r_at_low = float(r_win.iloc[min_pos])
        if not pd.isna(r_at_low):
            pdiff = abs(c_now - c_low) / max(c_low, 1e-9)
            if pdiff >= PRICE_THRESH and abs(r_now - r_at_low) >= RSI_THRESH:
                if c_now < c_low and r_now > r_at_low:
                    result.iloc[i] = 'bullish_div'   # price LL, RSI HL → reversal
                elif c_now > c_low and r_now < r_at_low:
                    result.iloc[i] = 'hidden_bull'   # price HL, RSI LL → continuation

        # ---- Bearish divergence (compare vs price high in window) ----
        if result.iloc[i] == '':
            max_pos   = int(c_win.values.argmax())
            c_high    = float(c_win.iloc[max_pos])
            r_at_high = float(r_win.iloc[max_pos])
            if not pd.isna(r_at_high):
                pdiff = abs(c_now - c_high) / max(c_high, 1e-9)
                if pdiff >= PRICE_THRESH and abs(r_now - r_at_high) >= RSI_THRESH:
                    if c_now > c_high and r_now < r_at_high:
                        result.iloc[i] = 'bearish_div'   # price HH, RSI LH → reversal
                    elif c_now < c_high and r_now > r_at_high:
                        result.iloc[i] = 'hidden_bear'   # price LH, RSI HH → continuation

    return result


def _safe(val) -> float | None:
    """Convert numpy scalar to Python float, or None if NaN."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if math.isnan(f) else round(f, 6)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Per-symbol compute
# ---------------------------------------------------------------------------

def compute_for_symbol(symbol: str, yf_ticker: str | None = None,
                       end_date: date | None = None,
                       start_date: date | None = None) -> list[dict]:
    """Compute signals for `symbol`.

    end_date   — last bar to include (defaults to today). Pass a past date for backtesting.
    start_date — earliest bar to include in results (signals before this are still computed
                 for warmup but not returned). Defaults to end_date - LOOKBACK_DAYS.
    """
    if yf_ticker is None:
        yf_ticker = symbol
    end   = end_date or date.today()
    # When backfilling, anchor warmup to start_date so indicators exist from day 1.
    # Otherwise anchor to end_date (live/recent mode).
    fetch_from = (start_date - timedelta(days=LOOKBACK_DAYS)) if start_date else (end - timedelta(days=LOOKBACK_DAYS))
    start = fetch_from

    try:
        # yfinance `end` is exclusive — add 1 day so we include end_date's bar
        df = yf.download(yf_ticker, start=str(start), end=str(end + timedelta(days=1)),
                         progress=False, auto_adjust=True)
    except Exception as e:
        print(f"  ERR   {symbol}: yfinance download failed — {e}", file=sys.stderr)
        return []

    if df.empty:
        print(f"  WARN  {yf_ticker}: no price data returned")
        return []

    # Flatten MultiIndex columns
    if hasattr(df.columns, "get_level_values"):
        df.columns = df.columns.get_level_values(0)
    df.columns = [c.lower() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]  # guard against multi-ticker column bleed
    df = df.dropna(subset=["open", "high", "low", "close", "volume"])

    if len(df) < SENKOU_B_PERIOD + DISPLACEMENT:
        print(f"  WARN  {symbol}: only {len(df)} bars — need {SENKOU_B_PERIOD + DISPLACEMENT} for full Ichimoku")
        return []

    close  = df["close"]
    high   = df["high"]
    low    = df["low"]
    volume = df["volume"]

    rsi  = compute_rsi(close)
    ichi = compute_ichimoku(high, low, close)

    # Volume signals
    vol_avg    = volume.rolling(20).mean()
    vol_ratio  = (volume / vol_avg.replace(0, np.nan)).fillna(0)

    # OBV: cumulative sum of signed volume
    direction = np.sign(close.diff().fillna(0))
    obv       = (direction * volume).cumsum()
    # OBV trend: slope over last 10 bars (positive=rising, negative=falling)
    obv_slope = obv.diff(10)

    def _vol_signal(i: int) -> str:
        ratio = float(vol_ratio.iloc[i])
        if ratio < 1.3:
            return "neutral"
        price_up = float(close.iloc[i]) > float(close.iloc[i - 1]) if i > 0 else False
        return "accumulation" if price_up else "distribution"

    def _obv_trend(i: int) -> str:
        slope = float(obv_slope.iloc[i])
        if abs(slope) < 1:
            return "flat"
        return "rising" if slope > 0 else "falling"

    results = []
    prev_row = None

    for i, (ts, _) in enumerate(df.iterrows()):
        r_close   = close.iloc[i]
        r_rsi     = _safe(rsi.iloc[i])
        r_tenkan  = _safe(ichi["tenkan"].iloc[i])
        r_kijun   = _safe(ichi["kijun"].iloc[i])
        r_sa      = _safe(ichi["senkou_a"].iloc[i])
        r_sb      = _safe(ichi["senkou_b"].iloc[i])

        # Skip rows where core indicators aren't warm yet
        if any(v is None for v in [r_tenkan, r_kijun, r_sa, r_sb]):
            prev_row = None
            continue

        # Derived categoricals
        rsi_zone = "neutral"
        if r_rsi is not None:
            if r_rsi > 70:
                rsi_zone = "overbought"
            elif r_rsi < 30:
                rsi_zone = "oversold"

        cloud_top    = max(r_sa, r_sb)
        cloud_bottom = min(r_sa, r_sb)
        cloud_color  = "green" if r_sa >= r_sb else "red"

        if r_close > cloud_top:
            price_vs_cloud = "above"
        elif r_close < cloud_bottom:
            price_vs_cloud = "below"
        else:
            price_vs_cloud = "inside"

        tk_cross = "neutral"
        if prev_row is not None and prev_row["tenkan"] is not None and prev_row["kijun"] is not None:
            pt = float(prev_row["tenkan"])
            pk = float(prev_row["kijun"])
            if (pt <= pk) and (r_tenkan > r_kijun):
                tk_cross = "bullish_cross"
            elif (pt >= pk) and (r_tenkan < r_kijun):
                tk_cross = "bearish_cross"
            elif r_tenkan > r_kijun:
                tk_cross = "bullish"
            elif r_tenkan < r_kijun:
                tk_cross = "bearish"

        current = {
            "close":    r_close,
            "tenkan":   r_tenkan,
            "kijun":    r_kijun,
            "senkou_a": r_sa,
            "senkou_b": r_sb,
            "rsi_14":   r_rsi,
        }
        score, _ = score_signal(pd.Series(current), pd.Series(prev_row) if prev_row else None)

        total_score = score
        if total_score >= 4:
            signal = "bullish"
        elif total_score <= -4:
            signal = "bearish"
        else:
            signal = "mixed"

        results.append({
            "symbol":         symbol,
            "date":           ts.strftime("%Y-%m-%d"),
            "close":          round(float(r_close), 6),
            "rsi_14":         r_rsi if r_rsi is not None else 0.0,
            "rsi_zone":       rsi_zone,
            "tenkan":         r_tenkan,
            "kijun":          r_kijun,
            "senkou_a":       r_sa,
            "senkou_b":       r_sb,
            "cloud_color":    cloud_color,
            "price_vs_cloud": price_vs_cloud,
            "tk_cross":       tk_cross,
            "rsi_divergence": "",
            "signal":         signal,
            "signal_score":   total_score,
            "vol_ratio":      round(float(vol_ratio.iloc[i]), 4),
            "obv_trend":      _obv_trend(i),
            "vol_signal":     _vol_signal(i),
        })
        prev_row = current

    # Filter to requested date range if start_date given
    if start_date is not None:
        results = [r for r in results if r["date"] >= str(start_date)]

    return results


# ---------------------------------------------------------------------------
# Intraday (1h) per-symbol compute
# ---------------------------------------------------------------------------

def compute_for_symbol_1h(symbol: str, yf_ticker: str | None = None) -> list[dict]:
    """Compute Ichimoku + RSI + volume signals on 1h bars.

    Fetches the last 60 calendar days of 1h OHLCV from yfinance.
    Returns records with 'datetime' (UTC ISO string, no tz suffix) instead of 'date'.
    All computation logic is identical to the daily version — only the timeframe differs.
    """
    if yf_ticker is None:
        yf_ticker = symbol

    MIN_BARS = SENKOU_B_PERIOD + DISPLACEMENT  # 78 bars minimum for full Ichimoku warmup

    try:
        df = yf.download(yf_ticker, period="60d", interval="1h",
                         progress=False, auto_adjust=True)
    except Exception as e:
        print(f"  ERR   {symbol}: yfinance 1h download failed — {e}", file=sys.stderr)
        return []

    if df.empty:
        print(f"  WARN  {yf_ticker}: no 1h price data returned")
        return []

    if hasattr(df.columns, "get_level_values"):
        df.columns = df.columns.get_level_values(0)
    df.columns = [c.lower() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]  # guard against multi-ticker column bleed
    df = df.dropna(subset=["open", "high", "low", "close", "volume"])

    if len(df) < MIN_BARS:
        print(f"  WARN  {symbol}: only {len(df)} 1h bars — need {MIN_BARS} for full Ichimoku")
        return []

    close  = df["close"]
    high   = df["high"]
    low    = df["low"]
    volume = df["volume"]

    rsi  = compute_rsi(close)
    ichi = compute_ichimoku(high, low, close)

    vol_avg   = volume.rolling(20).mean()
    vol_ratio = (volume / vol_avg.replace(0, np.nan)).fillna(0)

    direction = np.sign(close.diff().fillna(0))
    obv       = (direction * volume).cumsum()
    obv_slope = obv.diff(10)

    def _vol_signal(i: int) -> str:
        ratio = float(vol_ratio.iloc[i])
        if ratio < 1.3:
            return "neutral"
        price_up = float(close.iloc[i]) > float(close.iloc[i - 1]) if i > 0 else False
        return "accumulation" if price_up else "distribution"

    def _obv_trend(i: int) -> str:
        slope = float(obv_slope.iloc[i])
        if abs(slope) < 1:
            return "flat"
        return "rising" if slope > 0 else "falling"

    results  = []
    prev_row = None

    for i, (ts, _) in enumerate(df.iterrows()):
        r_close  = close.iloc[i]
        r_rsi    = _safe(rsi.iloc[i])
        r_tenkan = _safe(ichi["tenkan"].iloc[i])
        r_kijun  = _safe(ichi["kijun"].iloc[i])
        r_sa     = _safe(ichi["senkou_a"].iloc[i])
        r_sb     = _safe(ichi["senkou_b"].iloc[i])

        if any(v is None for v in [r_tenkan, r_kijun, r_sa, r_sb]):
            prev_row = None
            continue

        rsi_zone = "neutral"
        if r_rsi is not None:
            if r_rsi > 70:
                rsi_zone = "overbought"
            elif r_rsi < 30:
                rsi_zone = "oversold"

        cloud_top    = max(r_sa, r_sb)
        cloud_bottom = min(r_sa, r_sb)
        cloud_color  = "green" if r_sa >= r_sb else "red"

        if r_close > cloud_top:
            price_vs_cloud = "above"
        elif r_close < cloud_bottom:
            price_vs_cloud = "below"
        else:
            price_vs_cloud = "inside"

        tk_cross = "neutral"
        if prev_row is not None and prev_row["tenkan"] is not None and prev_row["kijun"] is not None:
            pt = float(prev_row["tenkan"])
            pk = float(prev_row["kijun"])
            if (pt <= pk) and (r_tenkan > r_kijun):
                tk_cross = "bullish_cross"
            elif (pt >= pk) and (r_tenkan < r_kijun):
                tk_cross = "bearish_cross"
            elif r_tenkan > r_kijun:
                tk_cross = "bullish"
            elif r_tenkan < r_kijun:
                tk_cross = "bearish"

        current = {
            "close":    r_close,
            "tenkan":   r_tenkan,
            "kijun":    r_kijun,
            "senkou_a": r_sa,
            "senkou_b": r_sb,
            "rsi_14":   r_rsi,
        }
        score, _ = score_signal(pd.Series(current), pd.Series(prev_row) if prev_row else None)

        total_score = score
        if total_score >= 4:
            signal = "bullish"
        elif total_score <= -4:
            signal = "bearish"
        else:
            signal = "mixed"

        # yfinance 1h returns tz-aware UTC timestamps — strip tz for ClickHouse DateTime
        try:
            dt_utc = ts.tz_convert("UTC").tz_localize(None)
        except Exception:
            dt_utc = ts.replace(tzinfo=None)
        dt_str = dt_utc.strftime("%Y-%m-%d %H:%M:%S")

        results.append({
            "symbol":         symbol,
            "datetime":       dt_str,
            "close":          round(float(r_close), 6),
            "rsi_14":         r_rsi if r_rsi is not None else 0.0,
            "rsi_zone":       rsi_zone,
            "tenkan":         r_tenkan,
            "kijun":          r_kijun,
            "senkou_a":       r_sa,
            "senkou_b":       r_sb,
            "cloud_color":    cloud_color,
            "price_vs_cloud": price_vs_cloud,
            "tk_cross":       tk_cross,
            "rsi_divergence": "",
            "signal":         signal,
            "signal_score":   total_score,
            "vol_ratio":      round(float(vol_ratio.iloc[i]), 4),
            "obv_trend":      _obv_trend(i),
            "vol_signal":     _vol_signal(i),
        })
        prev_row = current

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ch_http = os.environ.get("CH_HTTP", "localhost:18123")
    ch_user = os.environ.get("CH_USER", "default")
    ch_pass = os.environ.get("CH_PASS", "")
    auth    = f"user={urllib.parse.quote(ch_user)}&password={urllib.parse.quote(ch_pass)}"

    # Always load known crypto symbols so we can apply the -USD suffix
    crypto_rows = ch_query(ch_http, auth,
        "SELECT DISTINCT symbol FROM portfolio.crypto_positions")
    crypto_symbols = {r[0] for r in crypto_rows}

    # Watchlist: extra symbols to always track (even without a position)
    watchlist_env = os.environ.get("WATCHLIST", "").strip()
    watchlist = {s.strip().upper() for s in watchlist_env.split(",") if s.strip()} if watchlist_env else set()

    # Symbols from args, or auto-read from held positions + watchlist
    symbols = sys.argv[1:]
    if not symbols:
        stock_rows  = ch_query(ch_http, auth,
            "SELECT DISTINCT symbol FROM portfolio.stock_positions")
        option_rows = ch_query(ch_http, auth,
            "SELECT DISTINCT symbol FROM portfolio.option_positions WHERE option_expiry >= today()")
        symbols = sorted(
            {r[0] for r in stock_rows}
            | {r[0] for r in option_rows}
            | crypto_symbols
            | watchlist
        )
    elif watchlist:
        # CLI args given — merge watchlist in too
        symbols = sorted(set(symbols) | watchlist)

    if not symbols:
        print("  no symbols — pass symbols as args or hold a position")
        return

    print(f"Computing signals for: {', '.join(symbols)}")

    columns = [
        "symbol", "date", "close",
        "rsi_14", "rsi_zone",
        "tenkan", "kijun", "senkou_a", "senkou_b",
        "cloud_color", "price_vs_cloud", "tk_cross",
        "rsi_divergence", "signal", "signal_score",
        "vol_ratio", "obv_trend", "vol_signal",
    ]

    total_rows = 0
    for symbol in symbols:
        # Accept "BTC-USD" syntax: use full string as yf ticker, strip -USD for storage
        if symbol.upper().endswith("-USD"):
            yf_ticker = symbol
            symbol    = symbol[:-4]
        elif symbol in crypto_symbols:
            yf_ticker = f"{symbol}-USD"
        else:
            yf_ticker = symbol
        records = compute_for_symbol(symbol, yf_ticker)
        if not records:
            continue

        rows = [[r[c] for c in columns] for r in records]
        ch_insert(ch_http, auth, "signals.indicators", columns, rows)
        total_rows += len(rows)

        # Print today's signal summary
        latest  = records[-1]
        rsi_str = f"{latest['rsi_14']:.1f}" if latest["rsi_14"] else "n/a"
        print(
            f"  {symbol:<8} {latest['date']}  close={latest['close']:.2f}"
            f"  RSI={rsi_str} [{latest['rsi_zone']}]"
            f"  cloud={latest['price_vs_cloud']} ({latest['cloud_color']})"
            f"  TK={latest['tk_cross']}"
            f"  → {latest['signal'].upper()} (score={latest['signal_score']:+d})"
        )

    print(f"\n  inserted {total_rows} rows across {len(symbols)} symbol(s)")


if __name__ == "__main__":
    main()
