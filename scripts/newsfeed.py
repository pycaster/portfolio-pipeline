#!/usr/bin/env python3
"""
newsfeed.py — Financial news signal pipeline (sector-based)

Fetches RSS articles, classifies sectors via qwen3.5 (one article at a time),
scores sentiment with FinBERT, stores in ClickHouse, tracks price outcomes
against portfolio holdings, and computes Information Coefficient per
source/sector/ticker.

Modes:
    --ingest      Fetch new articles (RSS + EDGAR Form 4), classify sectors, score sentiment
    --outcomes    Fill price outcomes for articles >= 1 day old (run nightly)
    --ic          Recompute IC table from outcomes
    --alert       Print actionable signals for Sally (run in heartbeat)
    --mentions    Show ticker mention velocity vs 7-day baseline (spike detection)
    --status      Show pipeline status

Usage:
    python3 newsfeed.py --ingest
    python3 newsfeed.py --outcomes
    python3 newsfeed.py --ic
    python3 newsfeed.py --alert
    python3 newsfeed.py --mentions
    python3 newsfeed.py --status
    python3 newsfeed.py --outcomes --test   # skip 1-day age gate
"""

import argparse
import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

# ── Sector definitions ──────────────────────────────────────────────────────

# Used in the LLM classification prompt.
SECTOR_DEFINITIONS = {
    "semiconductors":  "chips, GPU, CPU, TSMC, fab, Nvidia, AMD, Intel, AI hardware, wafer, foundry",
    "defense_tech":    "military, Pentagon, weapons, NATO, intelligence agencies, defense contracts, Palantir, war, missiles",
    "energy":          "oil, gas, nuclear, OPEC, crude, LNG, renewables, pipeline, refinery, petroleum, electricity",
    "ai_cloud":        "AI models, cloud computing, data centers, OpenAI, Microsoft, AWS, hyperscaler, LLM, training compute",
    "macro_rates":     "Federal Reserve, interest rates, inflation, GDP, recession, Treasury, bonds, yield curve, CPI, FOMC",
    "geopolitical":    "China, Taiwan, Russia, Ukraine, Iran, trade war, tariffs, sanctions, NATO, Middle East conflict",
    "fintech_saas":    "software, SaaS, fintech, banking, payments, Stripe, Visa, financial services",
    "other":           "does not clearly fit any sector above",
}

# Maps portfolio tickers → which sectors are relevant for signal routing.
# Update this when portfolio changes significantly.
PORTFOLIO_SECTOR_MAP = {
    "NVDA": ["semiconductors", "ai_cloud", "geopolitical"],
    "PLTR": ["defense_tech", "ai_cloud", "macro_rates"],
    "IREN": ["energy", "ai_cloud", "macro_rates"],
}

# ── ClickHouse config ───────────────────────────────────────────────────────

CH_HTTP = os.environ.get("CH_HTTP", "localhost:18123")
CH_USER = os.environ.get("CH_USER", "default")
CH_PASS = os.environ.get("CH_PASS", "")

# ── LocalAI classifier config ────────────────────────────────────────────────
# Uses Qwen2.5-3B-Instruct (CPU-only, non-thinking) for fast sector classification.
# qwen3.5 stays on GPU for Sally. Direct port 8080 bypasses nginx proxy timeouts.

LOCALAI_URL   = os.environ.get("LOCALAI_URL",  "http://localhost:8080/v1/chat/completions")
LOCALAI_MODEL = os.environ.get("LOCALAI_MODEL", "classifier")

# ── RSS / Atom sources ───────────────────────────────────────────────────────
# Reddit feeds use Atom format; ZeroHedge uses standard RSS.
# Reddit: using /hot.rss to filter by engagement (avoids low-quality spam from /new).

RSS_SOURCES = {
    "zerohedge":  "https://feeds.feedburner.com/zerohedge/feed",
    "reddit_wsb": "https://www.reddit.com/r/wallstreetbets/hot.rss",
}

# SEC EDGAR Form 4 feeds — insider buy/sell filings, one per held ticker.
# Requires declared user-agent (SEC policy). Uses CIK numbers.
SEC_USER_AGENT = "portfolio-pipeline venkat@vrmap.net"
EDGAR_SOURCES = {
    "nvda": "1045810",   # NVIDIA
    "pltr": "1321655",   # Palantir
    "iren": "1517767",   # IREN (Iris Energy)
}

# DD-quality keywords in post title (elevates signal weight)
DD_TITLE_KEYWORDS = ["DD", "Due Diligence", "Research", "Analysis", "Deep Dive", "Thesis"]

# ── Alert thresholds ────────────────────────────────────────────────────────

ALERT_SENTIMENT_MIN = 0.82
ALERT_WINDOW_HOURS  = 3
IC_MIN_SAMPLES      = 10
IC_ALERT_THRESHOLD  = 0.08

# Sectors that map to current portfolio holdings (inverse of PORTFOLIO_SECTOR_MAP)
def portfolio_relevant_sectors():
    sectors = set()
    for s_list in PORTFOLIO_SECTOR_MAP.values():
        sectors.update(s_list)
    return sectors


def tickers_for_sector(sector):
    return [t for t, sectors in PORTFOLIO_SECTOR_MAP.items() if sector in sectors]


# ── ClickHouse helpers ──────────────────────────────────────────────────────

def _auth():
    return f"user={urllib.parse.quote(CH_USER)}&password={urllib.parse.quote(CH_PASS)}"


def ch_query(sql):
    url = (
        f"http://{CH_HTTP}/?"
        f"{_auth()}&query={urllib.parse.quote(sql + ' FORMAT JSONCompact')}"
    )
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read()).get("data", [])


def ch_insert(table, columns, rows):
    if not rows:
        return
    col_str = ", ".join(columns)
    lines = "\n".join(
        json.dumps(dict(zip(columns, row)), default=str) for row in rows
    )
    url = (
        f"http://{CH_HTTP}/?"
        f"{_auth()}&query="
        f"{urllib.parse.quote(f'INSERT INTO {table} ({col_str}) FORMAT JSONEachRow')}"
    )
    req = urllib.request.Request(url, data=lines.encode(), method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        resp.read()


# ── RSS fetching ────────────────────────────────────────────────────────────

def strip_html(text):
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def fetch_rss(url):
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "portfolio-pipeline/newsfeed"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        content = resp.read()

    root = ET.fromstring(content)
    ns_rss  = {"content": "http://purl.org/rss/1.0/modules/content/"}
    ns_atom = {"atom": "http://www.w3.org/2005/Atom"}
    articles = []

    # Detect feed format by root tag
    is_atom = "atom" in root.tag.lower() or root.tag == "{http://www.w3.org/2005/Atom}feed"

    if is_atom:
        items = root.findall("{http://www.w3.org/2005/Atom}entry")
    else:
        items = root.findall(".//item")

    for item in items:
        if is_atom:
            # Atom: <link href="..."/> element
            link_el = item.find("{http://www.w3.org/2005/Atom}link")
            link = (link_el.get("href") if link_el is not None else "").strip()
            title = (item.findtext("{http://www.w3.org/2005/Atom}title") or "").strip()
            raw   = item.findtext("{http://www.w3.org/2005/Atom}content") or \
                    item.findtext("{http://www.w3.org/2005/Atom}summary") or ""
            pub_str = item.findtext("{http://www.w3.org/2005/Atom}published") or \
                      item.findtext("{http://www.w3.org/2005/Atom}updated") or ""
            try:
                published_at = datetime.fromisoformat(pub_str.replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                published_at = datetime.now(timezone.utc)
        else:
            link = (item.findtext("link") or "").strip()
            title = (item.findtext("title") or "").strip()
            raw   = item.findtext("content:encoded", namespaces=ns_rss) or \
                    item.findtext("description") or ""
            pub_str = item.findtext("pubDate") or ""
            try:
                published_at = parsedate_to_datetime(pub_str).astimezone(timezone.utc)
            except Exception:
                published_at = datetime.now(timezone.utc)

        if not link:
            continue

        article_id = hashlib.sha256(link.encode()).hexdigest()[:32]
        full_text  = strip_html(raw)[:2000]
        tickers    = list(dict.fromkeys(re.findall(r"\$([A-Z]{1,6})\b", title + " " + full_text)))

        articles.append({
            "article_id": article_id,
            "url": link,
            "title": title,
            "full_text": full_text,
            "published_at": published_at,
            "tickers": tickers,
        })

    return articles


def fetch_edgar_form4(ticker, cik):
    """
    Fetch recent Form 4 filings for a ticker from SEC EDGAR.
    Returns list of article dicts with insider buy/sell info.
    Each filing is fetched individually to extract insider name + transaction type.
    """
    feed_url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcompany&CIK={cik}&type=4&dateb=&owner=include&count=10&search_text=&output=atom"
    )
    req = urllib.request.Request(
        feed_url,
        headers={"User-Agent": SEC_USER_AGENT},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        content = resp.read()

    root = ET.fromstring(content)
    ns = "{http://www.w3.org/2005/Atom}"
    articles = []

    for entry in root.findall(f"{ns}entry"):
        link_el = entry.find(f"{ns}link")
        index_url = link_el.get("href") if link_el is not None else ""
        if not index_url:
            continue

        pub_str = entry.findtext(f"{ns}updated") or entry.findtext(f"{ns}published") or ""
        try:
            published_at = datetime.fromisoformat(pub_str.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            published_at = datetime.now(timezone.utc)

        # Fetch the filing index to find the actual Form 4 XML
        try:
            xml_url = index_url.replace("-index.htm", ".txt").replace(
                "Archives/edgar/data", "Archives/edgar/data"
            )
            # Convert index URL to primary XML doc URL
            idx_req = urllib.request.Request(
                index_url, headers={"User-Agent": SEC_USER_AGENT}
            )
            with urllib.request.urlopen(idx_req, timeout=15) as r:
                idx_html = r.read().decode(errors="replace")

            # Find the .xml form4 link in the index HTML
            xml_match = re.search(r'href="(/Archives/edgar/data/[^"]+\.xml)"', idx_html)
            if not xml_match:
                continue
            form_url = "https://www.sec.gov" + xml_match.group(1)

            form_req = urllib.request.Request(
                form_url, headers={"User-Agent": SEC_USER_AGENT}
            )
            with urllib.request.urlopen(form_req, timeout=15) as r:
                form_xml = r.read().decode(errors="replace")

            form_root = ET.fromstring(form_xml)

            # Extract insider name and transaction details
            name_el = form_root.find(".//reportingOwner/reportingOwnerId/rptOwnerName")
            insider_name = name_el.text.strip() if name_el is not None else "Unknown"

            is_officer = form_root.find(".//reportingOwner/reportingOwnerRelationship/isOfficer")
            is_director = form_root.find(".//reportingOwner/reportingOwnerRelationship/isDirector")
            role = "Officer" if (is_officer is not None and is_officer.text == "1") \
                   else "Director" if (is_director is not None and is_director.text == "1") \
                   else "Insider"

            # Get all non-derivative transactions
            transactions = []
            for txn in form_root.findall(".//nonDerivativeTransaction"):
                code_el = txn.find("transactionAmounts/transactionAcquiredDisposedCode/value")
                shares_el = txn.find("transactionAmounts/transactionShares/value")
                price_el = txn.find("transactionAmounts/transactionPricePerShare/value")
                code = code_el.text if code_el is not None else "?"
                shares = shares_el.text if shares_el is not None else "0"
                price = price_el.text if price_el is not None else "0"
                action = "BUY" if code == "A" else "SELL" if code == "D" else code
                try:
                    total = float(shares) * float(price)
                    transactions.append(f"{action} {float(shares):,.0f} shares @ ${float(price):.2f} (${total:,.0f})")
                except Exception:
                    transactions.append(f"{action} {shares} shares")

            if not transactions:
                continue

            txn_summary = "; ".join(transactions)
            title = f"Form 4: {insider_name} ({role}) {ticker.upper()} — {transactions[0]}"
            full_text = f"SEC Form 4 filing. {insider_name} ({role}) at {ticker.upper()}: {txn_summary}. Filing: {index_url}"

            article_id = hashlib.sha256(index_url.encode()).hexdigest()[:32]
            articles.append({
                "article_id": article_id,
                "url": index_url,
                "title": title,
                "full_text": full_text,
                "published_at": published_at,
                "tickers": [ticker.upper()],
            })

        except Exception as e:
            print(f"  WARN: EDGAR parse failed for {ticker}: {e}", file=sys.stderr)
            continue

    return articles


# ── qwen3.5 sector classification ───────────────────────────────────────────

SECTOR_LIST = list(SECTOR_DEFINITIONS.keys())

SECTOR_PROMPT = """\
Classify this financial news article into ALL relevant sectors.

Valid sectors: semiconductors, defense_tech, energy, ai_cloud, macro_rates, geopolitical, fintech_saas, other

Article title: {title}
Article text: {text}

Reply with ONLY a JSON array of sector names from the list above.
Example: ["macro_rates", "geopolitical"]
No explanation. No other text."""


def classify_sectors(title, full_text):
    """
    Call qwen3.5 via CCR to classify article into sectors.
    Returns list of sector names (subset of SECTOR_LIST).
    Falls back to ["other"] on any error.
    """
    prompt = SECTOR_PROMPT.format(
        title=title,
        text=full_text[:500],
    )

    payload = json.dumps({
        "model": LOCALAI_MODEL,
        "max_tokens": 64,  # classifier model — non-thinking, answer is a short JSON array
        "messages": [{"role": "user", "content": prompt}],
    }).encode()

    req = urllib.request.Request(
        LOCALAI_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            result = json.loads(resp.read())
        raw = result["choices"][0]["message"]["content"].strip()
        # Parse JSON array from response
        match = re.search(r"\[.*?\]", raw, re.DOTALL)
        if not match:
            return ["other"]
        sectors = json.loads(match.group())
        # Validate — keep only known sector names
        valid = [s for s in sectors if s in SECTOR_LIST]
        return valid if valid else ["other"]
    except Exception as e:
        print(f"  WARN: sector classification failed: {e}", file=sys.stderr)
        return ["other"]


# ── FinBERT sentiment ───────────────────────────────────────────────────────

_sentiment_pipe = None


def get_sentiment_pipeline():
    global _sentiment_pipe
    if _sentiment_pipe is None:
        try:
            from transformers import pipeline as hf_pipeline
        except ImportError:
            print("ERR: transformers not installed. Run: make newsfeed-setup", file=sys.stderr)
            sys.exit(1)
        # Run on CPU — GPUs are reserved for LocalAI (qwen3.5 fills both)
        device = "cpu"
        print(f"  loading FinBERT on {device}...", file=sys.stderr)
        _sentiment_pipe = hf_pipeline(
            "text-classification",
            model="ProsusAI/finbert",
            device=device,
            truncation=True,
            max_length=512,
        )
    return _sentiment_pipe


def score_sentiment(text):
    """Score a single text. Returns (label, score, signed_score)."""
    pipe = get_sentiment_pipeline()
    r = pipe(text)[0]
    label = r["label"]
    score = r["score"]
    signed = score if label == "positive" else (-score if label == "negative" else 0.0)
    return label, score, signed


# ── Ingest ──────────────────────────────────────────────────────────────────

def cmd_ingest():
    relevant_sectors = portfolio_relevant_sectors()
    print(f"  portfolio sectors: {sorted(relevant_sectors)}")

    seen = {r[0] for r in ch_query(
        "SELECT DISTINCT article_id FROM signals.newsfeed_articles"
    )}
    print(f"  already stored: {len(seen)} articles")

    rows_to_insert = []

    for source, rss_url in RSS_SOURCES.items():
        print(f"  fetching {source}...")
        try:
            articles = fetch_rss(rss_url)
        except Exception as e:
            print(f"  WARN: failed to fetch {source}: {e}", file=sys.stderr)
            continue

        new_articles = [a for a in articles if a["article_id"] not in seen]
        print(f"  {source}: {len(articles)} in feed, {len(new_articles)} new")

        for article in new_articles:
            # 1. Classify sectors (classifier model)
            sectors = classify_sectors(article["title"], article["full_text"])
            sector_match = int(bool(set(sectors) & relevant_sectors))
            is_dd = any(kw.lower() in article["title"].lower() for kw in DD_TITLE_KEYWORDS)
            dd_tag = " [DD]" if is_dd else ""
            print(f"    [{source}] sectors={sectors} match={sector_match}{dd_tag} — {article['title'][:60]}")

            # 2. Score sentiment (FinBERT)
            label, score, signed = score_sentiment(
                f"{article['title']}. {article['full_text'][:400]}"
            )

            rows_to_insert.append([
                article["article_id"],
                source,
                article["url"],
                article["title"],
                article["full_text"],
                article["published_at"].strftime("%Y-%m-%d %H:%M:%S"),
                sectors,
                sector_match,
                label,
                round(score, 4),
                round(signed, 4),
                article["tickers"],
            ])

    # ── EDGAR Form 4 insider filings ────────────────────────────────────────
    for ticker, cik in EDGAR_SOURCES.items():
        source = f"edgar_{ticker}"
        print(f"  fetching {source}...")
        try:
            articles = fetch_edgar_form4(ticker, cik)
        except Exception as e:
            print(f"  WARN: failed to fetch {source}: {e}", file=sys.stderr)
            continue

        new_articles = [a for a in articles if a["article_id"] not in seen]
        print(f"  {source}: {len(articles)} filings, {len(new_articles)} new")

        for article in new_articles:
            sectors = classify_sectors(article["title"], article["full_text"])
            sector_match = int(bool(set(sectors) & relevant_sectors))
            print(f"    [{source}] sectors={sectors} match={sector_match} — {article['title'][:60]}")

            label, score, signed = score_sentiment(
                f"{article['title']}. {article['full_text'][:400]}"
            )

            rows_to_insert.append([
                article["article_id"],
                source,
                article["url"],
                article["title"],
                article["full_text"],
                article["published_at"].strftime("%Y-%m-%d %H:%M:%S"),
                sectors,
                sector_match,
                label,
                round(score, 4),
                round(signed, 4),
                article["tickers"],
            ])
            seen.add(article["article_id"])

    if rows_to_insert:
        ch_insert(
            "signals.newsfeed_articles",
            [
                "article_id", "source", "url", "title", "full_text",
                "published_at", "sectors", "sector_match",
                "sentiment", "sentiment_score", "sentiment_signed",
                "tickers",
            ],
            rows_to_insert,
        )
        print(f"  stored {len(rows_to_insert)} new articles")
    else:
        print("  nothing new")


# ── Outcomes ────────────────────────────────────────────────────────────────

def cmd_outcomes(test_mode=False):
    """
    For sector-matched articles >= 1 day old, compute price changes for all
    portfolio tickers that the article's sectors map to.
    """
    age_filter = "1=1" if test_mode else "a.published_at < now() - INTERVAL 1 DAY"
    if test_mode:
        print("  [test mode] skipping 1-day age gate")

    pending = ch_query(f"""
        SELECT a.article_id, a.sectors, a.published_at, a.sentiment_signed
        FROM signals.newsfeed_articles a FINAL
        WHERE a.sector_match = 1
          AND {age_filter}
          AND a.article_id NOT IN (
              SELECT DISTINCT article_id FROM signals.newsfeed_outcomes
          )
    """)

    if not pending:
        print("  no pending outcome articles")
        return

    print(f"  scoring outcomes for {len(pending)} articles...")
    rows = []

    for row in pending:
        article_id, sectors, published_at_str, sentiment_signed = row
        try:
            pub_dt = datetime.strptime(str(published_at_str), "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue

        pub_date = pub_dt.date()

        # For each sector in the article, find relevant portfolio tickers
        tickers_covered = set()
        sector_for_ticker = {}
        for sector in sectors:
            for ticker in tickers_for_sector(sector):
                if ticker not in tickers_covered:
                    tickers_covered.add(ticker)
                    sector_for_ticker[ticker] = sector

        for ticker, sector in sector_for_ticker.items():
            price_rows = ch_query(f"""
                SELECT date, close FROM portfolio.prices FINAL
                WHERE symbol = '{ticker}'
                  AND date >= '{pub_date}'
                ORDER BY date ASC LIMIT 3
            """)
            if len(price_rows) < 2:
                continue

            base_close = float(price_rows[0][1])
            d1_close   = float(price_rows[1][1]) if len(price_rows) > 1 else None
            d2_close   = float(price_rows[2][1]) if len(price_rows) > 2 else None
            base_date  = price_rows[0][0]

            pc_1d = round((d1_close / base_close - 1) * 100, 4) if d1_close else None
            pc_2d = round((d2_close / base_close - 1) * 100, 4) if d2_close else None

            rows.append([
                article_id, ticker, sector,
                float(sentiment_signed),
                str(published_at_str),
                str(base_date),
                pc_1d, pc_2d,
            ])

    if rows:
        ch_insert(
            "signals.newsfeed_outcomes",
            ["article_id", "ticker", "sector", "sentiment_signed",
             "published_at", "price_date", "price_change_1d", "price_change_2d"],
            rows,
        )
        print(f"  scored {len(rows)} article × ticker outcome pairs")
    else:
        print("  no outcomes computable yet")


# ── IC Calculation ──────────────────────────────────────────────────────────

def cmd_ic():
    """Recompute IC per source/sector/ticker from outcomes."""
    combos = ch_query("""
        SELECT DISTINCT a.source, o.sector, o.ticker
        FROM signals.newsfeed_outcomes o
        JOIN signals.newsfeed_articles a ON o.article_id = a.article_id
        WHERE o.price_change_1d IS NOT NULL
    """)

    rows = []
    for source, sector, ticker in combos:
        ic_rows = ch_query(f"""
            SELECT
                corr(o.sentiment_signed, o.price_change_1d) AS ic_1d,
                corr(o.sentiment_signed, o.price_change_2d) AS ic_2d,
                count() AS n
            FROM signals.newsfeed_outcomes o
            JOIN signals.newsfeed_articles a ON o.article_id = a.article_id
            WHERE a.source = '{source}'
              AND o.sector  = '{sector}'
              AND o.ticker  = '{ticker}'
              AND o.price_change_1d IS NOT NULL
              AND o.price_change_2d IS NOT NULL
        """)
        if not ic_rows:
            continue
        ic_1d = float(ic_rows[0][0]) if ic_rows[0][0] is not None else 0.0
        ic_2d = float(ic_rows[0][1]) if ic_rows[0][1] is not None else 0.0
        n     = int(ic_rows[0][2])
        rows.append([source, sector, ticker, ic_1d, ic_2d, n])
        print(f"  {source}/{sector}/{ticker}: ic_1d={ic_1d:.3f} ic_2d={ic_2d:.3f} n={n}")

    if rows:
        ch_insert(
            "signals.newsfeed_ic",
            ["source", "sector", "ticker", "ic_1d", "ic_2d", "sample_count"],
            rows,
        )
        print(f"  IC table updated ({len(rows)} combos)")
    else:
        print("  not enough outcome data for IC yet")


# ── Alert ───────────────────────────────────────────────────────────────────

def cmd_alert(test_mode=False):
    """
    Print actionable signals for Sally.
    Conditions (AND):
      1. sector_match = 1
      2. sentiment_score >= ALERT_SENTIMENT_MIN
      3. published within ALERT_WINDOW_HOURS (or test_mode)
      4. IC > IC_ALERT_THRESHOLD if n >= IC_MIN_SAMPLES (else pass-through with note)
    """
    if test_mode:
        cutoff_str = "1970-01-01 00:00:00"
    else:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=ALERT_WINDOW_HOURS)
        cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    recent = ch_query(f"""
        SELECT article_id, source, title, url, sectors,
               sentiment, sentiment_score, sentiment_signed, published_at
        FROM signals.newsfeed_articles FINAL
        WHERE sector_match = 1
          AND sentiment_score >= {ALERT_SENTIMENT_MIN}
          AND published_at >= '{cutoff_str}'
        ORDER BY published_at DESC
    """)

    if not recent:
        sys.exit(0)

    # Load IC for gating
    ic_data = {}
    for r in ch_query(
        "SELECT source, sector, ticker, ic_1d, sample_count FROM signals.newsfeed_ic FINAL"
    ):
        ic_data[(r[0], r[1], r[2])] = (float(r[3]), int(r[4]))

    alerts = []
    for row in recent:
        article_id, source, title, url, sectors, sentiment, score, signed, pub_at = row
        for sector in sectors:
            tickers = tickers_for_sector(sector)
            if not tickers:
                continue
            for ticker in tickers:
                ic, n = ic_data.get((source, sector, ticker), (None, 0))
                if n >= IC_MIN_SAMPLES and abs(ic) < IC_ALERT_THRESHOLD:
                    continue  # IC established but below threshold — suppress
                ic_note = (
                    f"IC={ic:.2f} (n={n})" if n >= IC_MIN_SAMPLES
                    else f"IC=accumulating ({n}/{IC_MIN_SAMPLES} samples)"
                )
                alerts.append({
                    "source": source, "sector": sector, "ticker": ticker,
                    "title": title, "url": url,
                    "sentiment": sentiment, "score": round(score, 2),
                    "ic_note": ic_note, "published_at": str(pub_at),
                })

    if not alerts:
        sys.exit(0)

    for a in alerts:
        print(
            f"SIGNAL [{a['source'].upper()}] {a['sentiment'].upper()} ({a['score']}) "
            f"on ${a['ticker']} via {a['sector']} | {a['ic_note']}\n"
            f"  \"{a['title']}\"\n"
            f"  {a['url']}\n"
            f"  published: {a['published_at']}"
        )


# ── Mention velocity ────────────────────────────────────────────────────────

MENTION_SPIKE_RATIO = 2.0   # alert if 24h mentions >= 2x daily baseline
MENTION_MIN_RECENT  = 2     # must have at least this many in 24h to alert

def cmd_mentions():
    """
    Show ticker mention velocity: compare last 24h count vs 7-day daily baseline.
    Alerts when a ticker is getting 2x+ normal attention — useful pre-event signal.
    Only looks at tickers extracted from article text ($TICKER mentions).
    """
    # 7-day baseline (days 1-7 ago, excluding last 24h to avoid contamination)
    baseline_rows = ch_query("""
        SELECT ticker, count() AS n
        FROM (
            SELECT arrayJoin(tickers) AS ticker
            FROM signals.newsfeed_articles FINAL
            WHERE published_at >= now() - INTERVAL 7 DAY
              AND published_at <  now() - INTERVAL 1 DAY
              AND ticker != ''
        )
        GROUP BY ticker
        ORDER BY n DESC
    """)

    # Last 24h
    recent_rows = ch_query("""
        SELECT ticker, count() AS n
        FROM (
            SELECT arrayJoin(tickers) AS ticker
            FROM signals.newsfeed_articles FINAL
            WHERE published_at >= now() - INTERVAL 1 DAY
              AND ticker != ''
        )
        GROUP BY ticker
        ORDER BY n DESC
    """)

    baseline = {r[0]: int(r[1]) for r in baseline_rows}
    recent   = {r[0]: int(r[1]) for r in recent_rows}

    # Baseline is over 6 days (7-day window minus the last day)
    BASELINE_DAYS = 6

    print(f"{'TICKER':<8}  {'24h':>5}  {'7d/day':>7}  {'RATIO':>6}  NOTE")
    print("-" * 45)

    all_tickers = sorted(set(list(recent.keys()) + list(baseline.keys())))
    spikes = []

    for ticker in all_tickers:
        r_count = recent.get(ticker, 0)
        b_count = baseline.get(ticker, 0)
        b_per_day = b_count / BASELINE_DAYS if b_count > 0 else 0

        if b_per_day > 0:
            ratio = r_count / b_per_day
        elif r_count > 0:
            ratio = float("inf")
        else:
            continue  # nothing to show

        portfolio_flag = " *" if ticker in PORTFOLIO_SECTOR_MAP else ""
        note = ""
        if r_count >= MENTION_MIN_RECENT and ratio >= MENTION_SPIKE_RATIO:
            note = "⚡ SPIKE"
            spikes.append((ticker, r_count, b_per_day, ratio))

        ratio_str = f"{ratio:.1f}x" if ratio != float("inf") else "NEW"
        print(f"  {ticker:<8}  {r_count:>5}  {b_per_day:>7.1f}  {ratio_str:>6}  {note}{portfolio_flag}")

    print()
    if spikes:
        print("Velocity spikes (>= 2x baseline, >= 2 articles in 24h):")
        for ticker, r, b, ratio in sorted(spikes, key=lambda x: -x[3]):
            in_portfolio = ticker in PORTFOLIO_SECTOR_MAP
            flag = " [PORTFOLIO]" if in_portfolio else ""
            print(f"  ${ticker}: {r} mentions today vs {b:.1f}/day baseline ({ratio:.1f}x){flag}")
    else:
        print("No velocity spikes detected.")

    print(f"\n(* = in portfolio map)")


# ── Status ──────────────────────────────────────────────────────────────────

def cmd_status():
    counts = ch_query(
        "SELECT source, count() FROM signals.newsfeed_articles FINAL GROUP BY source"
    )
    print("Article counts:")
    for source, n in counts:
        print(f"  {source}: {n}")

    sector_counts = ch_query("""
        SELECT arrayJoin(sectors) AS sector, count() AS n
        FROM signals.newsfeed_articles FINAL
        GROUP BY sector ORDER BY n DESC
    """)
    if sector_counts:
        print("\nSector distribution:")
        for sector, n in sector_counts:
            mark = " *" if sector in portfolio_relevant_sectors() else ""
            print(f"  {sector}: {n}{mark}")
        print("  (* = maps to a current holding)")

    outcome_count = ch_query("SELECT count() FROM signals.newsfeed_outcomes")
    print(f"\nOutcomes scored: {outcome_count[0][0] if outcome_count else 0}")

    ic_rows = ch_query(
        "SELECT source, sector, ticker, ic_1d, ic_2d, sample_count "
        "FROM signals.newsfeed_ic FINAL ORDER BY abs(ic_1d) DESC LIMIT 20"
    )
    if ic_rows:
        print("\nIC table (top 20 by |ic_1d|):")
        for r in ic_rows:
            print(f"  {r[0]}/{r[1]}/{r[2]}: ic_1d={r[3]:.3f} ic_2d={r[4]:.3f} n={r[5]}")
    else:
        print("\nIC table: empty")


# ── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="News feed signal pipeline (sector-based)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--ingest",   action="store_true")
    group.add_argument("--outcomes", action="store_true")
    group.add_argument("--ic",       action="store_true")
    group.add_argument("--alert",    action="store_true")
    group.add_argument("--mentions", action="store_true")
    group.add_argument("--status",   action="store_true")
    parser.add_argument("--test",    action="store_true", help="Skip age/IC gates for testing")
    args = parser.parse_args()

    if args.ingest:
        cmd_ingest()
    elif args.outcomes:
        cmd_outcomes(test_mode=args.test)
    elif args.ic:
        cmd_ic()
    elif args.alert:
        cmd_alert(test_mode=args.test)
    elif args.mentions:
        cmd_mentions()
    elif args.status:
        cmd_status()
