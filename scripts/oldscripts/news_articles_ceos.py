#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Builds daily CEO articles from Google News RSS and saves:
  data/processed_articles/YYYY-MM-DD-ceo-articles-modal.csv

Inputs:
- rosters/main-roster.csv  (must include: CEO, Company, CEO Alias)

Output columns:
  ceo, company, title, url, source, sentiment  (sentiment ∈ {positive, neutral, negative})

Notes:
- Uses Google News RSS (no API key). Be respectful; we keep requests tiny.
- Classifies sentiment on the HEADLINE text using VADER (lightweight, works well on short headlines).
"""

from __future__ import annotations
import os, time, html, sys
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import quote_plus, urlparse

import pandas as pd
import requests
import feedparser
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


# Updated paths - consolidated with brand articles
BASE = Path(__file__).parent.parent
MAIN_ROSTER = BASE / "rosters" / "main-roster.csv"
OUT_DIR = BASE / "data" / "processed_articles"
OUT_DIR.mkdir(parents=True, exist_ok=True)

USER_AGENT = "Mozilla/5.0 (compatible; CEO-NewsBot/1.0; +https://example.com/bot)"
RSS_TMPL = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"

# Tunables (env overrides)
MAX_PER_ALIAS = int(os.getenv("ARTICLES_MAX_PER_ALIAS", "25"))
SLEEP_SEC = float(os.getenv("ARTICLES_SLEEP_SEC", "0.35"))   # polite delay between requests
TARGET_DATE = os.getenv("ARTICLES_DATE", "").strip()         # YYYY-MM-DD or empty for today (UTC)


def target_date() -> str:
    if TARGET_DATE:
        try:
            datetime.strptime(TARGET_DATE, "%Y-%m-%d")
            return TARGET_DATE
        except ValueError:
            print(f"WARNING: invalid ARTICLES_DATE={TARGET_DATE!r}; falling back to today.")
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def read_roster(path: Path) -> pd.DataFrame:
    """
    Reads rosters/main-roster.csv
    Expected columns: CEO, Company, CEO Alias (case-insensitive)
    Returns DataFrame with columns: alias, ceo, company
    """
    if not path.exists():
        raise FileNotFoundError(f"Missing roster file: {path}")
    
    df = pd.read_csv(path, encoding="utf-8-sig")
    
    # Normalize column names to lowercase for matching
    cols = {c.strip().lower(): c for c in df.columns}

    def col(name: str) -> str:
        """Find column by case-insensitive name"""
        for k, v in cols.items():
            if k == name.lower():
                return v
        raise KeyError(f"Expected column '{name}' in {path.name}")

    # Get the columns we need
    ceo_col = col("ceo")
    company_col = col("company")
    alias_col = col("ceo alias")

    # Create output dataframe
    out = df[[alias_col, ceo_col, company_col]].copy()
    out.columns = ["alias", "ceo", "company"]
    out["alias"] = out["alias"].astype(str).str.strip()
    out["ceo"] = out["ceo"].astype(str).str.strip()
    out["company"] = out["company"].astype(str).str.strip()
    
    # Filter out empty rows
    out = out[(out["alias"] != "") & (out["ceo"] != "") & (out["alias"] != "nan")]
    if out.empty:
        raise ValueError("No valid CEO rows after normalization.")
    return out.drop_duplicates()


def fetch_rss(query: str) -> feedparser.FeedParserDict:
    url = RSS_TMPL.format(query=quote_plus(query))
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    # feedparser handles bytes/text
    return feedparser.parse(resp.content)


def label_sentiment(analyzer: SentimentIntensityAnalyzer, text: str) -> str:
    s = analyzer.polarity_scores(text or "")
    c = s.get("compound", 0.0)
    if c >= 0.25:
        return "positive"
    if c <= -0.05:
        return "negative"
    return "neutral"


def extract_source(entry) -> str:
    # feedparser sometimes puts the publisher in entry.source.title
    try:
        src = entry.get("source", {}).get("title", "") or ""
    except Exception:
        src = ""
    if src:
        return str(src).strip()
    # fallback: domain from link
    link = entry.get("link") or entry.get("id") or ""
    try:
        host = urlparse(link).hostname or ""
        return host.replace("www.", "")
    except Exception:
        return ""


def build_articles_for_alias(alias: str, ceo: str, company: str, analyzer) -> list[dict]:
    try:
        feed = fetch_rss(alias)
    except Exception as e:
        print(f"ERROR fetching RSS for {alias!r}: {e}")
        return []

    rows = []
    for entry in (feed.entries or [])[:MAX_PER_ALIAS]:
        title = html.unescape(entry.get("title", "")).strip()
        link = (entry.get("link") or entry.get("id") or "").strip()
        source = extract_source(entry)
        if not title:
            continue
        sent = label_sentiment(analyzer, title)
        rows.append({
            "ceo": ceo,
            "company": company,
            "title": title,
            "url": link,
            "source": source,
            "sentiment": sent,
        })
    return rows


def main() -> int:
    out_date = target_date()
    # Updated to use new naming convention
    out_path = OUT_DIR / f"{out_date}-ceo-articles-modal.csv"
    print(f"Building articles for {out_date} → {out_path}")

    try:
        roster = read_roster(MAIN_ROSTER)
    except Exception as e:
        print(f"FATAL: {e}")
        # Still write an empty file so the UI is predictable
        pd.DataFrame(columns=["ceo","company","title","url","source","sentiment"]).to_csv(out_path, index=False)
        return 1

    analyzer = SentimentIntensityAnalyzer()
    all_rows: list[dict] = []

    for i, row in roster.iterrows():
        alias = row["alias"]
        ceo = row["ceo"]
        company = row["company"]
        if not alias:
            continue
        print(f"[{i+1}/{len(roster)}] {alias}")
        rows = build_articles_for_alias(alias, ceo, company, analyzer)
        all_rows.extend(rows)
        time.sleep(SLEEP_SEC)

    # de-duplicate (same ceo/title/url)
    if all_rows:
        df = pd.DataFrame(all_rows)
        df = df.drop_duplicates(subset=["ceo","title","url"]).reset_index(drop=True)
    else:
        df = pd.DataFrame(columns=["ceo","company","title","url","source","sentiment"])

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"✔ Wrote {len(df):,} rows → {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
