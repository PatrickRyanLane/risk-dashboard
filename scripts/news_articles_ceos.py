#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Builds daily CEO articles from Google News RSS.
Features: batching, checkpointing, connection pooling, retry logic.

Output: data/processed_articles/YYYY-MM-DD-ceo-articles-modal.csv
Checkpoint: data/checkpoints/YYYY-MM-DD-ceo-checkpoint.json
"""

from __future__ import annotations
import os, time, html, sys, argparse, json
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import quote_plus, urlparse

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import feedparser
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from storage_utils import CloudStorageManager
from llm_utils import is_uncertain
from db_writer import upsert_articles_mentions
from risk_rules import (
    classify_control,
    is_financial_routine,
    parse_company_domains,
    should_force_negative_ceo,
    strip_neutral_terms_ceo,
)



def _is_reddit_source(source: str) -> bool:
    """Return True if the article source is Reddit."""
    if not source:
        return False
    return "reddit" in source.lower()

def load_roster_domains(storage, roster_path: str) -> dict[str, set[str]]:
    company_domains: dict[str, set[str]] = {}
    try:
        if storage:
            if not storage.file_exists(roster_path):
                print(f"[WARN] Roster not found in Cloud Storage at {roster_path}")
                return company_domains
            df = storage.read_csv(roster_path)
        else:
            roster_file = Path(roster_path)
            if not roster_file.exists():
                print(f"[WARN] Roster not found at {roster_file}")
                return company_domains
            df = pd.read_csv(roster_file, encoding="utf-8-sig")

        cols = {c.strip().lower(): c for c in df.columns}
        company_col = cols.get("company")
        website_col = None
        for key in ["website", "websites", "domain", "url", "site", "homepage"]:
            if key in cols:
                website_col = cols[key]
                break
        if not company_col or not website_col:
            print("[WARN] Missing company or website column in roster")
            return company_domains

        for _, row in df.iterrows():
            company = str(row[company_col]).strip()
            if not company or company.lower() == "nan":
                continue
            val = row[website_col]
            if pd.isna(val):
                continue
            domains = parse_company_domains(str(val))
            if domains:
                company_domains[company] = domains
    except Exception as e:
        print(f"[WARN] Failed reading roster domains: {e}")
    return company_domains


# Paths
BASE = Path(__file__).parent.parent
MAIN_ROSTER = "rosters/main-roster.csv"
OUT_DIR = "data/processed_articles"
CHECKPOINT_DIR = "data/checkpoints"

USER_AGENT = "Mozilla/5.0 (compatible; CEO-NewsBot/1.0; +https://example.com/bot)"
RSS_TMPL = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"

# Tunables
MAX_PER_ALIAS = int(os.getenv("ARTICLES_MAX_PER_ALIAS", "25"))
SLEEP_SEC = float(os.getenv("ARTICLES_SLEEP_SEC", "0.25"))
DEFAULT_BATCH_SIZE = int(os.getenv("ARTICLES_BATCH_SIZE", "100"))

def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_rss(session: requests.Session, query: str) -> feedparser.FeedParserDict:
    url = RSS_TMPL.format(query=quote_plus(query))
    resp = session.get(url, timeout=20)
    resp.raise_for_status()
    return feedparser.parse(resp.content)


def classify(title: str, analyzer: SentimentIntensityAnalyzer, source: str = "", url: str = ""):
    """
    Classify sentiment with multi-stage filtering.
    """
    flags = {
        "is_reddit": False,
        "is_forced": False,
        "is_finance": False,
        "forced_reason": "",
        "compound": None,
        "cleaned": "",
    }
    # 1. Force NEGATIVE for Reddit
    if _is_reddit_source(source):
        flags["is_reddit"] = True
        flags["forced_reason"] = "reddit"
        return "negative", flags

    # 2. Neutralize routine financial coverage
    if is_financial_routine(title, url=url, source=source):
        flags["is_finance"] = True
        flags["forced_reason"] = "finance"
        return "neutral", flags
    
    # 3. Force NEGATIVE for CEO trouble terms
    if should_force_negative_ceo(title):
        flags["is_forced"] = True
        flags["forced_reason"] = "ceo_terms"
        return "negative", flags
    
    # 4. Strip neutral terms
    cleaned = strip_neutral_terms_ceo(title or "")
    flags["cleaned"] = cleaned
    
    # If nothing meaningful remains, neutral
    if not cleaned or len(cleaned.split()) < 2:
        return "neutral", flags
    
    # 5. VADER on cleaned headline
    scores = analyzer.polarity_scores(cleaned)
    compound = scores.get("compound", 0.0)
    flags["compound"] = compound
    
    # Unified thresholds
    if compound >= 0.15:
        return "positive", flags
    if compound <= -0.10:
        return "negative", flags
    return "neutral", flags


def extract_source(entry) -> str:
    try:
        src = entry.get("source", {}).get("title", "") or ""
    except Exception:
        src = ""
    if src:
        return str(src).strip()
    link = entry.get("link") or entry.get("id") or ""
    try:
        host = urlparse(link).hostname or ""
        return host.replace("www.", "")
    except Exception:
        return ""


def build_articles_for_alias(session: requests.Session, alias: str, ceo: str, company: str, analyzer, company_domains: dict[str, set[str]]) -> list[dict]:
    try:
        feed = fetch_rss(session, alias)
    except Exception as e:
        print(f"  ⚠️ RSS error for {alias!r}: {e}")
        return []

    rows = []
    for entry in (feed.entries or [])[:MAX_PER_ALIAS]:
        title = html.unescape(entry.get("title", "")).strip()
        link = (entry.get("link") or entry.get("id") or "").strip()
        source = extract_source(entry)
        if not title:
            continue
            
        finance_routine = is_financial_routine(title, url=link, source=source)
        sent, flags = classify(title, analyzer, source, link)
        finance_routine = flags.get("is_finance", False)
        is_forced = bool(flags.get("forced_reason"))
        uncertain, uncertain_reason = is_uncertain(
            sent,
            finance_routine,
            is_forced,
            flags.get("compound"),
            flags.get("cleaned", ""),
            title
        )
        llm_label = ""
        llm_severity = ""
        llm_reason = ""
        control_class = "controlled" if classify_control(company, link, company_domains, entity_type="ceo", person_name=ceo) else "uncontrolled"
        
        rows.append({
            "ceo": ceo,
            "company": company,
            "title": title,
            "url": link,
            "source": source,
            "sentiment": sent,
            "controlled": control_class,
            "finance_routine": finance_routine,
            "uncertain": uncertain,
            "uncertain_reason": uncertain_reason,
            "llm_label": llm_label,
            "llm_severity": llm_severity,
            "llm_reason": llm_reason,
        })
    return rows


def read_roster(storage, roster_path: str) -> pd.DataFrame:
    try:
        if storage:
            if not storage.file_exists(roster_path):
                raise FileNotFoundError(f"Roster not found: {roster_path}")
            print(f"📋 Loading roster from Cloud Storage: {roster_path}")
            df = storage.read_csv(roster_path)
        else:
            print(f"📋 Loading roster from local file: {roster_path}")
            roster_file = Path(roster_path)
            if not roster_file.exists():
                raise FileNotFoundError(f"Roster not found: {roster_file}")
            df = pd.read_csv(roster_file, encoding="utf-8-sig")
        
        # Normalize columns
        cols = {c.strip().lower(): c for c in df.columns}
        
        def col(name):
            return next((v for k, v in cols.items() if k == name.lower()), None)

        alias_col = col("ceo alias")
        ceo_col = col("ceo")
        company_col = col("company")
        
        if not (alias_col and ceo_col and company_col):
             raise KeyError("Missing required columns (CEO, Company, CEO Alias)")

        out = df[[alias_col, ceo_col, company_col]].copy()
        out.columns = ["alias", "ceo", "company"]
        for c in out.columns:
            out[c] = out[c].astype(str).str.strip()
        
        out = out[(out["alias"] != "") & (out["ceo"] != "") & (out["alias"] != "nan")]
        if out.empty:
            raise ValueError("No valid rows")
            
        print(f"✅ Loaded {len(out)} CEOs from roster")
        return out.drop_duplicates().reset_index(drop=True)
        
    except Exception as e:
        print(f"❌ Error loading roster: {e}")
        raise


def load_checkpoint(storage, checkpoint_path: str, expected_date: str) -> dict:
    default = {"last_index": -1, "articles": [], "date": expected_date}
    try:
        checkpoint = None
        if storage:
            if storage.file_exists(checkpoint_path):
                checkpoint = json.loads(storage.read_text(checkpoint_path))
        else:
            local_path = Path(checkpoint_path)
            if local_path.exists():
                with open(local_path, 'r') as f:
                    checkpoint = json.load(f)
        
        if checkpoint:
            if checkpoint.get("date") != expected_date:
                print(f"🔄 Checkpoint from {checkpoint.get('date')} ignored")
                return default
            print(f"📍 Resuming from checkpoint: index {checkpoint.get('last_index')}")
            return checkpoint
    except Exception as e:
        print(f"⚠️ Could not load checkpoint: {e}")
    return default


def save_checkpoint(storage, checkpoint_path: str, last_index: int, articles: list, date: str):
    checkpoint = {
        "last_index": last_index,
        "articles": articles,
        "date": date,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    try:
        if storage:
            storage.write_text(json.dumps(checkpoint), checkpoint_path)
        else:
            local_path = Path(checkpoint_path)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            with open(local_path, 'w') as f:
                json.dump(checkpoint, f)
        print(f"💾 Checkpoint saved: index {last_index}")
    except Exception as e:
        print(f"⚠️ Could not save checkpoint: {e}")


def delete_checkpoint(storage, checkpoint_path: str):
    try:
        if storage:
            if storage.file_exists(checkpoint_path):
                storage.delete_file(checkpoint_path)
        else:
            local_path = Path(checkpoint_path)
            if local_path.exists():
                local_path.unlink()
        print(f"🗑️ Checkpoint cleared")
    except Exception as e:
        print(f"⚠️ Could not delete checkpoint: {e}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch CEO news articles.")
    parser.add_argument("--date", type=str, default=None, help="YYYY-MM-DD")
    parser.add_argument("--bucket", type=str, default="risk-dashboard", help="GCS bucket")
    parser.add_argument("--roster", type=str, default=MAIN_ROSTER, help="Roster path")
    parser.add_argument("--output-dir", type=str, default=OUT_DIR, help="Output dir")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Batch size")
    parser.add_argument("--checkpoint-interval", type=int, default=25, help="Checkpoint interval")
    parser.add_argument("--no-resume", action="store_true", help="Ignore checkpoint")
    parser.add_argument("--local", action="store_true", help="Use local storage")
    args = parser.parse_args()
    
    out_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_path = f"{args.output_dir}/{out_date}-ceo-articles-modal.csv"
    checkpoint_path = f"{CHECKPOINT_DIR}/{out_date}-ceo-checkpoint.json"
    
    storage = None
    if args.local:
        print("📁 Using local file storage (--local flag)")
    else:
        print(f"☁️  Using Cloud Storage bucket: {args.bucket}")
        storage = CloudStorageManager(args.bucket)
    
    print(f"📅 Building articles for {out_date}")
    
    try:
        roster = read_roster(storage, args.roster)
        company_domains = load_roster_domains(storage, args.roster)
    except Exception as e:
        print(f"❌ FATAL: {e}")
        return 1

    if args.no_resume:
        all_rows = []
        start_index = 0
    else:
        ckpt = load_checkpoint(storage, checkpoint_path, out_date)
        all_rows = ckpt.get("articles", [])
        start_index = ckpt.get("last_index", -1) + 1

    end_index = min(start_index + args.batch_size, len(roster))
    
    if start_index >= len(roster):
        print(f"✅ All {len(roster)} CEOs already processed")
        delete_checkpoint(storage, checkpoint_path)
        return 0

    print(f"🚀 Processing {start_index + 1} to {end_index} of {len(roster)}")
    session = create_session()
    analyzer = SentimentIntensityAnalyzer()
    batch_start = time.time()

    for i in range(start_index, end_index):
        row = roster.iloc[i]
        alias = row["alias"]
        if not alias: continue
            
        print(f"[{i + 1}/{len(roster)}] {alias}")
        rows = build_articles_for_alias(session, alias, row["ceo"], row["company"], analyzer, company_domains)
        all_rows.extend(rows)
        
        if (i + 1) % args.checkpoint_interval == 0:
            save_checkpoint(storage, checkpoint_path, i, all_rows, out_date)
        time.sleep(SLEEP_SEC)

    save_checkpoint(storage, checkpoint_path, end_index - 1, all_rows, out_date)
    
    elapsed = time.time() - batch_start
    print(f"⚡ Batch complete: {end_index - start_index} CEOs in {elapsed:.1f}s")

    if end_index >= len(roster):
        print(f"✅ All {len(roster)} CEOs processed!")
        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
            columns=["ceo", "company", "title", "url", "source", "sentiment", "controlled"]
        )
        # Dedupe
        df = df.drop_duplicates(subset=["ceo", "title", "url"]).reset_index(drop=True)
        
        try:
            if storage:
                storage.write_csv(df, output_path, index=False)
                print(f"✅ Saved to Cloud Storage: {output_path}")
            else:
                out = Path(output_path)
                out.parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(out, index=False)
                print(f"✅ Saved locally: {out}")
            try:
                db_count = upsert_articles_mentions(df, "ceo", out_date)
                print(f"✅ DB upserted {db_count} CEO article rows")
            except Exception as e:
                print(f"⚠️ DB upsert failed: {e}")
            delete_checkpoint(storage, checkpoint_path)
        except Exception as e:
            print(f"❌ Error saving data: {e}")
            return 1
    else:
        print(f"⏳ {len(roster) - end_index} remaining. Run again to continue.")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
