#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Builds daily CEO articles from Google News RSS.
Features: batching, checkpointing, connection pooling, retry logic.

Output: data/processed_articles/YYYY-MM-DD-ceo-articles-modal.csv
Checkpoint: data/checkpoints/YYYY-MM-DD-ceo-checkpoint.json
"""

from __future__ import annotations
import os, time, html, sys, argparse, json, re
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

# ============================================================================
# WORD FILTERING RULES
# ============================================================================
NEUTRALIZE_TITLE_TERMS = [
    r"(?i)\bflees\b",           # Often used figuratively or in names
    r"(?i)\bsavage\b",          # Common surname (e.g., Dan Savage)
    r"(?i)\brob\b",             # Common first name (e.g., Rob Walton)
    r"(?i)\bnicholas\s+lower\b", # Specific CEO name combination
    r"(?i)\bmad\s+money\b",     # Jim Cramer's show
    r"(?i)\bno\s+organic\b",    # About organic food availability
    r"(?i)\brob\b",        # Potentially a person's name
    r"(?i)\blower\b",      # CEO with last name Lower
    r"(?i)\benergy\b",     # Lot of brands with energy in their name   
    r"(?i)\brebel\b",      # Potential product name
]
NEUTRALIZE_TITLE_RE = re.compile("|".join(NEUTRALIZE_TITLE_TERMS), flags=re.IGNORECASE)

ALWAYS_NEGATIVE_TERMS = [
    # Compensation scrutiny
    r"(?i)\bpaid\b", r"(?i)\bcompensation\b", r"(?i)\bpay\b", r"(?i)\bnet worth\b",
    # Corporate governance
    r"(?i)\bmandate\b",
    # Leadership changes
    r"(?i)\bexit(s)?\b", r"(?i)\bstep\s+down\b", r"(?i)\bsteps\s+down\b", r"(?i)\bremoved\b",
    # Skepticism/scrutiny
    r"(?i)\bstill\b", r"(?i)\bturnaround\b",
    # Personal accusations
    r"(?i)\bface\b", r"(?i)\bcontroversy\b", r"(?i)\baccused\b", r"(?i)\bcommitted\b", 
    r"(?i)\bapologizes\b", r"(?i)\bapology\b", r"(?i)\baware\b",
    # Financial/personal troubles
    r"(?i)\bloss\b", r"(?i)\bdivorce\b", r"(?i)\bbankruptcy\b",
    # Data security
    r"(?i)\bdata leaks?\b",
    # Labor relations
    r"(?i)\bunion\s+buster\b",
    # Termination
    r"(?i)\bfired\b", r"(?i)\bfiring\b", r"(?i)\bfires\b",
    r"(?<!t)\bax(e|ed|es)?\b", r"(?i)\bsack(ed|s)?\b", r"(?i)\boust(ed)?\b",
    # Stock performance
    r"(?i)\bplummeting\b",
]
ALWAYS_NEGATIVE_RE = re.compile("|".join(ALWAYS_NEGATIVE_TERMS), flags=re.IGNORECASE)


def _should_force_negative_title(title: str) -> bool:
    """Return True if title contains CEO-specific negative terms."""
    return bool(ALWAYS_NEGATIVE_RE.search(title or ""))


def _strip_neutral_terms(title: str) -> str:
    """Remove neutral terms from title before sentiment analysis."""
    s = str(title or "")
    s = NEUTRALIZE_TITLE_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _is_reddit_source(source: str) -> bool:
    """Return True if the article source is Reddit."""
    if not source:
        return False
    return "reddit" in source.lower()


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


def classify(title: str, analyzer: SentimentIntensityAnalyzer, source: str = "") -> str:
    """
    Classify sentiment with multi-stage filtering.
    """
    # 1. Force NEGATIVE for Reddit
    if _is_reddit_source(source):
        return "negative"
    
    # 2. Force NEGATIVE for CEO trouble terms
    if _should_force_negative_title(title):
        return "negative"
    
    # 3. Strip neutral terms
    cleaned = _strip_neutral_terms(title or "")
    
    # If nothing meaningful remains, neutral
    if not cleaned or len(cleaned.split()) < 2:
        return "neutral"
    
    # 4. VADER on cleaned headline
    scores = analyzer.polarity_scores(cleaned)
    compound = scores.get("compound", 0.0)
    
    # Unified thresholds
    if compound >= 0.15:
        return "positive"
    if compound <= -0.10:
        return "negative"
    return "neutral"


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


def build_articles_for_alias(session: requests.Session, alias: str, ceo: str, company: str, analyzer) -> list[dict]:
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
            
        sent = classify(title, analyzer, source)
        
        rows.append({
            "ceo": ceo,
            "company": company,
            "title": title,
            "url": link,
            "source": source,
            "sentiment": sent,
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
        rows = build_articles_for_alias(session, alias, row["ceo"], row["company"], analyzer)
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
            columns=["ceo", "company", "title", "url", "source", "sentiment"]
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
            delete_checkpoint(storage, checkpoint_path)
        except Exception as e:
            print(f"❌ Error saving data: {e}")
            return 1
    else:
        print(f"⏳ {len(roster) - end_index} remaining. Run again to continue.")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())