#!/usr/bin/env python3
"""
Fetch brand news articles from Google News RSS and analyze sentiment.
Features: batching, checkpointing, connection pooling, retry logic.

Output: data/processed_articles/YYYY-MM-DD-brand-articles-modal.csv
Checkpoint: data/checkpoints/YYYY-MM-DD-brand-checkpoint.json
"""
from __future__ import annotations

import argparse
import json, os, re, sys, time, urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from storage_utils import CloudStorageManager

# ============================================================================
# WORD FILTERING RULES
# ============================================================================
NEUTRALIZE_TITLE_TERMS = [
    r"(?i)\bgrand\b",           # Grand Hyatt, Grand Cherokee
    r"(?i)\bdiamond\b",         # Diamond Foods
    r"(?i)\bsell\b",            # Headlines about "selling" aren't inherently negative
    r"(?i)\blow\b",             # Low prices, Lowe's
    r"(?i)\bdream\b",           # DreamWorks
    r"(?i)\bdarling\b",         # Darling Ingredients
    r"(?i)\bwells\b",           # Wells Fargo
    r"(?i)\bbest\s+buy\b",      # Best Buy (positive brand name)
    r"(?i)\bkilled\b",          # Often used hyperbolically in headlines
    r"(?i)\bmlm\b",             # Multi-level marketing discussions
    r"(?i)\bmad\s+money\b",     # Jim Cramer's show
    r"(?i)\brate\s+cut\b",      # Interest rate discussions
    r"(?i)\bone\s+stop\s+shop\b",  # Stop & Shop stores
    r"(?i)\bfuneral\b",         # Service Corporation (funeral services)
    r"(?i)\bcremation\b",       # Service Corporation
    r"(?i)\bcemetery\b",        # Service Corporation
    r"(?i)\blimited\b",         # The Limited Brands
    r"(?i)\bno\s+organic\b",    # About organic food availability
    r"(?i)\brob\b",        # Potentially a person's name
    r"(?i)\blower\b",      # CEO with last name Lower
    r"(?i)\benergy\b",     # Lot of brands with energy in their name   
    r"(?i)\brebel\b",      # Potential product name
]
NEUTRALIZE_TITLE_RE = re.compile("|".join(NEUTRALIZE_TITLE_TERMS), flags=re.IGNORECASE)

LEGAL_TROUBLE_TERMS = [
    # Legal actions
    r"(?i)\blawsuit(s)?\b", r"(?i)\bsued\b", r"(?i)\bsuing\b", r"(?i)\blegal\b",
    r"(?i)\bsettlement(s)?\b", r"(?i)\bfine(d)?\b", r"(?i)\bclass[- ]action\b",
    # Regulatory bodies
    r"(?i)\bftc\b", r"(?i)\bsec\b", r"(?i)\bdoj\b", r"(?i)\bcfpb\b",
    # Corporate crises
    r"(?i)\bantitrust\b", r"(?i)\bban(s|ned)?\b", r"(?i)\bdata leaks?\b",
    r"(?i)\brecall(s|ed)?\b",
    r"(?i)\blayoff(s)?\b", r"(?i)\bexit(s)?\b", r"(?i)\bstep\s+down\b", r"(?i)\bsteps\s+down\b",
    # Investigations
    r"(?i)\bprobe(s|d)?\b", r"(?i)\binvestigation(s)?\b",
    r"(?i)\bsanction(s|ed)?\b", r"(?i)\bpenalt(y|ies)\b",
    # Scandals
    r"(?i)\bfraud\b", r"(?i)\bembezzl(e|ement)\b", r"(?i)\baccused\b", r"(?i)\bcommitted\b",
    r"(?i)\bdivorce\b", r"(?i)\bbankruptcy\b", r"(?i)\bapologizes\b", r"(?i)\bapology\b",
    # Financial Terms
    r"(?i)\bheadwinds\b", r"(?i)\bcontroversy\b",
]
LEGAL_TROUBLE_RE = re.compile("|".join(LEGAL_TROUBLE_TERMS), flags=re.IGNORECASE)


def _title_mentions_legal_trouble(title: str) -> bool:
    """Return True if title mentions legal trouble terms."""
    return bool(LEGAL_TROUBLE_RE.search(title or ""))


def _strip_neutral_terms(headline: str) -> str:
    """Remove neutral terms from a headline so they don't affect sentiment."""
    if not headline:
        return ""
    cleaned = NEUTRALIZE_TITLE_RE.sub(" ", headline)
    return " ".join(cleaned.split())


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

USER_AGENT = "Mozilla/5.0 (compatible; Brand-NewsBot/1.0; +https://example.com/bot)"

# Tunables
MAX_PER_ALIAS = int(os.getenv("ARTICLES_MAX_PER_ALIAS", "50"))
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


def google_news_rss(q: str) -> str:
    qs = urllib.parse.quote(q)
    return f"https://news.google.com/rss/search?q={qs}&hl=en-US&gl=US&ceid=US:en"


def classify(headline: str, analyzer: SentimentIntensityAnalyzer, source: str = "") -> str:
    """
    Classify sentiment with multi-stage filtering.
    """
    # 1. Force NEGATIVE for Reddit
    if _is_reddit_source(source):
        return "negative"
    
    # 2. Force NEGATIVE for legal trouble / crisis
    if _title_mentions_legal_trouble(headline):
        return "negative"
    
    # 3. Strip neutral terms
    cleaned = _strip_neutral_terms(headline or "")
    
    # If nothing meaningful remains, neutral
    if not cleaned or len(cleaned.split()) < 2:
        return "neutral"
    
    # 4. VADER on cleaned headline
    scores = analyzer.polarity_scores(cleaned)
    compound = scores["compound"]
    
    # Unified thresholds
    if compound >= 0.15:
        return "positive"
    if compound <= -0.10:
        return "negative"
    return "neutral"


def fetch_one(session: requests.Session, brand: str, analyzer, date: str) -> list[dict]:
    url = google_news_rss(brand)
    
    r = session.get(url, timeout=15)
    r.raise_for_status()
    
    # Robust parsing: Try lxml (fast XML), fallback to html.parser (standard lib)
    try:
        soup = BeautifulSoup(r.text, "xml")
    except Exception:
        soup = BeautifulSoup(r.text, "html.parser")
    
    out = []
    for item in soup.find_all("item"):
        title = (item.title.text or "").strip()
        link = (item.link.text or "").strip()
        # Extract real URL if it's a Google redirect
        try:
            if "url=" in link:
                parsed = urllib.parse.urlparse(link)
                link = urllib.parse.parse_qs(parsed.query).get("url", [link])[0]
        except Exception:
            pass
            
        source = (item.source.text or "").strip() if item.source else ""
        sent = classify(title, analyzer, source)
        
        out.append({
            "company": brand,
            "title": title,
            "url": link,
            "source": source,
            "date": date,
            "sentiment": sent
        })
    return out[:MAX_PER_ALIAS]


def load_companies_from_roster(storage, roster_path: str) -> list[str]:
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
        
        company_col = next((c for c in df.columns if c.strip().lower() == 'company'), None)
        if not company_col:
            raise ValueError("No 'Company' column found in roster")
        
        companies = df[company_col].dropna().astype(str).str.strip()
        companies = sorted(set(c for c in companies if c and c.lower() != 'nan'))
        
        print(f"✅ Loaded {len(companies)} companies from roster")
        return companies
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


def main():
    parser = argparse.ArgumentParser(description="Fetch brand news articles.")
    parser.add_argument("--date", type=str, default=None, help="YYYY-MM-DD")
    parser.add_argument("--bucket", type=str, default="risk-dashboard", help="GCS bucket name")
    parser.add_argument("--roster", type=str, default=MAIN_ROSTER, help="Roster path")
    parser.add_argument("--output-dir", type=str, default=OUT_DIR, help="Output dir")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Batch size")
    parser.add_argument("--checkpoint-interval", type=int, default=25, help="Checkpoint interval")
    parser.add_argument("--no-resume", action="store_true", help="Ignore checkpoint")
    parser.add_argument("--local", action="store_true", help="Use local file storage")
    args = parser.parse_args()
    
    date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_path = f"{args.output_dir}/{date}-brand-articles-modal.csv"
    checkpoint_path = f"{CHECKPOINT_DIR}/{date}-brand-checkpoint.json"
    
    storage = None
    if args.local:
        print("📁 Using local file storage (--local flag)")
    else:
        print(f"☁️  Using Cloud Storage bucket: {args.bucket}")
        storage = CloudStorageManager(args.bucket)
    
    print(f"📅 Processing articles for {date}")
    
    try:
        brands = load_companies_from_roster(storage, args.roster)
    except Exception as e:
        print(f"❌ FATAL: {e}")
        sys.exit(1)
    
    if args.no_resume:
        all_rows = []
        start_index = 0
    else:
        ckpt = load_checkpoint(storage, checkpoint_path, date)
        all_rows = ckpt.get("articles", [])
        start_index = ckpt.get("last_index", -1) + 1
    
    end_index = min(start_index + args.batch_size, len(brands))
    
    if start_index >= len(brands):
        print(f"✅ All {len(brands)} brands already processed")
        delete_checkpoint(storage, checkpoint_path)
        sys.exit(0)

    print(f"🚀 Processing {start_index + 1} to {end_index} of {len(brands)}")
    session = create_session()
    analyzer = SentimentIntensityAnalyzer()
    batch_start = time.time()

    for i in range(start_index, end_index):
        brand = brands[i]
        print(f"[{i + 1}/{len(brands)}] {brand}")
        try:
            rows = fetch_one(session, brand, analyzer, date)
            all_rows.extend(rows)
        except Exception as e:
            print(f"  ⚠️ Error fetching {brand}: {e}")
        
        if (i + 1) % args.checkpoint_interval == 0:
            save_checkpoint(storage, checkpoint_path, i, all_rows, date)
        time.sleep(SLEEP_SEC)

    # Save end of batch
    save_checkpoint(storage, checkpoint_path, end_index - 1, all_rows, date)
    
    elapsed = time.time() - batch_start
    count = end_index - start_index
    print(f"⚡ Batch complete: {count} brands in {elapsed:.1f}s")

    if end_index >= len(brands):
        print(f"✅ All {len(brands)} brands processed!")
        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
            columns=["company", "title", "url", "source", "date", "sentiment"]
        )
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
            sys.exit(1)
    else:
        print(f"⏳ {len(brands) - end_index} remaining. Run again to continue.")

if __name__ == "__main__":
    main()