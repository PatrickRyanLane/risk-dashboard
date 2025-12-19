#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Builds daily CEO articles from Google News RSS.
Features: batching, checkpointing, connection pooling, retry logic.

Sentiment rules (applied in order):
1. Force NEGATIVE if source is Reddit (user content tends to be critical)
2. Force NEGATIVE if title mentions CEO-specific trouble terms (fired, ousted, compensation, etc.)
3. Force NEUTRAL if title contains CEO name words that sound emotional (Rob, Savage, etc.)
4. Otherwise, use VADER on the cleaned headline

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

# ============================================================================
# CEO-SPECIFIC WORD FILTERING RULES
# These rules help ensure sentiment classification isn't skewed by:
# 1. CEO names that contain emotional words (e.g., "Rob Walton", "Dan Savage")
# 2. CEO-specific crisis terms that should always be flagged as negative
# ============================================================================

# Words in CEO headlines that should be STRIPPED before sentiment analysis
# Often these are parts of names or common phrases that would skew VADER
NEUTRALIZE_TITLE_TERMS = [
    r"\bflees\b",           # Often used figuratively or in names
    r"\bsavage\b",          # Common surname (e.g., Dan Savage)
    r"\brob\b",             # Common first name (e.g., Rob Walton)
    r"\bnicholas\s+lower\b", # Specific CEO name combination
    r"\bmad\s+money\b",     # Jim Cramer's show
    r"\bno\s+organic\b",    # About organic food availability
]
NEUTRALIZE_TITLE_RE = re.compile("|".join(NEUTRALIZE_TITLE_TERMS), flags=re.IGNORECASE)

# CEO-specific terms that should ALWAYS trigger NEGATIVE classification
# These indicate reputation risk for a CEO specifically
ALWAYS_NEGATIVE_TERMS = [
    # Compensation scrutiny (common CEO negative coverage)
    r"\bpaid\b", r"\bcompensation\b", r"\bpay\b",
    # Corporate governance issues
    r"\bmandate\b",
    # Leadership changes (usually negative for the departing CEO)
    r"\bexit(s)?\b", r"\bstep\s+down\b", r"\bsteps\s+down\b", r"\bremoved\b",
    # Skepticism/scrutiny language
    r"\bstill\b",  # "CEO still hasn't..." implies criticism
    r"\bturnaround\b",  # Company in trouble
    # Personal accusations
    r"\bface\b", r"\baccused\b", r"\bcommitted\b",
    r"\baware\b",  # "CEO was aware of..." implies cover-up
    # Financial/personal troubles
    r"\bloss\b", r"\bdivorce\b", r"\bbankruptcy\b",
    # Labor relations
    r"\bunion\s+buster\b",
    # Termination (in any direction)
    r"\bfired\b", r"\bfiring\b", r"\bfires\b",
    r"(?<!t)\bax(e|ed|es)?\b",  # "axed" but not "taxes"
    r"\bsack(ed|s)?\b", r"\boust(ed)?\b",
    # Stock performance
    r"\bplummeting\b",
]
ALWAYS_NEGATIVE_RE = re.compile("|".join(ALWAYS_NEGATIVE_TERMS), flags=re.IGNORECASE)


def _should_force_negative_title(title: str) -> bool:
    """
    Return True if title contains CEO-specific negative terms.
    
    Why: CEOs face unique reputational risks. Headlines about compensation,
    ousting, or accusations are almost always negative for the CEO's reputation,
    even if VADER might score them neutrally.
    
    Example: "CEO pay rises to $50M amid layoffs" might be neutral to VADER,
    but it's clearly negative for the CEO's reputation.
    """
    return bool(ALWAYS_NEGATIVE_RE.search(title or ""))


def _should_neutralize_title(title: str) -> bool:
    """
    Return True if the title contains terms that should neutralize sentiment.
    Used as a check before deciding to return neutral.
    """
    return bool(NEUTRALIZE_TITLE_RE.search(str(title or "")))


def _strip_neutral_terms(title: str) -> str:
    """
    Remove neutral terms from title before sentiment analysis.
    
    Why: Some CEO names contain words that VADER interprets as emotional.
    For example, "Rob Walton announces new initiative" would get a negative
    score because VADER sees "rob" as stealing. This strips those terms.
    
    Returns the cleaned title with normalized whitespace.
    """
    s = str(title or "")
    s = NEUTRALIZE_TITLE_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _is_reddit_source(source: str) -> bool:
    """
    Return True if the article source is Reddit.
    
    Why: Reddit discussions about CEOs tend to be negative/critical.
    For reputation risk monitoring, we treat all Reddit content as negative
    to match the SERP processing logic.
    
    Args:
        source: The source name from the RSS feed (e.g., "Reddit", "reddit.com")
    
    Returns:
        True if source appears to be Reddit
    """
    if not source:
        return False
    source_lower = source.lower().strip()
    return "reddit" in source_lower


from storage_utils import CloudStorageManager

# Paths
BASE = Path(__file__).parent.parent
MAIN_ROSTER = "rosters/main-roster.csv"
OUT_DIR = "data/processed_articles"
CHECKPOINT_DIR = "data/checkpoints"

USER_AGENT = "Mozilla/5.0 (compatible; CEO-NewsBot/1.0; +https://example.com/bot)"
RSS_TMPL = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"

# Tunables (env overrides)
MAX_PER_ALIAS = int(os.getenv("ARTICLES_MAX_PER_ALIAS", "25"))
SLEEP_SEC = float(os.getenv("ARTICLES_SLEEP_SEC", "0.25"))
DEFAULT_BATCH_SIZE = int(os.getenv("ARTICLES_BATCH_SIZE", "100"))


def create_session() -> requests.Session:
    """
    Create a requests session with connection pooling and retry logic.
    
    Connection pooling: Reuses TCP connections instead of creating new ones
    for each request, reducing latency by ~15-20%.
    
    Retry logic: Automatically retries failed requests with exponential backoff,
    preventing lost progress from transient network errors.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,                      # Max 3 retries
        backoff_factor=0.5,           # Wait 0.5s, 1s, 2s between retries
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these HTTP codes
        allowed_methods=["GET"],      # Only retry GET requests
        raise_on_status=False         # Don't raise exception, let us handle it
    )
    
    # Mount adapter with retry strategy to session
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,          # Connection pool size
        pool_maxsize=10               # Max connections per host
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    return session


def read_roster(storage=None, roster_path=MAIN_ROSTER) -> pd.DataFrame:
    """
    Reads roster from Cloud Storage or local file.
    Expected columns: CEO, Company, CEO Alias (case-insensitive)
    Returns DataFrame with columns: alias, ceo, company
    """
    try:
        if storage:
            if not storage.file_exists(roster_path):
                raise FileNotFoundError(
                    f"Roster not found in Cloud Storage: gs://{storage.bucket_name}/{roster_path}\n"
                    f"Please upload the roster file first."
                )
            print(f"📋 Loading roster from Cloud Storage: {roster_path}")
            df = storage.read_csv(roster_path)
        else:
            print(f"📋 Loading roster from local file: {roster_path}")
            roster_file = BASE / roster_path if not Path(roster_path).is_absolute() else Path(roster_path)
            
            if not roster_file.exists():
                raise FileNotFoundError(f"Missing roster file: {roster_file}")
            
            df = pd.read_csv(roster_file, encoding="utf-8-sig")
        
        # Normalize column names to lowercase for matching
        cols = {c.strip().lower(): c for c in df.columns}

        def col(name: str) -> str:
            for k, v in cols.items():
                if k == name.lower():
                    return v
            raise KeyError(f"Expected column '{name}' in roster")

        ceo_col = col("ceo")
        company_col = col("company")
        alias_col = col("ceo alias")

        out = df[[alias_col, ceo_col, company_col]].copy()
        out.columns = ["alias", "ceo", "company"]
        out["alias"] = out["alias"].astype(str).str.strip()
        out["ceo"] = out["ceo"].astype(str).str.strip()
        out["company"] = out["company"].astype(str).str.strip()
        
        out = out[(out["alias"] != "") & (out["ceo"] != "") & (out["alias"] != "nan")]
        if out.empty:
            raise ValueError("No valid CEO rows after normalization.")
        
        print(f"✅ Loaded {len(out)} CEOs from roster")
        return out.drop_duplicates().reset_index(drop=True)
        
    except Exception as e:
        print(f"❌ Error loading roster: {e}")
        raise


def load_checkpoint(storage, checkpoint_path: str, expected_date: str) -> dict:
    """
    Load checkpoint from Cloud Storage or local file.
    
    Automatically ignores checkpoints from previous days to ensure
    fresh data collection each day.
    
    Args:
        storage: CloudStorageManager instance or None
        checkpoint_path: Path to checkpoint file
        expected_date: Today's date (YYYY-MM-DD) - checkpoint must match
    
    Returns:
        Checkpoint dict with 'last_index' and 'articles' keys
    """
    default = {"last_index": -1, "articles": [], "date": expected_date}
    
    try:
        checkpoint = None
        if storage:
            if storage.file_exists(checkpoint_path):
                content = storage.read_text(checkpoint_path)
                checkpoint = json.loads(content)
        else:
            local_path = BASE / checkpoint_path
            if local_path.exists():
                with open(local_path, 'r') as f:
                    checkpoint = json.load(f)
        
        if checkpoint:
            checkpoint_date = checkpoint.get("date", "")
            
            # If checkpoint is from a different day, start fresh
            if checkpoint_date != expected_date:
                print(f"🔄 Checkpoint from {checkpoint_date} ignored (today is {expected_date})")
                return default
            
            print(f"📍 Resuming from checkpoint: index {checkpoint.get('last_index', -1)}")
            return checkpoint
            
    except Exception as e:
        print(f"⚠️ Could not load checkpoint: {e}")
    
    return default


def save_checkpoint(storage, checkpoint_path: str, last_index: int, articles: list, date: str):
    """Save checkpoint to Cloud Storage or local file."""
    checkpoint = {
        "last_index": last_index,
        "articles": articles,
        "date": date,  # Include date for day-boundary detection
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    
    try:
        if storage:
            storage.write_text(json.dumps(checkpoint), checkpoint_path)
        else:
            local_path = BASE / checkpoint_path
            local_path.parent.mkdir(parents=True, exist_ok=True)
            with open(local_path, 'w') as f:
                json.dump(checkpoint, f)
        print(f"💾 Checkpoint saved: index {last_index}")
    except Exception as e:
        print(f"⚠️ Could not save checkpoint: {e}")


def delete_checkpoint(storage, checkpoint_path: str):
    """Delete checkpoint after successful completion."""
    try:
        if storage:
            if storage.file_exists(checkpoint_path):
                storage.delete_file(checkpoint_path)
        else:
            local_path = BASE / checkpoint_path
            if local_path.exists():
                local_path.unlink()
        print(f"🗑️ Checkpoint cleared")
    except Exception as e:
        print(f"⚠️ Could not delete checkpoint: {e}")


def fetch_rss(session: requests.Session, query: str) -> feedparser.FeedParserDict:
    """Fetch RSS feed using session with connection pooling."""
    url = RSS_TMPL.format(query=quote_plus(query))
    resp = session.get(url, timeout=20)
    resp.raise_for_status()
    return feedparser.parse(resp.content)


def label_sentiment(analyzer: SentimentIntensityAnalyzer, title: str, source: str = "") -> str:
    """
    Classify sentiment with multi-stage filtering for CEO news.
    
    The classification follows this priority order:
    1. NEGATIVE: If source is Reddit (user-generated content tends to be critical)
    2. NEGATIVE: If headline mentions CEO-specific trouble terms
    3. NEUTRAL: If headline only contains neutral name/phrase terms
    4. VADER: Use sentiment analysis on cleaned headline
    
    Args:
        analyzer: VADER SentimentIntensityAnalyzer instance
        title: The article title to classify
        source: The article source (e.g., "Reddit", "CNN")
    
    Returns:
        "positive", "negative", or "neutral"
    """
    # Step 1: Check if source is Reddit → force NEGATIVE
    # Reddit discussions about CEOs tend to be critical/negative
    if _is_reddit_source(source):
        return "negative"
    
    # Step 2: Check for CEO-specific negative terms → force NEGATIVE
    # These catch headlines that indicate reputation risk for the CEO
    if _should_force_negative_title(title):
        return "negative"
    
    # Step 3: Check if title should be neutralized entirely
    # (e.g., consists mainly of name parts that sound emotional)
    if _should_neutralize_title(title):
        # Strip the neutral terms and check what's left
        cleaned = _strip_neutral_terms(title)
        if not cleaned or len(cleaned.split()) < 2:
            return "neutral"
    else:
        cleaned = title
    
    # Step 4: Run VADER on the (potentially cleaned) title
    cleaned = _strip_neutral_terms(title) if title else ""
    
    if not cleaned:
        return "neutral"
    
    scores = analyzer.polarity_scores(cleaned)
    compound = scores.get("compound", 0.0)
    
    # Unified thresholds: positive ≥0.15, negative ≤-0.10
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
    """Fetch and parse articles for a single CEO alias."""
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
        sent = label_sentiment(analyzer, title, source)
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
    parser = argparse.ArgumentParser(description="Fetch CEO news articles and analyze sentiment")
    parser.add_argument("--date", type=str, default=None, help="Date for data file (YYYY-MM-DD)")
    parser.add_argument("--bucket", type=str, default="risk-dashboard", help="GCS bucket name (default: risk-dashboard)")
    parser.add_argument("--roster", type=str, default=MAIN_ROSTER, help="Path to roster file")
    parser.add_argument("--output-dir", type=str, default=OUT_DIR, help="Output directory path")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Entities per batch")
    parser.add_argument("--checkpoint-interval", type=int, default=25, help="Save checkpoint every N entities")
    parser.add_argument("--no-resume", action="store_true", help="Start fresh, ignore checkpoint")
    parser.add_argument("--local", action="store_true", help="Use local file storage instead of GCS")
    args = parser.parse_args()
    
    out_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_path = f"{args.output_dir}/{out_date}-ceo-articles-modal.csv"
    checkpoint_path = f"{CHECKPOINT_DIR}/{out_date}-ceo-checkpoint.json"
    
    # Initialize storage (GCS by default, local with --local flag)
    storage = None
    if args.local:
        print("📁 Using local file storage (--local flag)")
    else:
        print(f"☁️  Using Cloud Storage bucket: {args.bucket}")
        storage = CloudStorageManager(args.bucket)
    
    print(f"📅 Building articles for {out_date}")
    print(f"📦 Batch size: {args.batch_size} | Checkpoint interval: {args.checkpoint_interval}")

    # Load roster (fail fast)
    try:
        roster = read_roster(storage, args.roster)
    except Exception as e:
        print(f"❌ FATAL: {e}")
        return 1

    # Load checkpoint
    if args.no_resume:
        print("🔄 Starting fresh (--no-resume)")
        all_rows = []
        start_index = 0
    else:
        checkpoint = load_checkpoint(storage, checkpoint_path, out_date)
        all_rows = checkpoint.get("articles", [])
        start_index = checkpoint.get("last_index", -1) + 1
    
    end_index = min(start_index + args.batch_size, len(roster))
    
    if start_index >= len(roster):
        print(f"✅ All {len(roster)} CEOs already processed")
        if all_rows:
            df = pd.DataFrame(all_rows)
            df = df.drop_duplicates(subset=["ceo", "title", "url"]).reset_index(drop=True)
            if storage:
                storage.write_csv(df, output_path, index=False)
            else:
                out_file = BASE / output_path
                out_file.parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(out_file, index=False)
            print(f"📊 Final output: {len(df)} articles")
        delete_checkpoint(storage, checkpoint_path)
        return 0

    print(f"🚀 Processing CEOs {start_index + 1} to {end_index} of {len(roster)}")

    # Create session with connection pooling and retry logic
    session = create_session()
    analyzer = SentimentIntensityAnalyzer()
    
    batch_start_time = time.time()

    for i in range(start_index, end_index):
        row = roster.iloc[i]
        alias = row["alias"]
        ceo = row["ceo"]
        company = row["company"]
        
        if not alias:
            continue
            
        print(f"[{i + 1}/{len(roster)}] {alias}")
        rows = build_articles_for_alias(session, alias, ceo, company, analyzer)
        all_rows.extend(rows)
        
        if (i + 1) % args.checkpoint_interval == 0:
            save_checkpoint(storage, checkpoint_path, i, all_rows, out_date)
        
        time.sleep(SLEEP_SEC)

    # Print batch stats
    batch_elapsed = time.time() - batch_start_time
    batch_count = end_index - start_index
    print(f"⚡ Batch: {batch_count} CEOs in {batch_elapsed:.1f}s ({batch_count/batch_elapsed:.1f}/sec)")

    save_checkpoint(storage, checkpoint_path, end_index - 1, all_rows, out_date)

    if end_index >= len(roster):
        print(f"✅ All {len(roster)} CEOs processed!")
        
        if all_rows:
            df = pd.DataFrame(all_rows)
            df = df.drop_duplicates(subset=["ceo", "title", "url"]).reset_index(drop=True)
        else:
            df = pd.DataFrame(columns=["ceo", "company", "title", "url", "source", "sentiment"])

        try:
            if storage:
                storage.write_csv(df, output_path, index=False)
                print(f"✅ Saved to Cloud Storage: gs://{storage.bucket_name}/{output_path}")
                public_url = storage.get_public_url(output_path)
                print(f"🌐 Public URL: {public_url}")
            else:
                out_file = BASE / output_path
                out_file.parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(out_file, index=False)
                print(f"✅ Saved locally: {out_file}")
            
            print(f"📊 Total articles: {len(df):,}")
            delete_checkpoint(storage, checkpoint_path)
            
        except Exception as e:
            print(f"❌ Error saving data: {e}")
            return 1
    else:
        remaining = len(roster) - end_index
        print(f"⏳ Batch complete. {remaining} CEOs remaining.")
        print(f"   Run again to continue from index {end_index}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
