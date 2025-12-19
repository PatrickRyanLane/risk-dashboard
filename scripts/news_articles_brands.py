#!/usr/bin/env python3
"""
Fetch brand news articles from Google News RSS and analyze sentiment.
Features: batching, checkpointing, connection pooling, retry logic.

Sentiment rules (applied in order):
1. Force NEGATIVE if source is Reddit (user content tends to be critical)
2. Force NEGATIVE if title mentions legal trouble (lawsuits, recalls, etc.)
3. Force NEUTRAL if title contains brand name words that sound emotional (Grand, Diamond, etc.)
4. Otherwise, use VADER on the cleaned headline

Output: data/processed_articles/YYYY-MM-DD-brand-articles-modal.csv
Checkpoint: data/checkpoints/YYYY-MM-DD-brand-checkpoint.json
"""

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

# ============================================================================
# WORD FILTERING RULES
# These rules help ensure sentiment classification isn't skewed by:
# 1. Brand names that contain emotional words (e.g., "Best Buy", "Grand Hyatt")
# 2. Legal/crisis terms that should always be flagged as negative
# ============================================================================

# Words in titles that should NOT contribute to sentiment (often part of brand names)
# These get stripped before VADER analysis
NEUTRALIZE_TITLE_TERMS = [
    r"\bgrand\b",           # Grand Hyatt, Grand Cherokee
    r"\bdiamond\b",         # Diamond Foods
    r"\bsell\b",            # Headlines about "selling" aren't inherently negative
    r"\blow\b",             # Low prices, Lowe's
    r"\bdream\b",           # DreamWorks
    r"\bdarling\b",         # Darling Ingredients
    r"\bwells\b",           # Wells Fargo
    r"\bbest\s+buy\b",      # Best Buy (positive brand name)
    r"\bkilled\b",          # Often used hyperbolically in headlines
    r"\bmlm\b",             # Multi-level marketing discussions
    r"\bmad\s+money\b",     # Jim Cramer's show
    r"\brate\s+cut\b",      # Interest rate discussions
    r"\bone\s+stop\s+shop\b",  # Stop & Shop stores
    r"\bfuneral\b",         # Service Corporation (funeral services)
    r"\bcremation\b",       # Service Corporation
    r"\bcemetery\b",        # Service Corporation
    r"\blimited\b",         # The Limited Brands
    r"\bno\s+organic\b",    # About organic food availability
]
NEUTRALIZE_TITLE_RE = re.compile("|".join(NEUTRALIZE_TITLE_TERMS), flags=re.IGNORECASE)

# Force-NEGATIVE if the title mentions legal trouble or crisis situations
# These override any other sentiment classification
LEGAL_TROUBLE_TERMS = [
    # Legal actions
    r"\blawsuit(s)?\b", r"\bsued\b", r"\bsuing\b",
    r"\bsettlement(s)?\b", r"\bfine(d)?\b", r"\bclass[- ]action\b",
    # Regulatory bodies (usually means trouble)
    r"\bftc\b", r"\bsec\b", r"\bdoj\b", r"\bcfpb\b",
    # Corporate crises
    r"\bantitrust\b", r"\bban(s|ned)?\b",
    r"\brecall(s|ed)?\b",
    r"\blayoff(s)?\b", r"\bexit(s)?\b", r"\bstep\s+down\b", r"\bsteps\s+down\b",
    # Investigations
    r"\bprobe(s|d)?\b", r"\binvestigation(s)?\b",
    r"\bsanction(s|ed)?\b", r"\bpenalt(y|ies)\b",
    # Scandals
    r"\bfraud\b", r"\bembezzl(e|ement)\b", r"\baccused\b", r"\bcommitted\b",
    r"\bdivorce\b", r"\bbankruptcy\b",
]
LEGAL_TROUBLE_RE = re.compile("|".join(LEGAL_TROUBLE_TERMS), flags=re.IGNORECASE)


def _title_mentions_legal_trouble(title: str) -> bool:
    """
    Return True if title mentions legal trouble terms.
    
    Why: Headlines about lawsuits, recalls, regulatory actions, etc. should
    always be classified as negative for reputation risk purposes, regardless
    of the overall tone of the headline.
    
    Example: "Company settles lawsuit for $50M" might sound neutral to VADER,
    but it's clearly negative for the company's reputation.
    """
    return bool(LEGAL_TROUBLE_RE.search(title or ""))


def _strip_neutral_terms(headline: str) -> str:
    """
    Remove configured neutral terms from a headline so they don't affect sentiment.
    
    Why: Many brand names contain words that VADER interprets as emotional.
    For example, "Best Buy reports quarterly earnings" would get a positive
    score because of "Best" - but that's just the company name!
    
    This function removes those terms before sentiment analysis.
    """
    if not headline:
        return ""
    # Replace them with a space, then normalize whitespace
    cleaned = NEUTRALIZE_TITLE_RE.sub(" ", headline)
    return " ".join(cleaned.split())


def _should_neutralize_title(title: str) -> bool:
    """
    Return True if the title contains terms that should neutralize sentiment.
    
    Used as a secondary check - if the entire cleaned title becomes empty
    or very short after stripping, we return neutral instead of guessing.
    """
    return bool(NEUTRALIZE_TITLE_RE.search(title or ""))


def _is_reddit_source(source: str) -> bool:
    """
    Return True if the article source is Reddit.
    
    Why: Reddit discussions about companies tend to be negative/critical.
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

USER_AGENT = "Mozilla/5.0 (compatible; Brand-NewsBot/1.0; +https://example.com/bot)"

# Tunables (env overrides)
MAX_PER_ALIAS = int(os.getenv("ARTICLES_MAX_PER_ALIAS", "50"))
SLEEP_SEC = float(os.getenv("ARTICLES_SLEEP_SEC", "0.25"))
DEFAULT_BATCH_SIZE = int(os.getenv("ARTICLES_BATCH_SIZE", "100"))


def create_session() -> requests.Session:
    """
    Create a requests session with connection pooling and retry logic.
    
    Connection pooling: Reuses TCP connections instead of creating new ones
    for each request, reducing latency by ~15-20%.
    
    Retry logic: Automatically retries failed requests with exponential backoff,
    preventing lost progress on transient network errors.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    
    retry_strategy = Retry(
        total=3,                      # Max 3 retries
        backoff_factor=0.5,           # Wait 0.5s, 1s, 2s between retries
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    return session


def google_news_rss(q):
    qs = urllib.parse.quote(q)
    return f"https://news.google.com/rss/search?q={qs}&hl=en-US&gl=US&ceid=US:en"


def classify(headline: str, analyzer: SentimentIntensityAnalyzer, source: str = "") -> str:
    """
    Classify sentiment with multi-stage filtering for brand news.
    
    The classification follows this priority order:
    1. NEGATIVE: If source is Reddit (user-generated content tends to be critical)
    2. NEGATIVE: If headline mentions legal trouble (lawsuits, recalls, etc.)
    3. NEUTRAL: If headline only contains neutral brand terms
    4. VADER: Use sentiment analysis on cleaned headline
    
    Args:
        headline: The article title to classify
        analyzer: VADER SentimentIntensityAnalyzer instance
        source: The article source (e.g., "Reddit", "CNN")
    
    Returns:
        "positive", "negative", or "neutral"
    """
    # Step 1: Check if source is Reddit → force NEGATIVE
    # Reddit discussions about companies tend to be critical/negative
    if _is_reddit_source(source):
        return "negative"
    
    # Step 2: Check for legal trouble terms → force NEGATIVE
    # This catches headlines that might sound neutral but indicate reputation risk
    if _title_mentions_legal_trouble(headline):
        return "negative"
    
    # Step 3: Strip neutral brand-name terms before sentiment analysis
    cleaned = _strip_neutral_terms(headline or "")
    
    # If nothing meaningful remains after stripping, return neutral
    if not cleaned or len(cleaned.split()) < 2:
        return "neutral"
    
    # Step 4: Run VADER sentiment on the cleaned headline
    scores = analyzer.polarity_scores(cleaned)
    compound = scores["compound"]
    
    # Unified thresholds: positive ≥0.15, negative ≤-0.10
    if compound >= 0.15:
        return "positive"
    if compound <= -0.10:
        return "negative"
    return "neutral"


def fetch_one(session: requests.Session, brand: str, analyzer, date: str) -> list[dict]:
    """Fetch articles for a single brand using pooled session."""
    url = google_news_rss(f'"{brand}"')
    r = session.get(url, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "xml")
    out = []
    for item in soup.find_all("item"):
        title = (item.title.text or "").strip()
        link = (item.link.text or "").strip()
        try:
            if "url=" in link:
                link = urllib.parse.parse_qs(urllib.parse.urlparse(link).query).get("url", [link])[0]
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


def load_companies_from_roster(storage=None, roster_path=MAIN_ROSTER) -> list[str]:
    """
    Load unique company names from roster (Cloud Storage or local).
    Fails fast with clear error if roster not found.
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
                raise FileNotFoundError(f"Roster not found: {roster_file}")
            
            df = pd.read_csv(roster_file, encoding="utf-8-sig")
        
        # Find company column (case-insensitive)
        company_col = None
        for col in df.columns:
            if col.strip().lower() == 'company':
                company_col = col
                break
        
        if not company_col:
            raise ValueError("No 'Company' column found in roster")
        
        companies = df[company_col].dropna().astype(str).str.strip()
        companies = sorted(set(c for c in companies if c and c != 'nan'))
        
        print(f"✅ Loaded {len(companies)} companies from roster")
        return companies
            
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


def main():
    parser = argparse.ArgumentParser(description="Fetch brand news articles and analyze sentiment")
    parser.add_argument("--date", type=str, default=None, help="Date for data file (YYYY-MM-DD)")
    parser.add_argument("--bucket", type=str, default="risk-dashboard", help="GCS bucket name (default: risk-dashboard)")
    parser.add_argument("--roster", type=str, default=MAIN_ROSTER, help="Path to roster file")
    parser.add_argument("--output-dir", type=str, default=OUT_DIR, help="Output directory")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Entities per batch")
    parser.add_argument("--checkpoint-interval", type=int, default=25, help="Save checkpoint every N entities")
    parser.add_argument("--no-resume", action="store_true", help="Start fresh, ignore checkpoint")
    parser.add_argument("--local", action="store_true", help="Use local file storage instead of GCS")
    args = parser.parse_args()
    
    date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_path = f"{args.output_dir}/{date}-brand-articles-modal.csv"
    checkpoint_path = f"{CHECKPOINT_DIR}/{date}-brand-checkpoint.json"
    
    # Initialize storage (GCS by default, local with --local flag)
    storage = None
    if args.local:
        print("📁 Using local file storage (--local flag)")
    else:
        print(f"☁️  Using Cloud Storage bucket: {args.bucket}")
        storage = CloudStorageManager(args.bucket)
    
    print(f"📅 Processing articles for date: {date}")
    print(f"📦 Batch size: {args.batch_size} | Checkpoint interval: {args.checkpoint_interval}")
    
    # Load companies (fail fast)
    try:
        brands = load_companies_from_roster(storage, args.roster)
    except Exception as e:
        print(f"❌ FATAL: {e}")
        sys.exit(1)
    
    # Load checkpoint
    if args.no_resume:
        print("🔄 Starting fresh (--no-resume)")
        all_rows = []
        start_index = 0
    else:
        checkpoint = load_checkpoint(storage, checkpoint_path, date)
        all_rows = checkpoint.get("articles", [])
        start_index = checkpoint.get("last_index", -1) + 1
    
    end_index = min(start_index + args.batch_size, len(brands))
    
    if start_index >= len(brands):
        print(f"✅ All {len(brands)} brands already processed")
        if all_rows:
            df = pd.DataFrame(all_rows)
            if storage:
                storage.write_csv(df, output_path, index=False)
            else:
                out_file = BASE / output_path
                out_file.parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(out_file, index=False)
            print(f"📊 Final output: {len(df)} articles")
        delete_checkpoint(storage, checkpoint_path)
        sys.exit(0)

    print(f"🚀 Processing brands {start_index + 1} to {end_index} of {len(brands)}")

    # Create session with connection pooling and retry logic
    session = create_session()
    analyzer = SentimentIntensityAnalyzer()
    
    batch_start_time = time.time()

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

    # Save checkpoint for this batch
    save_checkpoint(storage, checkpoint_path, end_index - 1, all_rows, date)
    
    # Print batch stats
    batch_elapsed = time.time() - batch_start_time
    batch_count = end_index - start_index
    print(f"⚡ Batch complete: {batch_count} brands in {batch_elapsed:.1f}s ({batch_count/batch_elapsed:.1f}/sec)")

    # Check if done
    if end_index >= len(brands):
        print(f"✅ All {len(brands)} brands processed!")
        
        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
            columns=["company", "title", "url", "source", "date", "sentiment"]
        )
        
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
            sys.exit(1)
    else:
        remaining = len(brands) - end_index
        print(f"⏳ {remaining} brands remaining. Run again to continue.")


if __name__ == "__main__":
    main()
