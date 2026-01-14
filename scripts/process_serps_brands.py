#!/usr/bin/env python3
"""
Process daily BRAND SERP data.
Updated to read from and write to Google Cloud Storage.

Outputs:
  1) Row-level processed SERPs:       data/processed_serps/{date}-brand-serps-modal.csv
  2) Per-company daily aggregate:     data/processed_serps/{date}-brand-serps-table.csv
  3) Rolling daily index:             data/daily_counts/brand-serps-daily-counts-chart.csv
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import os, sys
from datetime import datetime
from typing import Dict, Tuple, Set
from urllib.parse import urlparse
from pathlib import Path

import pandas as pd
import requests
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from storage_utils import CloudStorageManager

# Config / constants
S3_URL_TEMPLATE = (
    "https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/{date}-brand-serps.csv"
)

MAIN_ROSTER_PATH = "rosters/main-roster.csv"
OUT_ROWS_DIR = "data/processed_serps"
OUT_DAILY_DIR = "data/processed_serps"
OUT_ROLLUP = "data/daily_counts/brand-serps-daily-counts-chart.csv"

FORCE_POSITIVE_IF_CONTROLLED = True

ALWAYS_CONTROLLED_DOMAINS: Set[str] = {
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
    "play.google.com",
    "apps.apple.com",
}

# ============================================================================
# WORD FILTERING RULES
# Words/phrases to ignore for title-based sentiment classification
# ============================================================================
NEUTRALIZE_TITLE_TERMS = [
    # Brand names that contain emotional-sounding words
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

# Force-negative if the title mentions legal trouble
LEGAL_TROUBLE_TERMS = [
    # Legal actions
    r"(?i)\blawsuit(s)?\b", r"(?i)\bsued\b", r"(?i)\bsuing\b", r"(?i)\blegal\b",
    r"(?i)\bsettlement(s)?\b", r"(?i)\bfine(d)?\b", r"(?i)\bclass[- ]action\b",
    # Regulatory bodies (usually means trouble)
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
    #Financial Terms
    r"(?i)\bcontroversy\b", r"(?i)\bheadwinds\b",
]
LEGAL_TROUBLE_RE = re.compile("|".join(LEGAL_TROUBLE_TERMS), flags=re.IGNORECASE)


def _title_mentions_legal_trouble(title: str) -> bool:
    """Return True if title mentions legal trouble terms (force negative)."""
    return bool(LEGAL_TROUBLE_RE.search(title or ""))


def _should_neutralize_title(title: str) -> bool:
    """Return True if the title contains terms that should neutralize sentiment."""
    return bool(NEUTRALIZE_TITLE_RE.search(title or ""))

# Argument parsing
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Process daily brand SERPs.")
    ap.add_argument("--date", help="YYYY-MM-DD (defaults to today)", default=None)
    ap.add_argument("--bucket", type=str, default="risk-dashboard",
                   help="GCS bucket name (default: risk-dashboard)")
    ap.add_argument("--local", action="store_true",
                   help="Use local file storage instead of GCS")
    ap.add_argument("--roster", default=MAIN_ROSTER_PATH, help="Path to roster file")
    return ap.parse_args()

def get_target_date(arg_date: str | None) -> str:
    if arg_date:
        try:
            datetime.strptime(arg_date, "%Y-%m-%d")
            return arg_date
        except ValueError:
            pass
    return datetime.utcnow().strftime("%Y-%m-%d")

def fetch_csv_from_s3(url: str) -> pd.DataFrame | None:
    try:
        resp = requests.get(url, timeout=45)
        resp.raise_for_status()
        return pd.read_csv(io.StringIO(resp.text))
    except Exception as e:
        print(f"[WARN] Could not fetch {url} – {e}")
        return None

# Domain normalization
def _hostname(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower()
        return host.replace("www.", "")
    except Exception:
        return ""

def _norm_token(s: str) -> str:
    return "".join(ch for ch in (s or "").lower() if ch.isalnum())


def _is_brand_youtube_channel(company: str, url: str) -> bool:
    """
    Treat brand-owned YouTube channels as controlled if the path slug
    contains the normalized brand token.
    """
    if not url or not company:
        return False

    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().replace("www.", "")

    if host not in {"youtube.com", "m.youtube.com"}:
        return False

    brand_token = _norm_token(company)
    if not brand_token:
        return False

    path = (parsed.path or "").strip("/")

    if not path:
        return False

    if path.lower().startswith("user/"):
        slug = path[5:]
    elif path.startswith("@"):
        slug = path[1:]
    else:
        slug = path.split("/", 1)[0]

    if not slug:
        return False

    slug_token = _norm_token(slug)
    return bool(slug_token) and brand_token in slug_token

def load_roster_domains(storage, path: str = MAIN_ROSTER_PATH) -> Dict[str, Set[str]]:
    """
    Load controlled domains from roster, keyed by company name.
    """
    company_domains: Dict[str, Set[str]] = {}

    try:
        if storage:
            if not storage.file_exists(path):
                print(f"[WARN] Roster not found in Cloud Storage at {path}")
                return company_domains
            df = storage.read_csv(path)
        else:
            roster_file = Path(path)
            if not roster_file.exists():
                print(f"[WARN] Roster not found at {roster_file}")
                return company_domains
            df = pd.read_csv(roster_file, encoding="utf-8-sig")
        
        cols = {c.strip().lower(): c for c in df.columns}
        
        company_col = None
        for key in ["company"]:
            if key in cols:
                company_col = cols[key]
                break
        
        website_col = None
        for key in ["website", "websites", "domain", "url", "site", "homepage"]:
            if key in cols:
                website_col = cols[key]
                break
        
        if not company_col or not website_col:
            print(f"[WARN] Missing company or website column in roster")
            return company_domains
        
        print(f"[INFO] Loading controlled domains from roster...")
        
        for idx, row in df.iterrows():
            company = str(row[company_col]).strip()
            if not company or company.lower() == "nan":
                continue
            
            val = row[website_col]
            if pd.isna(val):
                continue
            
            val = str(val).strip()
            if not val or val.lower() == "nan":
                continue
            
            if company not in company_domains:
                company_domains[company] = set()
            
            urls = val.split("|")
            
            for url in urls:
                url = url.strip()
                if not url or url.lower() == "nan":
                    continue
                
                if not url.startswith(("http://", "https://")):
                    url = f"http://{url}"
                
                host = _hostname(url)
                if host and "." in host:
                    company_domains[company].add(host)
        
        total_domains = sum(len(domains) for domains in company_domains.values())
        print(f"[OK] Loaded {len(company_domains)} companies with {total_domains} total controlled domains")
                        
    except Exception as e:
        print(f"[WARN] Failed reading roster: {e}")

    return company_domains

def classify_control(company: str, url: str, company_domains: Dict[str, Set[str]]) -> bool:
    """
    Classify if a URL is controlled by a company.
    """
    host = _hostname(url)
    if not host:
        return False

    if _is_brand_youtube_channel(company, url):
        return True

    for good in ALWAYS_CONTROLLED_DOMAINS:
        if host == good or host.endswith("." + good):
            return True

    company_specific_domains = company_domains.get(company, set())
    for rd in company_specific_domains:
        if host == rd or host.endswith("." + rd):
            return True

    brand_token = _norm_token(company)
    if brand_token:
        host_parts = host.split('.')
        normalized_parts = [_norm_token(part) for part in host_parts if part]
        if brand_token in normalized_parts[:-1]:
            return True

    return False

def vader_label_on_title(analyzer: SentimentIntensityAnalyzer, title: str) -> str:
    """
    Apply VADER with custom thresholds.
    Unified thresholds: positive >= 0.15, negative <= -0.10
    """
    s = analyzer.polarity_scores(title or "")
    c = s.get("compound", 0.0)
    
    if c >= 0.15:
        return "positive"
    elif c <= -0.10:
        return "negative"
    else:
        return "neutral"

def process_for_date(storage, target_date: str, roster_path: str) -> None:
    print(f"[INFO] Processing brand SERPs for {target_date} …")

    company_domains = load_roster_domains(storage, roster_path)

    url = S3_URL_TEMPLATE.format(date=target_date)
    raw = fetch_csv_from_s3(url)
    if raw is None or raw.empty:
        print(f"[WARN] No raw brand SERP data available for {target_date}. Nothing to write.")
        return

    expected = ["company", "position", "title", "link", "snippet"]
    for col in expected:
        if col not in raw.columns:
            raw[col] = ""

    analyzer = SentimentIntensityAnalyzer()

    processed_rows = []
    for _, row in raw.iterrows():
        company = str(row.get("company", "") or "").strip()
        if not company:
            continue

        title = str(row.get("title", "") or "").strip()
        url = str(row.get("link", "") or "").strip()
        snippet = str(row.get("snippet", "") or "").strip()

        pos_val = row.get("position", 0)
        try:
            position = int(float(pos_val) if pos_val not in (None, "") else 0)
        except Exception:
            position = 0

        controlled = classify_control(company, url, company_domains)

        # --- Sentiment rules (deterministic order) ---
        host = _hostname(url)

        # 1) Force negative for reddit.com
        if host == "reddit.com" or (host and host.endswith(".reddit.com")):
            label = "negative"
        # 2) Force negative for Legal/Trouble terms
        elif _title_mentions_legal_trouble(title):
            label = "negative"
        # 3) Neutralize certain terms
        elif _should_neutralize_title(title):
            label = "neutral"
        else:
            # 4) VADER analysis on the raw title
            label = vader_label_on_title(analyzer, title)

            # 5) Force positive if controlled — but ONLY if we didn't already force negative above
            # (Note: This is applied after VADER but before final assignment, 
            # effectively overriding VADER but NOT overriding steps 1-3)
            if FORCE_POSITIVE_IF_CONTROLLED and controlled:
                label = "positive"

        processed_rows.append({
            "date": target_date,
            "company": company,
            "title": title,
            "url": url,
            "position": position,
            "snippet": snippet,
            "sentiment": label,
            "controlled": controlled,
        })

    if not processed_rows:
        print(f"[WARN] No processed rows for {target_date}.")
        return

    rows_df = pd.DataFrame(processed_rows)
    row_out_path = f"{OUT_ROWS_DIR}/{target_date}-brand-serps-modal.csv"
    
    try:
        if storage:
            storage.write_csv(rows_df, row_out_path, index=False)
            print(f"[OK] Wrote row-level SERPs to Cloud Storage: {row_out_path}")
        else:
            out_file = Path(row_out_path)
            out_file.parent.mkdir(parents=True, exist_ok=True)
            rows_df.to_csv(out_file, index=False)
            print(f"[OK] Wrote row-level SERPs: {out_file}")
    except Exception as e:
        print(f"[ERROR] Failed to write row-level SERPs: {e}")
        return

    agg = (
        rows_df.groupby("company", as_index=False)
        .agg(
            total=("company", "size"),
            controlled=("controlled", "sum"),
            negative_serp=("sentiment", lambda s: (s == "negative").sum()),
            neutral_serp=("sentiment", lambda s: (s == "neutral").sum()),
            positive_serp=("sentiment", lambda s: (s == "positive").sum()),
        )
    )
    agg.insert(0, "date", target_date)

    daily_out_path = f"{OUT_DAILY_DIR}/{target_date}-brand-serps-table.csv"
    
    try:
        if storage:
            storage.write_csv(agg, daily_out_path, index=False)
            print(f"[OK] Wrote daily aggregate to Cloud Storage: {daily_out_path}")
        else:
            out_file = Path(daily_out_path)
            out_file.parent.mkdir(parents=True, exist_ok=True)
            agg.to_csv(out_file, index=False)
            print(f"[OK] Wrote daily aggregate: {out_file}")
    except Exception as e:
        print(f"[ERROR] Failed to write daily aggregate: {e}")
        return

    # Update rolling index
    try:
        if storage:
            if storage.file_exists(OUT_ROLLUP):
                roll = storage.read_csv(OUT_ROLLUP)
                roll = roll[roll["date"] != target_date]
                roll = pd.concat([roll, agg], ignore_index=True)
            else:
                roll = agg.copy()
        else:
            rollup_file = Path(OUT_ROLLUP)
            if rollup_file.exists():
                roll = pd.read_csv(rollup_file)
                roll = roll[roll["date"] != target_date]
                roll = pd.concat([roll, agg], ignore_index=True)
            else:
                roll = agg.copy()

        cols = [
            "date",
            "company",
            "total",
            "controlled",
            "negative_serp",
            "neutral_serp",
            "positive_serp",
        ]
        roll = roll[cols].sort_values(["date", "company"]).reset_index(drop=True)
        
        if storage:
            storage.write_csv(roll, OUT_ROLLUP, index=False)
            print(f"[OK] Updated rolling index in Cloud Storage: {OUT_ROLLUP}")
        else:
            rollup_file = Path(OUT_ROLLUP)
            rollup_file.parent.mkdir(parents=True, exist_ok=True)
            roll.to_csv(rollup_file, index=False)
            print(f"[OK] Updated rolling index: {rollup_file}")
            
    except Exception as e:
        print(f"[ERROR] Failed to update rolling index: {e}")

def main() -> None:
    args = parse_args()
    date_str = get_target_date(args.date)
    
    # Initialize storage (GCS by default, local with --local flag)
    storage = None
    if args.local:
        print("📁 Using local file storage (--local flag)")
    else:
        print(f"☁️  Using Cloud Storage bucket: {args.bucket}")
        storage = CloudStorageManager(args.bucket)
    
    process_for_date(storage, date_str, args.roster)

if __name__ == "__main__":
    main()