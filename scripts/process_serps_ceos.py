#!/usr/bin/env python3
"""
Process daily CEO SERP data.
Updated to read from and write to Google Cloud Storage.

Similar to brand SERPs processor but for CEO queries.

Outputs:
  1) Row-level processed SERPs:       data/processed_serps/{date}-ceo-serps-modal.csv
  2) Per-CEO daily aggregate:         data/processed_serps/{date}-ceo-serps-table.csv
  3) Rolling daily index:             data/daily_counts/ceo-serps-daily-counts-chart.csv
"""

from __future__ import annotations

import argparse
import io
import os, sys
from datetime import datetime
from typing import Dict, Set
from urllib.parse import urlparse
from pathlib import Path

import pandas as pd
import requests
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Add parent directory to path to import storage_utils
sys.path.append(str(Path(__file__).parent.parent))
from storage_utils import CloudStorageManager

# Config
S3_URL_TEMPLATE = (
    "https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/{date}-ceo-serps.csv"
)

MAIN_ROSTER_PATH = "rosters/main-roster.csv"
OUT_ROWS_DIR = "data/processed_serps"
OUT_DAILY_DIR = "data/processed_serps"
OUT_ROLLUP = "data/daily_counts/ceo-serps-daily-counts-chart.csv"

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

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Process daily CEO SERPs.")
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

def _hostname(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower()
        return host.replace("www.", "")
    except Exception:
        return ""

def _norm_token(s: str) -> str:
    return "".join(ch for ch in (s or "").lower() if ch.isalnum())

def load_roster_domains(storage, path: str = MAIN_ROSTER_PATH) -> Dict[str, Set[str]]:
    """Load controlled domains from roster, keyed by company name."""
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
    """Classify if a URL is controlled by a company"""
    host = _hostname(url)
    if not host:
        return False

    # Rule 1: Always-controlled platforms
    for good in ALWAYS_CONTROLLED_DOMAINS:
        if host == good or host.endswith("." + good):
            return True

    # Rule 2: Company-specific roster domains
    company_specific_domains = company_domains.get(company, set())
    for rd in company_specific_domains:
        if host == rd or host.endswith("." + rd):
            return True

    # Rule 3: Domain contains the brand token
    brand_token = _norm_token(company)
    if brand_token:
        host_parts = host.split('.')
        normalized_parts = [_norm_token(part) for part in host_parts if part]
        if brand_token in normalized_parts[:-1]:
            return True

    return False

def vader_label_on_title(analyzer: SentimentIntensityAnalyzer, title: str) -> tuple:
    s = analyzer.polarity_scores(title or "")
    c = s.get("compound", 0.0)
    if c >= 0.2:
        lab = "positive"
    elif c <= -0.1:
        lab = "negative"
    else:
        lab = "neutral"
    return c, lab

def process_for_date(storage, target_date: str, roster_path: str) -> None:
    print(f"[INFO] Processing CEO SERPs for {target_date} …")

    company_domains = load_roster_domains(storage, roster_path)

    url = S3_URL_TEMPLATE.format(date=target_date)
    raw = fetch_csv_from_s3(url)
    if raw is None or raw.empty:
        print(f"[WARN] No raw CEO SERP data available for {target_date}. Nothing to write.")
        return

    # CEO SERPs might have different columns - adjust as needed
    expected = ["ceo", "company", "position", "title", "link", "snippet"]
    for col in expected:
        if col not in raw.columns:
            raw[col] = ""

    analyzer = SentimentIntensityAnalyzer()

    processed_rows = []
    for _, row in raw.iterrows():
        ceo = str(row.get("ceo", "") or "").strip()
        company = str(row.get("company", "") or "").strip()
        if not ceo or not company:
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

        _, label = vader_label_on_title(analyzer, title)
        if FORCE_POSITIVE_IF_CONTROLLED and controlled:
            label = "positive"

        processed_rows.append({
            "date": target_date,
            "ceo": ceo,
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
    row_out_path = f"{OUT_ROWS_DIR}/{target_date}-ceo-serps-modal.csv"
    
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

    # Aggregate by CEO
    agg = (
        rows_df.groupby(["ceo", "company"], as_index=False)
        .agg(
            total=("ceo", "size"),
            controlled=("controlled", "sum"),
            negative_serp=("sentiment", lambda s: (s == "negative").sum()),
            neutral_serp=("sentiment", lambda s: (s == "neutral").sum()),
            positive_serp=("sentiment", lambda s: (s == "positive").sum()),
        )
    )
    agg.insert(0, "date", target_date)

    daily_out_path = f"{OUT_DAILY_DIR}/{target_date}-ceo-serps-table.csv"
    
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
            "ceo",
            "company",
            "total",
            "controlled",
            "negative_serp",
            "neutral_serp",
            "positive_serp",
        ]
        roll = roll[cols].sort_values(["date", "ceo"]).reset_index(drop=True)
        
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
