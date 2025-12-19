#!/usr/bin/env python3
"""
Aggregate brand news sentiment by day/company.
Updated to read from and write to Google Cloud Storage.
"""

import argparse, csv, sys
from pathlib import Path
from datetime import date, timedelta
import pandas as pd

from storage_utils import CloudStorageManager

# Paths
ARTICLES_DIR = "data/processed_articles"
OUT_DIR = "data/processed_articles"
DAILY_INDEX = "data/daily_counts/brand-articles-daily-counts-chart.csv"

# Columns we will ALWAYS write for the daily index
INDEX_FIELDS = ["date","company","positive","neutral","negative","total","neg_pct"]

def iter_dates(from_str: str, to_str: str):
    d0 = date.fromisoformat(from_str)
    d1 = date.fromisoformat(to_str)
    if d1 < d0:
        raise SystemExit(f"--to ({to_str}) is before --from ({from_str})")
    d = d0
    one = timedelta(days=1)
    while d <= d1:
        yield d.isoformat()
        d += one

def read_articles(storage, dstr: str, articles_dir: str):
    """
    Read articles from Cloud Storage or local file
    """
    file_path = f"{articles_dir}/{dstr}-brand-articles-modal.csv"
    
    try:
        if storage:
            # Read from Cloud Storage
            if not storage.file_exists(file_path):
                print(f"[INFO] No headline file for {dstr} in Cloud Storage; nothing to aggregate.", flush=True)
                return []
            
            df = storage.read_csv(file_path)
            rows = []
            for _, row in df.iterrows():
                rows.append({
                    "company": str(row.get("company", "")).strip(),
                    "sentiment": str(row.get("sentiment", "")).strip().lower(),
                })
            return rows
        else:
            # Read from local file
            f = Path(file_path)
            if not f.exists():
                print(f"[INFO] No headline file for {dstr} at {f}; nothing to aggregate.", flush=True)
                return []

            rows = []
            with f.open("r", newline="", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    rows.append({
                        "company": (row.get("company") or "").strip(),
                        "sentiment": (row.get("sentiment") or "").strip().lower(),
                    })
            return rows
    except Exception as e:
        print(f"[WARN] Error reading articles for {dstr}: {e}")
        return []

def aggregate(rows):
    # counts per company {"company": {"positive": n, "neutral": n, "negative": n, "total": n}}
    agg = {}
    for r in rows:
        company = r["company"]
        if not company:
            continue
        s = r["sentiment"]
        bucket = agg.setdefault(company, {"positive":0,"neutral":0,"negative":0,"total":0})
        if s not in ("positive","neutral","negative"):
            s = "neutral"
        bucket[s] += 1
        bucket["total"] += 1
    return agg

def write_daily(storage, dstr: str, agg: dict, out_dir: str):
    """
    Write per-day file to Cloud Storage or local
    """
    output_path = f"{out_dir}/{dstr}-brand-articles-table.csv"
    
    # Create DataFrame
    rows = []
    for company, c in sorted(agg.items()):
        total = c["total"]
        neg_pct = (c["negative"] / total) if total else 0.0
        rows.append({
            "date": dstr,
            "company": company,
            "positive": c["positive"],
            "neutral": c["neutral"],
            "negative": c["negative"],
            "total": total,
            "neg_pct": f"{neg_pct:.6f}"
        })
    
    df = pd.DataFrame(rows)
    
    try:
        if storage:
            # Write to Cloud Storage
            storage.write_csv(df, output_path, index=False)
            print(f"[OK] Wrote to Cloud Storage: {output_path}")
        else:
            # Write to local file
            out = Path(output_path)
            out.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(out, index=False)
            print(f"[OK] Wrote {out}")
    except Exception as e:
        print(f"[ERROR] Failed to write daily file: {e}")

def upsert_daily_index(storage, dstr: str, agg: dict, daily_index_path: str):
    """
    Append/replace rows for a given date in the rolling index.
    """
    rows = []
    
    # Load existing rows
    try:
        if storage:
            # Read from Cloud Storage
            if storage.file_exists(daily_index_path):
                df = storage.read_csv(daily_index_path)
                rows = df.to_dict('records')
        else:
            # Read from local file
            index_file = Path(daily_index_path)
            if index_file.exists():
                with index_file.open(newline="", encoding="utf-8") as fh:
                    rows = list(csv.DictReader(fh))
    except Exception as e:
        print(f"[WARN] Could not load existing index: {e}")
        rows = []

    # Drop any rows for dstr, then append fresh
    rows = [r for r in rows if r.get("date") != dstr]
    for company, c in agg.items():
        total = c["total"]
        neg_pct = (c["negative"] / total) if total else 0.0
        rows.append({
            "date": dstr,
            "company": company,
            "positive": str(c["positive"]),
            "neutral": str(c["neutral"]),
            "negative": str(c["negative"]),
            "total": str(total),
            "neg_pct": f"{neg_pct:.6f}",
        })

    # Sort by date, then company
    rows.sort(key=lambda r: (r["date"], r["company"]))

    # Create DataFrame with only the columns we want
    cleaned = [{k: r.get(k, "") for k in INDEX_FIELDS} for r in rows]
    df = pd.DataFrame(cleaned)

    try:
        if storage:
            # Write to Cloud Storage
            storage.write_csv(df, daily_index_path, index=False)
            print(f"[OK] Updated Cloud Storage: {daily_index_path}")
        else:
            # Write to local file
            index_file = Path(daily_index_path)
            index_file.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(index_file, index=False)
            print(f"[OK] Updated {index_file}")
    except Exception as e:
        print(f"[ERROR] Failed to write index: {e}")

def process_one(storage, dstr: str, articles_dir: str, out_dir: str, daily_index_path: str):
    print(f"Processing {dstr}...")
    rows = read_articles(storage, dstr, articles_dir)
    if not rows:
        return
    agg = aggregate(rows)
    write_daily(storage, dstr, agg, out_dir)
    upsert_daily_index(storage, dstr, agg, daily_index_path)

def main():
    ap = argparse.ArgumentParser(description="Aggregate brand news sentiment by day/company.")
    ap.add_argument("--date", help="single YYYY-MM-DD to process")
    ap.add_argument("--from", dest="from_date", help="start YYYY-MM-DD (inclusive)")
    ap.add_argument("--to", dest="to_date", help="end YYYY-MM-DD (inclusive)")
    ap.add_argument("--bucket", type=str, default="risk-dashboard",
                   help="GCS bucket name (default: risk-dashboard)")
    ap.add_argument("--local", action="store_true",
                   help="Use local file storage instead of GCS")
    ap.add_argument("--articles-dir", default=ARTICLES_DIR, help="Articles directory path")
    ap.add_argument("--output-dir", default=OUT_DIR, help="Output directory path")
    ap.add_argument("--daily-index", default=DAILY_INDEX, help="Daily index file path")
    args = ap.parse_args()

    # Initialize storage (GCS by default, local with --local flag)
    storage = None
    if args.local:
        print("📁 Using local file storage (--local flag)")
    else:
        print(f"☁️  Using Cloud Storage bucket: {args.bucket}")
        storage = CloudStorageManager(args.bucket)

    if args.date:
        dates = [args.date]
    elif args.from_date and args.to_date:
        dates = list(iter_dates(args.from_date, args.to_date))
    else:
        dates = [date.today().isoformat()]

    for dstr in dates:
        process_one(storage, dstr, args.articles_dir, args.output_dir, args.daily_index)

if __name__ == "__main__":
    main()
