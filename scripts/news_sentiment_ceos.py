#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Daily CEO News Sentiment – counts normalizer.
Updated to read from and write to Google Cloud Storage.

Output columns: date, ceo, company, positive, neutral, negative, total, neg_pct, theme, alias
"""

from __future__ import annotations
import argparse
import os, sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import pandas as pd

from storage_utils import CloudStorageManager

# Defaults
DEFAULT_ROSTER = "rosters/main-roster.csv"
DEFAULT_ARTICLES_DIR = "data/processed_articles"
DEFAULT_DAILY_DIR = "data/processed_articles"
DEFAULT_OUT = "data/daily_counts/ceo-articles-daily-counts-chart.csv"

def iso_today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def load_roster(storage, path: str) -> pd.DataFrame:
    """
    Expected headers (case-insensitive): CEO Alias (or alias), CEO, Company
    Returns: DataFrame with columns [alias, ceo, company]
    """
    try:
        if storage:
            # Read from Cloud Storage
            if not storage.file_exists(path):
                raise FileNotFoundError(f"Roster file not found in Cloud Storage: {path}")
            df = storage.read_csv(path)
        else:
            # Read from local file
            roster_file = Path(path)
            if not roster_file.exists():
                raise FileNotFoundError(f"Roster file not found: {roster_file}")
            df = pd.read_csv(roster_file, encoding="utf-8-sig")

        cols = {c.strip().lower(): c for c in df.columns}

        def col(*names: str) -> str:
            """Find first matching column (case-insensitive)"""
            for name in names:
                for k, v in cols.items():
                    if k == name.lower():
                        return v
            raise KeyError(f"Expected one of {names} columns in roster")

        # Try different column name variations
        alias_col = col("ceo alias", "alias")
        ceo_col = col("ceo")
        company_col = col("company")

        out = df[[alias_col, ceo_col, company_col]].copy()
        out.columns = ["alias", "ceo", "company"]
        
        # normalize strings
        for c in ["alias", "ceo", "company"]:
            out[c] = out[c].astype(str).fillna("").str.strip()
        
        # Filter valid rows
        out = out[(out["alias"] != "") & (out["ceo"] != "") & (out["alias"] != "nan")]
        out = out.drop_duplicates(subset=["ceo"]).reset_index(drop=True)
        
        if out.empty:
            raise ValueError("No valid CEO rows after normalization.")
        
        print(f"✅ Loaded {len(out)} CEOs from roster")
        return out
        
    except Exception as e:
        print(f"❌ Error loading roster: {e}")
        raise

def load_articles(storage, articles_dir: str, date_str: str) -> pd.DataFrame:
    """
    Reads YYYY-MM-DD-ceo-articles-modal.csv if present.
    Expected columns (case-insensitive): ceo, company, title, url, source, sentiment
    Returns empty DataFrame (with expected columns) if file missing/empty.
    """
    file_path = f"{articles_dir}/{date_str}-ceo-articles-modal.csv"
    cols = ["ceo", "company", "title", "url", "source", "sentiment"]
    
    try:
        if storage:
            # Read from Cloud Storage
            if not storage.file_exists(file_path):
                return pd.DataFrame(columns=cols)
            
            df = storage.read_csv(file_path)
        else:
            # Read from local file
            f = Path(file_path)
            if not f.exists():
                return pd.DataFrame(columns=cols)
            df = pd.read_csv(f)
        
        if df.empty:
            return pd.DataFrame(columns=cols)

        # normalize
        df = df.rename(columns={c: c.lower() for c in df.columns})
        for c in cols:
            if c not in df.columns:
                df[c] = ""
        for c in ["ceo", "company", "title", "url", "source", "sentiment"]:
            df[c] = df[c].astype(str).fillna("").str.strip()
        df["sentiment"] = df["sentiment"].str.lower()
        
        print(f"📄 Loaded {len(df)} articles for {date_str}")
        return df[cols]
        
    except Exception as e:
        print(f"[WARN] Error loading articles: {e}")
        return pd.DataFrame(columns=cols)

def aggregate_counts(roster: pd.DataFrame, articles: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """
    Returns a DataFrame with legacy columns:
    date, ceo, company, positive, neutral, negative, total, neg_pct, theme, alias
    """
    # Start with roster so we always have one row per CEO (even with no headlines).
    base = roster.copy()

    # Sentiment counts per CEO
    if not articles.empty:
        grp = (
            articles.assign(sentiment=articles["sentiment"].str.lower())
            .groupby("ceo", dropna=False)["sentiment"]
            .value_counts()
            .unstack(fill_value=0)
            .rename(columns={"positive": "positive", "neutral": "neutral", "negative": "negative"})
        )
        # Ensure all three columns exist
        for col in ["positive", "neutral", "negative"]:
            if col not in grp.columns:
                grp[col] = 0
        grp = grp[["positive", "neutral", "negative"]]
        base = base.merge(grp, how="left", left_on="ceo", right_index=True)
    else:
        base["positive"] = 0
        base["neutral"] = 0
        base["negative"] = 0

    base[["positive", "neutral", "negative"]] = base[["positive", "neutral", "negative"]].fillna(0).astype(int)
    base["total"] = base["positive"] + base["neutral"] + base["negative"]
    base["neg_pct"] = base.apply(
        lambda r: round(100.0 * (r["negative"] / r["total"]), 1) if r["total"] > 0 else 0.0, axis=1
    )

    # Optional: theme column
    base["theme"] = ""

    # Reorder + add date
    out = base[["ceo", "company", "positive", "neutral", "negative", "total", "neg_pct", "theme", "alias"]].copy()
    out.insert(0, "date", date_str)
    return out

def write_daily_file(storage, daily_dir: str, date_str: str, daily_rows: pd.DataFrame) -> str:
    """
    Writes YYYY-MM-DD-ceo-articles-table.csv
    """
    path = f"{daily_dir}/{date_str}-ceo-articles-table.csv"
    
    try:
        if storage:
            # Write to Cloud Storage
            storage.write_csv(daily_rows, path, index=False)
            print(f"✅ Wrote to Cloud Storage: {path}")
        else:
            # Write to local file
            daily_path = Path(path)
            daily_path.parent.mkdir(parents=True, exist_ok=True)
            daily_rows.to_csv(daily_path, index=False)
            print(f"✅ Wrote {daily_path}")
        
        return path
    except Exception as e:
        print(f"❌ Error writing daily file: {e}")
        raise

def upsert_master_index(storage, out_path: str, date_str: str, daily_rows: pd.DataFrame) -> str:
    """
    Replaces rows for date_str in master index; creates the file if missing.
    """
    try:
        if storage:
            # Read from Cloud Storage
            if storage.file_exists(out_path):
                master = storage.read_csv(out_path)
                master = master.rename(columns={c: c.lower() for c in master.columns})
                # Normalize legacy headers if needed
                expected = ["date", "ceo", "company", "positive", "neutral", "negative", "total", "neg_pct", "theme", "alias"]
                for col in expected:
                    if col not in master.columns:
                        master[col] = [] if col in ["theme", "alias"] else 0
                master = master[expected]
                # Drop existing rows for this date and append fresh ones
                master = master[master["date"].astype(str) != date_str]
                master = pd.concat([master, daily_rows], ignore_index=True)
            else:
                master = daily_rows.copy()
        else:
            # Read from local file
            index_file = Path(out_path)
            if index_file.exists():
                master = pd.read_csv(index_file)
                master = master.rename(columns={c: c.lower() for c in master.columns})
                expected = ["date", "ceo", "company", "positive", "neutral", "negative", "total", "neg_pct", "theme", "alias"]
                for col in expected:
                    if col not in master.columns:
                        master[col] = [] if col in ["theme", "alias"] else 0
                master = master[expected]
                master = master[master["date"].astype(str) != date_str]
                master = pd.concat([master, daily_rows], ignore_index=True)
            else:
                master = daily_rows.copy()

        # Sort for readability
        master["date"] = master["date"].astype(str)
        master = master.sort_values(["date", "ceo"]).reset_index(drop=True)
        
        if storage:
            # Write to Cloud Storage
            storage.write_csv(master, out_path, index=False)
            print(f"✅ Updated Cloud Storage: {out_path} ({len(master):,} rows)")
        else:
            # Write to local file
            index_file = Path(out_path)
            index_file.parent.mkdir(parents=True, exist_ok=True)
            master.to_csv(index_file, index=False)
            print(f"✅ Updated {index_file} ({len(master):,} rows)")
        
        return out_path
        
    except Exception as e:
        print(f"❌ Error writing master index: {e}")
        raise

def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build daily CEO sentiment counts.")
    p.add_argument("--date", default=iso_today_utc(), help="Target date (YYYY-MM-DD). Default = today UTC.")
    p.add_argument("--roster", default=DEFAULT_ROSTER, help="Path to roster file")
    p.add_argument("--articles-dir", default=DEFAULT_ARTICLES_DIR, help="Folder with daily articles CSVs")
    p.add_argument("--daily-dir", default=DEFAULT_DAILY_DIR, help="Folder to write per-day CSVs")
    p.add_argument("--out", default=DEFAULT_OUT, help="Path to write/append master index")
    p.add_argument("--bucket", type=str, default="risk-dashboard",
                  help="GCS bucket name (default: risk-dashboard)")
    p.add_argument("--local", action="store_true",
                  help="Use local file storage instead of GCS")
    return p.parse_args(argv)

def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv)

    # Validate date
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        raise SystemExit(f"Invalid --date '{args.date}'. Expected YYYY-MM-DD.")

    # Initialize storage (GCS by default, local with --local flag)
    storage = None
    if args.local:
        print("📁 Using local file storage (--local flag)")
    else:
        print(f"☁️  Using Cloud Storage bucket: {args.bucket}")
        storage = CloudStorageManager(args.bucket)

    try:
        roster = load_roster(storage, args.roster)
        articles = load_articles(storage, args.articles_dir, args.date)
        daily_rows = aggregate_counts(roster, articles, args.date)

        daily_path = write_daily_file(storage, args.daily_dir, args.date, daily_rows)
        master_path = upsert_master_index(storage, args.out, args.date, daily_rows)

        print(f"\n✅ Success!")
        print(f"   Per-day file: {daily_path}")
        print(f"   Master index: {master_path}")
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
