#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Daily CEO News Sentiment — counts normalizer (restored legacy outputs). 

WHAT THIS DOES
--------------
- Reads CEO/company/alias data from rosters/main-roster.csv.
- Reads today's headline articles (if present) and aggregates per-CEO counts.
- Writes BOTH of the legacy artifacts the dashboard expects:
    1) Per-day file:        data/processed_articles/YYYY-MM-DD-ceo-articles-table.csv
    2) Master index file:   data/daily_counts/ceo-articles-daily-counts-chart.csv   (append/replace rows for that date)

BEHAVIOR WHEN THERE ARE NO ARTICLES
-----------------------------------
- If `data/processed_articles/YYYY-MM-DD-ceo-articles-modal.csv` is missing or empty,
  we still create rows (0 positive / 0 neutral / 0 negative) for every CEO
  in the roster, so the date appears in the dashboard date picker.

OUTPUT COLUMNS (match legacy)
------------------------------
date, ceo, company, positive, neutral, negative, total, neg_pct, theme, alias

USAGE (optional flags)
----------------------
python news_sentiment_ceos.py \
  --date 2025-09-18 \
  --roster rosters/main-roster.csv \
  --articles-dir data/processed_articles \
  --daily-dir data/processed_articles \
  --out data/daily_counts/ceo-articles-daily-counts-chart.csv
"""

from __future__ import annotations
import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import pandas as pd



# -------- Defaults (can be overridden by CLI flags) -------- #
DEFAULT_ROSTER = "rosters/main-roster.csv"
DEFAULT_ARTICLES_DIR = "data/processed_articles"
DEFAULT_DAILY_DIR = "data/processed_articles"
DEFAULT_OUT = "data/daily_counts/ceo-articles-daily-counts-chart.csv"


# ---------------------- Helpers ---------------------------- #
def iso_today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def load_roster(path: Path) -> pd.DataFrame:
    """
    Expected headers (case-insensitive): CEO Alias (or alias), CEO, Company
    Returns: DataFrame with columns [alias, ceo, company]
    """
    if not path.exists():
        raise FileNotFoundError(f"Roster file not found: {path}")

    df = pd.read_csv(path, encoding="utf-8-sig")
    cols = {c.strip().lower(): c for c in df.columns}

    def col(*names: str) -> str:
        """Find first matching column (case-insensitive)"""
        for name in names:
            for k, v in cols.items():
                if k == name.lower():
                    return v
        raise KeyError(f"Expected one of {names} columns in {path}")

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
    return out


def load_articles(articles_dir: Path, date_str: str) -> pd.DataFrame:
    """
    Reads data/processed_articles/YYYY-MM-DD-ceo-articles-modal.csv if present.
    Expected columns (case-insensitive): ceo, company, title, url, source, sentiment
    Returns empty DataFrame (with expected columns) if file missing/empty.
    """
    # Updated to use new filename pattern
    f = articles_dir / f"{date_str}-ceo-articles-modal.csv"
    cols = ["ceo", "company", "title", "url", "source", "sentiment"]
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
    return df[cols]


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

    # Optional: theme column (unknown unless you compute one elsewhere)
    base["theme"] = ""

    # Reorder + add date
    out = base[["ceo", "company", "positive", "neutral", "negative", "total", "neg_pct", "theme", "alias"]].copy()
    out.insert(0, "date", date_str)
    return out


def write_daily_file(daily_dir: Path, date_str: str, daily_rows: pd.DataFrame) -> Path:
    """
    Writes data/processed_articles/YYYY-MM-DD-ceo-articles-table.csv
    """
    daily_dir.mkdir(parents=True, exist_ok=True)
    # Updated to use new naming convention
    path = daily_dir / f"{date_str}-ceo-articles-table.csv"
    daily_rows.to_csv(path, index=False)
    return path


def upsert_master_index(out_path: Path, date_str: str, daily_rows: pd.DataFrame) -> Path:
    """
    Replaces rows for date_str in data/daily_counts/ceo-articles-daily-counts-chart.csv with daily_rows;
    creates the file if missing.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        master = pd.read_csv(out_path)
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

    # Sort for readability
    master["date"] = master["date"].astype(str)
    master = master.sort_values(["date", "ceo"]).reset_index(drop=True)
    master.to_csv(out_path, index=False)
    return out_path


# ----------------------- CLI / Main ------------------------ #
def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build daily CEO sentiment counts (legacy outputs).")
    p.add_argument("--date", default=iso_today_utc(), help="Target date (YYYY-MM-DD). Default = today UTC.")
    p.add_argument("--roster", default=DEFAULT_ROSTER, help="Path to main-roster.csv")
    p.add_argument("--articles-dir", default=DEFAULT_ARTICLES_DIR, help="Folder with daily articles CSVs")
    p.add_argument("--daily-dir", default=DEFAULT_DAILY_DIR, help="Folder to write per-day CSVs")
    p.add_argument("--out", default=DEFAULT_OUT, help="Path to write/append master index (daily_counts.csv)")
    return p.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv)

    # Validate date
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        raise SystemExit(f"Invalid --date '{args.date}'. Expected YYYY-MM-DD.")

    roster = load_roster(Path(args.roster))
    articles = load_articles(Path(args.articles_dir), args.date)
    daily_rows = aggregate_counts(roster, articles, args.date)

    daily_path = write_daily_file(Path(args.daily_dir), args.date, daily_rows)
    master_path = upsert_master_index(Path(args.out), args.date, daily_rows)

    print(f"✔ Wrote per-day file:  {daily_path}")
    print(f"✔ Updated master index: {master_path} (rows: {len(pd.read_csv(master_path)):,})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
