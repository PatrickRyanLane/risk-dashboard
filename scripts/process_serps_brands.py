#!/usr/bin/env python3
"""
Process daily BRAND SERP data.
Reads raw SerpAPI parquet and writes processed CSVs + DB rows.

Outputs:
  1) Row-level processed SERPs:       data/processed_serps/{date}-brand-serps-modal.csv
  2) Per-company daily aggregate:     data/processed_serps/{date}-brand-serps-table.csv
  3) Rolling daily index:             data/daily_counts/brand-serps-daily-counts-chart.csv
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime
from typing import Dict, Tuple, Set
from urllib.parse import urlparse
from pathlib import Path

import pandas as pd
import duckdb
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from storage_utils import CloudStorageManager
from llm_utils import is_uncertain
from db_writer import upsert_serp_results
from url_utils import resolve_url
from risk_rules import (
    classify_control,
    is_financial_routine,
    parse_company_domains,
    should_neutralize_finance_routine,
    should_neutralize_brand_title,
    title_mentions_legal_trouble,
)


LEGAL_SUFFIXES = {
    "inc", "incorporated",
    "corp", "corporation",
    "co", "company",
    "llc", "plc", "ltd", "limited",
    "group", "holdings", "holding", "hldgs",
    "exchange", "insurance",
    "the",
}


def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = str(name).strip()
    name = name.replace("&", " and ")
    name = name.replace(".com", " ").replace(".net", " ").replace(".org", " ")
    cleaned = "".join(ch for ch in name.lower() if ch.isalnum() or ch.isspace())
    cleaned = " ".join(cleaned.split())
    return cleaned.strip()


def simplify_company(name: str) -> str:
    tokens = normalize_name(name).split()
    tokens = [t for t in tokens if t not in LEGAL_SUFFIXES]
    return " ".join(tokens).strip()

# Config / constants
PARQUET_URL_TEMPLATE = (
    "https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/{date}-brand-raw-queries.parquet"
)

MAIN_ROSTER_PATH = "rosters/main-roster.csv"
OUT_ROWS_DIR = "data/processed_serps"
OUT_DAILY_DIR = "data/processed_serps"
OUT_ROLLUP = "data/daily_counts/brand-serps-daily-counts-chart.csv"

FORCE_POSITIVE_IF_CONTROLLED = True

# Argument parsing
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Process daily brand SERPs.")
    ap.add_argument("--date", help="YYYY-MM-DD (defaults to today)", default=None)
    ap.add_argument("--bucket", type=str, default="risk-dashboard",
                   help="GCS bucket name (default: risk-dashboard)")
    ap.add_argument("--local", action="store_true",
                   help="Use local file storage instead of GCS")
    ap.add_argument("--roster", default=MAIN_ROSTER_PATH, help="Path to roster file")
    ap.add_argument(
        "--source-path",
        default="",
        help="Optional parquet source path/URL override (defaults to daily S3 raw parquet).",
    )
    ap.add_argument(
        "--max-source-wait-minutes",
        type=int,
        default=20,
        help="How long to wait/poll for a complete raw parquet before failing (default: 20).",
    )
    ap.add_argument(
        "--source-check-interval-seconds",
        type=int,
        default=90,
        help="Polling interval while waiting for raw parquet readiness (default: 90).",
    )
    ap.add_argument(
        "--min-raw-queries",
        type=int,
        default=100,
        help="Absolute minimum raw queries required before processing (default: 100).",
    )
    ap.add_argument(
        "--min-raw-query-ratio",
        type=float,
        default=0.50,
        help="Minimum raw queries as ratio of roster companies (default: 0.50).",
    )
    ap.add_argument(
        "--min-mapped-queries",
        type=int,
        default=100,
        help="Absolute minimum mapped queries required before writing/upsert (default: 100).",
    )
    ap.add_argument(
        "--min-mapped-query-ratio",
        type=float,
        default=0.40,
        help="Minimum mapped queries as ratio of roster companies (default: 0.40).",
    )
    return ap.parse_args()

def get_target_date(arg_date: str | None) -> str:
    if arg_date:
        try:
            datetime.strptime(arg_date, "%Y-%m-%d")
            return arg_date
        except ValueError:
            pass
    return datetime.utcnow().strftime("%Y-%m-%d")

def fetch_parquet_with_metrics(path_or_url: str) -> Tuple[pd.DataFrame | None, Dict[str, int]]:
    metrics: Dict[str, int] = {
        "parquet_rows": 0,
        "raw_queries": 0,
        "raw_queries_with_payload": 0,
        "valid_json_rows": 0,
        "raw_queries_with_organic": 0,
        "organic_rows": 0,
    }
    try:
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        df = con.execute(f"select query, raw_json from read_parquet('{path_or_url}')").fetch_df()
    except Exception as e:
        print(f"[WARN] Could not fetch {path_or_url} – {e}")
        return None, metrics

    metrics["parquet_rows"] = int(len(df))

    all_queries: Set[str] = set()
    payload_queries: Set[str] = set()
    organic_queries: Set[str] = set()

    rows = []
    for _, row in df.iterrows():
        company = str(row.get("query") or "").strip()
        if not company:
            continue
        all_queries.add(company)
        raw = row.get("raw_json")
        if not raw:
            continue
        payload_queries.add(company)
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        metrics["valid_json_rows"] += 1
        data = obj.get("data", obj)
        organic = data.get("organic_results") or []
        if organic:
            organic_queries.add(company)
        for item in organic:
            if not isinstance(item, dict):
                continue
            rows.append({
                "company": company,
                "position": item.get("position") or item.get("rank") or 0,
                "title": item.get("title") or "",
                "link": item.get("link") or "",
                "snippet": item.get("snippet") or "",
            })
    metrics["raw_queries"] = len(all_queries)
    metrics["raw_queries_with_payload"] = len(payload_queries)
    metrics["raw_queries_with_organic"] = len(organic_queries)
    metrics["organic_rows"] = len(rows)
    if not rows:
        return pd.DataFrame(), metrics
    return pd.DataFrame(rows), metrics

# Domain normalization
def _hostname(url: str) -> str:
    try:
        host = (urlparse(resolve_url(url or "")).hostname or "").lower()
        return host.replace("www.", "")
    except Exception:
        return ""

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
            
            domains = parse_company_domains(str(val))
            if domains:
                company_domains[company].update(domains)
        
        total_domains = sum(len(domains) for domains in company_domains.values())
        print(f"[OK] Loaded {len(company_domains)} companies with {total_domains} total controlled domains")
                        
    except Exception as e:
        print(f"[WARN] Failed reading roster: {e}")

    return company_domains


def load_roster_companies(storage, path: str = MAIN_ROSTER_PATH) -> list[str]:
    companies: list[str] = []
    try:
        if storage:
            if not storage.file_exists(path):
                print(f"[WARN] Roster not found in Cloud Storage at {path}")
                return companies
            df = storage.read_csv(path)
        else:
            roster_file = Path(path)
            if not roster_file.exists():
                print(f"[WARN] Roster not found at {roster_file}")
                return companies
            df = pd.read_csv(roster_file, encoding="utf-8-sig")

        cols = {c.strip().lower(): c for c in df.columns}
        company_col = cols.get("company")
        if not company_col:
            print("[WARN] Missing company column in roster")
            return companies

        vals = (
            df[company_col]
            .astype(str)
            .str.strip()
        )
        companies = sorted({c for c in vals if c and c.lower() != "nan"})
    except Exception as e:
        print(f"[WARN] Failed reading roster companies: {e}")
    return companies


def load_company_industries(storage, path: str = MAIN_ROSTER_PATH) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        if storage:
            if not storage.file_exists(path):
                print(f"[WARN] Roster not found in Cloud Storage at {path}")
                return out
            df = storage.read_csv(path)
        else:
            roster_file = Path(path)
            if not roster_file.exists():
                print(f"[WARN] Roster not found at {roster_file}")
                return out
            df = pd.read_csv(roster_file, encoding="utf-8-sig")

        cols = {c.strip().lower(): c for c in df.columns}
        company_col = cols.get("company")
        industry_col = cols.get("industry") or cols.get("sector")
        if not company_col or not industry_col:
            return out

        for _, row in df.iterrows():
            company = str(row.get(company_col) or "").strip()
            if not company or company.lower() == "nan":
                continue
            industry = str(row.get(industry_col) or "").strip()
            if not industry or industry.lower() == "nan":
                continue
            out[company] = industry
    except Exception as e:
        print(f"[WARN] Failed reading company industries: {e}")
    return out


def load_company_aliases(storage, path: str = MAIN_ROSTER_PATH) -> Dict[str, str]:
    alias_map: Dict[str, str] = {}
    try:
        if storage:
            if not storage.file_exists(path):
                print(f"[WARN] Roster not found in Cloud Storage at {path}")
                return alias_map
            df = storage.read_csv(path)
        else:
            roster_file = Path(path)
            if not roster_file.exists():
                print(f"[WARN] Roster not found at {roster_file}")
                return alias_map
            df = pd.read_csv(roster_file, encoding="utf-8-sig")

        cols = {c.strip().lower(): c for c in df.columns}
        company_col = cols.get("company")
        alias_col = cols.get("company alias") or cols.get("company_alias") or cols.get("alias")
        if not company_col or not alias_col:
            return alias_map

        for _, row in df.iterrows():
            company = str(row[company_col]).strip()
            if not company or company.lower() == "nan":
                continue
            raw_alias = str(row[alias_col] or "").strip()
            if not raw_alias or raw_alias.lower() == "nan":
                continue
            for alias in re.split(r"[|;]+", raw_alias):
                alias = alias.strip()
                if not alias:
                    continue
                alias_map[normalize_name(alias)] = company
    except Exception as e:
        print(f"[WARN] Failed reading company aliases: {e}")
    return alias_map

def vader_label_on_title(analyzer: SentimentIntensityAnalyzer, title: str) -> Tuple[str, float]:
    """
    Apply VADER with custom thresholds.
    Unified thresholds: positive >= 0.15, negative <= -0.10
    """
    s = analyzer.polarity_scores(title or "")
    c = s.get("compound", 0.0)
    
    if c >= 0.15:
        return "positive", c
    elif c <= -0.10:
        return "negative", c
    else:
        return "neutral", c

def process_for_date(
    storage,
    target_date: str,
    roster_path: str,
    source_path: str,
    max_source_wait_minutes: int,
    source_check_interval_seconds: int,
    min_raw_queries: int,
    min_raw_query_ratio: float,
    min_mapped_queries: int,
    min_mapped_query_ratio: float,
) -> None:
    print(f"[INFO] Processing brand SERPs for {target_date} …")

    company_domains = load_roster_domains(storage, roster_path)
    roster_companies = load_roster_companies(storage, roster_path)
    company_industries = load_company_industries(storage, roster_path)
    company_aliases = load_company_aliases(storage, roster_path)
    company_lookup = {normalize_name(name): name for name in roster_companies}
    company_lookup.update(company_aliases)
    expected_queries = len(roster_companies)
    print(
        f"[METRIC] roster_companies={expected_queries} "
        f"roster_aliases={len(company_aliases)} "
        f"roster_domain_companies={len(company_domains)}"
    )
    simplified_lookup = {}
    simplified_conflicts = set()
    for name in roster_companies:
        key = simplify_company(name)
        if not key:
            continue
        if key in simplified_lookup and simplified_lookup[key] != name:
            simplified_conflicts.add(key)
        else:
            simplified_lookup[key] = name
    for key in simplified_conflicts:
        simplified_lookup.pop(key, None)

    source = source_path or PARQUET_URL_TEMPLATE.format(date=target_date)
    min_raw_queries = max(0, int(min_raw_queries))
    min_raw_query_ratio = max(0.0, float(min_raw_query_ratio))
    min_mapped_queries = max(0, int(min_mapped_queries))
    min_mapped_query_ratio = max(0.0, float(min_mapped_query_ratio))
    required_raw_queries = max(min_raw_queries, int(expected_queries * min_raw_query_ratio))
    required_mapped_queries = max(min_mapped_queries, int(expected_queries * min_mapped_query_ratio))
    print(
        f"[GUARD] expected_queries={expected_queries} "
        f"required_raw_queries={required_raw_queries} "
        f"required_mapped_queries={required_mapped_queries}"
    )

    wait_deadline = time.time() + max(0, max_source_wait_minutes) * 60
    attempt = 0
    raw = None
    raw_metrics: Dict[str, int] = {}
    while True:
        attempt += 1
        raw, raw_metrics = fetch_parquet_with_metrics(source)
        raw_query_count = int(raw_metrics.get("raw_queries_with_payload", 0))
        print(
            f"[METRIC] source_attempt={attempt} "
            f"raw_parquet_rows={raw_metrics.get('parquet_rows', 0)} "
            f"raw_queries={raw_metrics.get('raw_queries', 0)} "
            f"raw_queries_with_payload={raw_query_count} "
            f"raw_queries_with_organic={raw_metrics.get('raw_queries_with_organic', 0)} "
            f"raw_organic_rows={raw_metrics.get('organic_rows', 0)}"
        )

        if raw is not None and not raw.empty and raw_query_count >= required_raw_queries:
            break

        if max_source_wait_minutes <= 0 or time.time() >= wait_deadline:
            raise RuntimeError(
                "Raw SERP guardrail failed: "
                f"source={source} raw_queries_with_payload={raw_query_count} "
                f"required_raw_queries={required_raw_queries}"
            )

        print(
            "[WARN] Raw source below guardrail; "
            f"retrying in {max(1, source_check_interval_seconds)}s ..."
        )
        time.sleep(max(1, source_check_interval_seconds))

    expected = ["company", "position", "title", "link", "snippet"]
    for col in expected:
        if col not in raw.columns:
            raw[col] = ""

    analyzer = SentimentIntensityAnalyzer()
    processed_rows = []
    mapped_queries: Set[str] = set()
    unmapped = 0
    unmapped_names: Dict[str, int] = {}
    # DB connection is handled by db_writer during upsert.
    for _, row in raw.iterrows():
        raw_company = str(row.get("company", "") or "").strip()
        if not raw_company:
            continue
        norm_key = normalize_name(raw_company)
        company = company_lookup.get(norm_key, "")
        if not company:
            simple_key = simplify_company(raw_company)
            company = simplified_lookup.get(simple_key, "")
        if not company:
            unmapped += 1
            unmapped_names[raw_company] = unmapped_names.get(raw_company, 0) + 1
            continue
        mapped_queries.add(raw_company)

        title = str(row.get("title", "") or "").strip()
        url = str(row.get("link", "") or "").strip()
        snippet = str(row.get("snippet", "") or "").strip()
        source = str(row.get("source", "") or "").strip()

        pos_val = row.get("position", 0)
        try:
            position = int(float(pos_val) if pos_val not in (None, "") else 0)
        except Exception:
            position = 0

        controlled = classify_control(
            company,
            url,
            company_domains,
            publisher=source,
            snippet=snippet,
        )
        finance_routine = is_financial_routine(title, snippet=snippet, url=url, source=source)
        industry = company_industries.get(company, "")

        # --- Sentiment rules (deterministic order) ---
        host = _hostname(url)
        compound = None
        forced_reason = ""

        # 1) Force negative for reddit.com
        if host == "reddit.com" or (host and host.endswith(".reddit.com")):
            label = "negative"
            forced_reason = "reddit"
        # 2) Force negative for Legal/Trouble terms
        elif title_mentions_legal_trouble(title, snippet, url=url, source=source, industry=industry):
            label = "negative"
            forced_reason = "legal"
        # 3) Neutralize routine financial coverage
        elif should_neutralize_finance_routine(
            "negative",
            title,
            snippet=snippet,
            url=url,
            source=source,
            finance_routine=finance_routine,
        ):
            label = "neutral"
            forced_reason = "finance"
        # 4) Neutralize certain terms
        elif should_neutralize_brand_title(title):
            label = "neutral"
            forced_reason = "neutral_terms"
        else:
            # 4) VADER analysis on the raw title
            label, compound = vader_label_on_title(analyzer, title)

        # 5) Force positive if controlled (override all prior labels)
        if FORCE_POSITIVE_IF_CONTROLLED and controlled:
            label = "positive"
            forced_reason = "controlled"

        is_forced = bool(forced_reason)
        uncertain, uncertain_reason = is_uncertain(
            label,
            finance_routine,
            is_forced,
            compound,
            title,
            title
        )
        llm_label = ""
        llm_severity = ""
        llm_reason = ""

        processed_rows.append({
            "date": target_date,
            "company": company,
            "title": title,
            "url": url,
            "position": position,
            "snippet": snippet,
            "sentiment": label,
            "controlled": controlled,
            "finance_routine": finance_routine,
            "uncertain": uncertain,
            "uncertain_reason": uncertain_reason,
            "llm_label": llm_label,
            "llm_severity": llm_severity,
            "llm_reason": llm_reason,
        })

    mapped_query_count = len(mapped_queries)
    unmapped_query_count = len(unmapped_names)
    print(
        f"[METRIC] mapped_queries={mapped_query_count} "
        f"mapped_rows={len(processed_rows)} "
        f"unmapped_queries={unmapped_query_count} "
        f"unmapped_rows={unmapped}"
    )
    if mapped_query_count < required_mapped_queries:
        raise RuntimeError(
            "Mapped SERP guardrail failed: "
            f"mapped_queries={mapped_query_count} required_mapped_queries={required_mapped_queries} "
            f"source={source}"
        )

    if not processed_rows:
        print(f"[WARN] No processed rows for {target_date}.")
        return
    if unmapped:
        print(f"[WARN] {unmapped} SERP rows could not be mapped to roster companies.")
        top = sorted(unmapped_names.items(), key=lambda kv: kv[1], reverse=True)[:10]
        if top:
            print("[WARN] Top unmapped queries:")
            for name, count in top:
                print(f"  - {name}: {count}")
        out_rows = [{"query": k, "count": v} for k, v in sorted(unmapped_names.items(), key=lambda kv: kv[1], reverse=True)]
        if out_rows:
            out_df = pd.DataFrame(out_rows)
            out_path = f"data/processed_serps/mismatches/{target_date}-brand-serps-unmapped.csv"
            try:
                if storage:
                    storage.write_csv(out_df, out_path, index=False)
                    print(f"[OK] Wrote unmapped queries: {out_path}")
                else:
                    out_file = Path(out_path)
                    out_file.parent.mkdir(parents=True, exist_ok=True)
                    out_df.to_csv(out_file, index=False)
                    print(f"[OK] Wrote unmapped queries: {out_file}")
            except Exception as e:
                print(f"[WARN] Failed writing unmapped queries: {e}")

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

    try:
        db_count = upsert_serp_results(rows_df, "company", target_date)
        print(f"[OK] DB upserted {db_count} brand SERP rows")
    except Exception as e:
        print(f"[WARN] DB upsert failed: {e}")

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
    
    process_for_date(
        storage,
        date_str,
        args.roster,
        args.source_path,
        args.max_source_wait_minutes,
        args.source_check_interval_seconds,
        args.min_raw_queries,
        args.min_raw_query_ratio,
        args.min_mapped_queries,
        args.min_mapped_query_ratio,
    )

if __name__ == "__main__":
    main()
