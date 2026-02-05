#!/usr/bin/env python3
"""
Process daily CEO SERP data.
Reads raw SerpAPI parquet and writes processed CSVs + DB rows.

NOTE: Raw parquet uses query text as the alias (e.g., "Tim Cook Apple").
      We resolve aliases to actual CEO/company using the roster.

Outputs:
  1) Row-level processed SERPs:       data/processed_serps/{date}-ceo-serps-modal.csv
  2) Per-CEO daily aggregate:         data/processed_serps/{date}-ceo-serps-table.csv
  3) Rolling daily index:             data/daily_counts/ceo-serps-daily-counts-chart.csv
"""

from __future__ import annotations

import argparse
import json
import re
import os, sys
from datetime import datetime
from typing import Dict, Set, Tuple
from urllib.parse import urlparse
from pathlib import Path

import pandas as pd
import duckdb
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from storage_utils import CloudStorageManager
from llm_utils import is_uncertain
from db_writer import upsert_serp_results
from risk_rules import classify_control, is_financial_routine, parse_company_domains

# Config
PARQUET_URL_TEMPLATE = (
    "https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/{date}-ceo-raw-queries.parquet"
)

MAIN_ROSTER_PATH = "rosters/main-roster.csv"
OUT_ROWS_DIR = "data/processed_serps"
OUT_DAILY_DIR = "data/processed_serps"
OUT_ROLLUP = "data/daily_counts/ceo-serps-daily-counts-chart.csv"

FORCE_POSITIVE_IF_CONTROLLED = True

# ============================================================================
# CEO-SPECIFIC WORD FILTERING RULES
# ============================================================================
# REMOVED (?i) prefixes because re.IGNORECASE is used in re.compile
NEUTRALIZE_TITLE_TERMS = [
    r"\bflees\b",           # Often used figuratively or in names
    r"\bsavage\b",          # Common surname (e.g., Dan Savage)
    r"\brob\b",             # Common first name (e.g., Rob Walton)
    r"\bnicholas\s+lower\b", # Specific CEO name combination
    r"\bmad\s+money\b",     # Jim Cramer's show
    r"\bno\s+organic\b",    # About organic food availability
    r"\brob\b",        # Potentially a person's name
    r"\blower\b",      # CEO with last name Lower
    r"\benergy\b",     # Lot of brands with energy in their name   
    r"\brebel\b",      # Potential product name
]
NEUTRALIZE_TITLE_RE = re.compile("|".join(NEUTRALIZE_TITLE_TERMS), flags=re.IGNORECASE)

ALWAYS_NEGATIVE_TERMS = [
    # Compensation scrutiny (common CEO negative coverage)
    r"\bpaid\b", r"\bcompensation\b", r"\bpay\b", r"\bnet worth\b",
    # Corporate governance issues
    r"\bmandate\b",
    # Leadership changes (usually negative for the departing CEO)
    r"\bexit(s)?\b", r"\bstep\s+down\b", r"\bsteps\s+down\b", r"\bremoved\b",
    # Skepticism/scrutiny language
    r"\bstill\b",  # "CEO still hasn't..." implies criticism
    r"\bturnaround\b",  # Company in trouble
    # Personal accusations
    r"\bface\b", r"\bcontroversy\b", r"\baccused\b", r"\bcommitted\b", r"\bapologizes\b", r"\bapology\b",
    r"\baware\b",  # "CEO was aware of..." implies cover-up
    # Financial/personal troubles
    r"\bloss\b", r"\bdivorce\b", r"\bbankruptcy\b",
    # Data security
    r"\bdata leaks?\b",
    # Labor relations
    r"\bunion\s+buster\b",
    # Termination (in any direction)
    r"\bfired\b", r"\bfiring\b", r"\bfires\b",
    r"(?<!t)\bax(e|ed|es)?\b",  # "axed" but not "taxes"
    r"\bsack(ed|s)?\b", r"\boust(ed)?\b",
    # Stock performance
    r"\bplummeting\b",
]
ALWAYS_NEGATIVE_RE = re.compile("|".join(ALWAYS_NEGATIVE_TERMS), re.IGNORECASE)


# Legal suffixes to strip when matching company names
LEGAL_SUFFIXES = {"inc", "inc.", "corp", "co", "co.", "llc", "plc", "ltd", "ltd.", "ag", "sa", "nv"}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
def _should_force_negative_title(title: str) -> bool:
    """Return True if title contains CEO-specific negative terms."""
    return bool(ALWAYS_NEGATIVE_RE.search(title or ""))


def _should_neutralize_title(title: str) -> bool:
    """Return True if the title contains terms that should neutralize sentiment."""
    return bool(NEUTRALIZE_TITLE_RE.search(str(title or "")))

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


def norm(s: str) -> str:
    """Normalize string for matching: lowercase, alphanumeric + spaces only."""
    s = str(s or "").lower().strip()
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def simplify_company(s: str) -> str:
    """Remove legal suffixes from company name for matching."""
    toks = norm(s).split()
    toks = [t for t in toks if t not in LEGAL_SUFFIXES]
    return " ".join(toks)


def _hostname(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower()
        return host.replace("www.", "")
    except Exception:
        return ""


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


def fetch_parquet_from_s3(url: str) -> pd.DataFrame | None:
    try:
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        df = con.execute(f"select query, raw_json from read_parquet('{url}')").fetch_df()
    except Exception as e:
        print(f"[WARN] Could not fetch {url} – {e}")
        return None

    rows = []
    for _, row in df.iterrows():
        query = str(row.get("query") or "").strip()
        if not query:
            continue
        raw = row.get("raw_json")
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        data = obj.get("data", obj)
        organic = data.get("organic_results") or []
        for item in organic:
            if not isinstance(item, dict):
                continue
            rows.append({
                "query": query,
                "position": item.get("position") or item.get("rank") or 0,
                "title": item.get("title") or "",
                "url": item.get("link") or "",
                "snippet": item.get("snippet") or "",
            })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


# ============================================================================
# ROSTER LOADING WITH ALIAS MAPS
# ============================================================================
def load_roster_data(storage, roster_path: str = MAIN_ROSTER_PATH) -> Tuple[Dict, Dict, Dict]:
    """
    Load roster data including alias maps for resolving CEO queries.
    """
    alias_map = {}
    ceo_to_company = {}
    company_domains: Dict[str, Set[str]] = {}

    try:
        if storage:
            if not storage.file_exists(roster_path):
                print(f"[WARN] Roster not found in Cloud Storage at {roster_path}")
                return alias_map, ceo_to_company, company_domains
            df = storage.read_csv(roster_path)
        else:
            roster_file = Path(roster_path)
            if not roster_file.exists():
                print(f"[WARN] Roster not found at {roster_file}")
                return alias_map, ceo_to_company, company_domains
            df = pd.read_csv(roster_file, encoding="utf-8-sig")
        
        cols = {c.strip().lower(): c for c in df.columns}
        
        def col(*names):
            for name in names:
                for k, v in cols.items():
                    if k == name.lower():
                        return v
            return None

        ceo_col = col("ceo")
        company_col = col("company")
        alias_col = col("ceo alias", "alias")
        website_col = col("website", "websites", "domain", "url")

        if not (ceo_col and company_col):
            print("[WARN] Roster must have CEO and Company columns")
            return alias_map, ceo_to_company, company_domains

        # Build ceo_to_company mapping
        for _, row in df.iterrows():
            ceo = str(row[ceo_col]).strip()
            company = str(row[company_col]).strip()
            if ceo and company and ceo != "nan" and company != "nan":
                ceo_to_company[ceo] = company

        # Build alias_map from CEO Alias column
        if alias_col:
            for _, row in df.iterrows():
                alias = str(row[alias_col]).strip()
                ceo = str(row[ceo_col]).strip()
                company = str(row[company_col]).strip()
                if alias and ceo and company and alias != "nan":
                    alias_map[norm(alias)] = (ceo, company)

        # Also add "CEO Company" as an alias (fallback)
        for ceo, comp in ceo_to_company.items():
            alias_map.setdefault(norm(f"{ceo} {comp}"), (ceo, comp))

        print(f"[OK] Loaded {len(ceo_to_company)} CEOs, {len(alias_map)} aliases")

        # Build company_domains for control classification
        if website_col:
            print(f"[INFO] Loading controlled domains from roster...")
            for _, row in df.iterrows():
                company = str(row[company_col]).strip()
                if not company or company.lower() == "nan":
                    continue
                
                val = row[website_col]
                if pd.isna(val):
                    continue
                
                val = str(val).strip()
                if not val or val.lower() == "nan":
                    continue
                
                domains = parse_company_domains(val)
                if domains:
                    company_domains.setdefault(company, set()).update(domains)
            
            total_domains = sum(len(domains) for domains in company_domains.values())
            print(f"[OK] Loaded {len(company_domains)} companies with {total_domains} total controlled domains")

    except Exception as e:
        print(f"[WARN] Failed reading roster: {e}")

    return alias_map, ceo_to_company, company_domains


# ============================================================================
# COLUMN NORMALIZATION & ALIAS RESOLUTION
# ============================================================================
def normalize_raw_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize raw S3 columns to standard names.
    Raw S3 file has 'company' column which is actually the search alias.
    """
    cols = {c.lower(): c for c in df.columns}
    
    # The 'company' column in raw S3 = query alias (like "Tim Cook Apple")
    q_c = cols.get("company") or cols.get("query") or cols.get("search")
    t_c = cols.get("title") or cols.get("page_title") or cols.get("result")
    u_c = cols.get("url") or cols.get("link")
    p_c = cols.get("position") or cols.get("rank") or cols.get("pos")
    sn_c = cols.get("snippet") or cols.get("description")

    out = pd.DataFrame()
    out["query_alias"] = df[q_c].astype(str).str.strip() if q_c else ""
    out["title"] = df[t_c].astype(str).str.strip() if t_c else ""
    out["url"] = df[u_c].astype(str).str.strip() if u_c else ""
    out["position"] = pd.to_numeric(df[p_c], errors="coerce") if p_c else pd.Series([None]*len(df))
    out["snippet"] = df[sn_c].astype(str).str.strip() if sn_c else ""
    return out


def resolve_ceo_company(query_alias: str, alias_map: Dict, ceo_to_company: Dict) -> Tuple[str, str]:
    """
    Resolve a query alias to (ceo_name, company_name).
    """
    qn = norm(query_alias)
    
    # Exact match in alias_map
    if qn in alias_map:
        return alias_map[qn]

    # Fuzzy match: find best overlap of CEO + Company tokens
    best = None
    best_score = 0
    for ceo, comp in ceo_to_company.items():
        tokens = set(f"{norm(ceo)} {simplify_company(comp)}".split())
        if tokens.issubset(set(qn.split())):
            score = len(tokens)
            if score > best_score:
                best = (ceo, comp)
                best_score = score
    
    return best if best else ("", "")


# ============================================================================
# MAIN PROCESSING
# ============================================================================
def process_for_date(storage, target_date: str, roster_path: str) -> None:
    print(f"[INFO] Processing CEO SERPs for {target_date} …")

    # Load roster with alias maps
    alias_map, ceo_to_company, company_domains = load_roster_data(storage, roster_path)

    # Fetch raw data from S3
    url = PARQUET_URL_TEMPLATE.format(date=target_date)
    raw = fetch_parquet_from_s3(url)
    if raw is None or raw.empty:
        print(f"[WARN] No raw CEO SERP data available for {target_date}. Nothing to write.")
        return

    print(f"[INFO] Raw S3 data: {len(raw)} rows, columns: {list(raw.columns)}")

    # Normalize columns (raw 'company' = query alias)
    base = normalize_raw_columns(raw)
    print(f"[INFO] Normalized to: {list(base.columns)}")

    # Resolve aliases to actual CEO/company
    def resolve_row(query_alias):
        return pd.Series(resolve_ceo_company(query_alias, alias_map, ceo_to_company))
    
    base[["ceo", "company"]] = base["query_alias"].apply(resolve_row)

    analyzer = SentimentIntensityAnalyzer()
    processed_rows = []
    unresolved_count = 0
    # DB connection is handled by db_writer during upsert.
    
    for _, row in base.iterrows():
        ceo = str(row.get("ceo", "") or "").strip()
        company = str(row.get("company", "") or "").strip()
        
        if not ceo or not company:
            unresolved_count += 1
            continue

        title = str(row.get("title", "") or "").strip()
        url = str(row.get("url", "") or "").strip()
        snippet = str(row.get("snippet", "") or "").strip()

        pos_val = row.get("position", 0)
        try:
            position = int(float(pos_val) if pos_val not in (None, "") else 0)
        except Exception:
            position = 0

        controlled = classify_control(company, url, company_domains, entity_type="ceo", person_name=ceo)

        # --- Sentiment rules (deterministic order) ---
        host = _hostname(url)
        compound = None
        forced_reason = ""

        # 1) Force negative for reddit.com
        if host == "reddit.com" or (host and host.endswith(".reddit.com")):
            label = "negative"
            forced_reason = "reddit"
        # 2) Neutralize routine financial coverage
        elif is_financial_routine(title, snippet=snippet, url=url):
            label = "neutral"
            forced_reason = "finance"
        # 3) Force negative for CEO-specific terms
        elif _should_force_negative_title(title):
            label = "negative"
            forced_reason = "ceo_terms"
        # 4) Neutralize certain terms
        elif _should_neutralize_title(title):
            label = "neutral"
            forced_reason = "neutral_terms"
        else:
            # 4) VADER analysis on the raw title
            # (Neutral terms are already handled by step 3)
            label, compound = vader_label_on_title(analyzer, title)

            # 5) Force positive if controlled
            if FORCE_POSITIVE_IF_CONTROLLED and controlled:
                label = "positive"

        finance_routine = is_financial_routine(title, snippet=snippet, url=url)
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
            "ceo": ceo,
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

    if unresolved_count > 0:
        print(f"[WARN] {unresolved_count} rows could not be resolved to CEO/company")

    if not processed_rows:
        print(f"[WARN] No processed rows for {target_date}.")
        return

    print(f"[OK] Processed {len(processed_rows)} rows")

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

    try:
        db_count = upsert_serp_results(rows_df, "ceo", target_date)
        print(f"[OK] DB upserted {db_count} CEO SERP rows")
    except Exception as e:
        print(f"[WARN] DB upsert failed: {e}")

    # Aggregate by CEO (use majority company if multiple)
    def majority_company(series):
        s = pd.Series(series).replace("", pd.NA).dropna()
        if s.empty:
            return ""
        return s.mode().iloc[0]

    agg = (
        rows_df.groupby("ceo", dropna=False)
        .agg(
            total=("ceo", "size"),
            controlled=("controlled", "sum"),
            negative_serp=("sentiment", lambda s: (s == "negative").sum()),
            neutral_serp=("sentiment", lambda s: (s == "neutral").sum()),
            positive_serp=("sentiment", lambda s: (s == "positive").sum()),
            company=("company", majority_company),
        )
        .reset_index()
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
