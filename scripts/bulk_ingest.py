#!/usr/bin/env python3
import argparse
import os
from datetime import date, datetime, timedelta

from gcs_utils import exists_gcs, open_gcs_text, parse_gcs_path
from ingest_csvs import get_conn
from ingest_metrics import ingest_stock_csv, ingest_trends_csv, ingest_boards_csv
from ingest_v2 import ingest_article_mentions, ingest_serp_results, upsert_companies_ceos


def parse_args():
    p = argparse.ArgumentParser(description="Bulk ingest daily CSVs into Postgres/Supabase.")
    p.add_argument("--data-dir", required=True, help="Local path or gs:// bucket prefix")
    p.add_argument("--roster-path", help="Path to main-roster.csv (local or gs://)")
    p.add_argument("--date", help="YYYY-MM-DD (single date)")
    p.add_argument("--from", dest="from_date", help="YYYY-MM-DD start (inclusive)")
    p.add_argument("--to", dest="to_date", help="YYYY-MM-DD end (inclusive)")
    p.add_argument("--days-back", type=int, help="Number of days back from today (inclusive)")
    p.add_argument("--map-sentiment-to-risk", action="store_true",
                   help="Set risk_raw from sentiment (negative->risk, else no_risk)")
    p.add_argument("--include-serps", action="store_true",
                   help="Include processed_serps CSVs (default: skip; use parquet pipeline instead)")
    p.add_argument("--batch-size", type=int, default=1000)
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def iter_dates(from_str, to_str):
    d0 = date.fromisoformat(from_str)
    d1 = date.fromisoformat(to_str)
    if d1 < d0:
        raise SystemExit("--to is before --from")
    d = d0
    while d <= d1:
        yield d.isoformat()
        d += timedelta(days=1)


def resolve_dates(args):
    if args.date:
        return [args.date]
    if args.from_date and args.to_date:
        return list(iter_dates(args.from_date, args.to_date))
    if args.days_back:
        today = datetime.utcnow().date()
        start = today - timedelta(days=args.days_back - 1)
        return list(iter_dates(start.isoformat(), today.isoformat()))
    return [datetime.utcnow().date().isoformat()]


def build_file_map(data_dir, dstr, include_serps: bool):
    items = [
        (
            os.path.join(data_dir, "processed_articles", f"{dstr}-brand-articles-modal.csv"),
            "brand",
            "news",
        ),
        (
            os.path.join(data_dir, "processed_articles", f"{dstr}-ceo-articles-modal.csv"),
            "ceo",
            "news",
        ),
    ]
    if include_serps:
        items.extend([
            (
                os.path.join(data_dir, "processed_serps", f"{dstr}-brand-serps-modal.csv"),
                "brand",
                "serp",
            ),
            (
                os.path.join(data_dir, "processed_serps", f"{dstr}-ceo-serps-modal.csv"),
                "ceo",
                "serp",
            ),
        ])
    return items


def maybe_gcs_exists(path):
    if path.startswith("gs://"):
        return exists_gcs(path)
    return os.path.exists(path)


def open_text(path):
    if path.startswith("gs://"):
        return open_gcs_text(path)
    return open(path, "r", encoding="utf-8", newline="")

def default_roster_path(data_dir):
    if data_dir.startswith("gs://"):
        bucket, _ = parse_gcs_path(data_dir)
        if not bucket:
            return ""
        return f"gs://{bucket}/rosters/main-roster.csv"
    base = os.path.dirname(data_dir.rstrip("/"))
    return os.path.join(base, "rosters", "main-roster.csv")


def ingest_metrics(conn, data_dir, dstr):
    trends_path = os.path.join(data_dir, "trends_data", f"{dstr}-trends-data.csv")
    stocks_path = os.path.join(data_dir, "stock_prices", f"{dstr}-stock-data.csv")

    total = 0
    if maybe_gcs_exists(trends_path):
        with open_text(trends_path) as f:
            total += ingest_trends_csv(conn, f)
        print(f"{trends_path}: ingested trends daily rows")
    if maybe_gcs_exists(stocks_path):
        with open_text(stocks_path) as f:
            total += ingest_stock_csv(conn, f)
        print(f"{stocks_path}: ingested stock daily rows")
    return total


def ingest_roster(conn, roster_path):
    if not roster_path:
        return 0
    if not maybe_gcs_exists(roster_path):
        return 0
    with open_text(roster_path) as f:
        upsert_companies_ceos(conn, f)
        print(f"{roster_path}: upserted companies + CEOs")
        return 1


def ingest_boards(conn, boards_path):
    if not boards_path:
        return 0
    if not maybe_gcs_exists(boards_path):
        return 0
    with open_text(boards_path) as f:
        count = ingest_boards_csv(conn, f)
        print(f"{boards_path}: ingested {count} boards rows")
        return count


def main():
    args = parse_args()
    dates = resolve_dates(args)

    conn = get_conn()
    try:
        total = 0
        roster_path = args.roster_path or default_roster_path(args.data_dir)
        total += ingest_roster(conn, roster_path)
        boards_path = None
        if args.data_dir.startswith("gs://"):
            bucket, _ = parse_gcs_path(args.data_dir)
            boards_path = f"gs://{bucket}/rosters/boards-roster.csv"
        else:
            base = os.path.dirname(args.data_dir.rstrip("/"))
            boards_path = os.path.join(base, "rosters", "boards-roster.csv")
        total += ingest_boards(conn, boards_path)
        for dstr in dates:
            for path, entity_type, source_type in build_file_map(args.data_dir, dstr, args.include_serps):
                if not maybe_gcs_exists(path):
                    continue
                with open_text(path) as f:
                    if source_type == 'news':
                        ingest_article_mentions(conn, f, 'company' if entity_type == 'brand' else 'ceo', dstr)
                    else:
                        ingest_serp_results(conn, f, 'company' if entity_type == 'brand' else 'ceo', dstr)

            total += ingest_metrics(conn, args.data_dir, dstr)
        print(f"Total upserted rows: {total}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
