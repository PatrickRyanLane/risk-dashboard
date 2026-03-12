#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timedelta

from gcs_utils import exists_gcs, open_gcs_text, parse_gcs_path
from ingest_csvs import get_conn
from ingest_v2 import ingest_article_mentions, upsert_companies_ceos


def parse_args():
    p = argparse.ArgumentParser(
        description="Backfill article mentions from dated processed_articles CSVs instead of live RSS."
    )
    p.add_argument("--data-dir", default="gs://risk-dashboard/data", help="Local path or gs:// bucket prefix")
    p.add_argument("--roster-path", help="Path to main-roster.csv (local or gs://)")
    p.add_argument("--date", help="YYYY-MM-DD (single date)")
    p.add_argument("--from", dest="from_date", help="YYYY-MM-DD start (inclusive)")
    p.add_argument("--to", dest="to_date", help="YYYY-MM-DD end (inclusive)")
    p.add_argument(
        "--entity-type",
        choices=["brand", "ceo", "both"],
        default="both",
        help="Which article files to ingest",
    )
    p.add_argument(
        "--allow-missing",
        action="store_true",
        help="Skip missing files instead of failing the run before ingest starts.",
    )
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
    raise SystemExit("Provide either --date or both --from and --to")


def maybe_exists(path):
    if path.startswith("gs://"):
        return exists_gcs(path)
    return os.path.exists(path)


def open_text(path):
    if path.startswith("gs://"):
        return open_gcs_text(path)
    return open(path, "r", encoding="utf-8", newline="")


def normalize_data_dir(data_dir: str) -> str:
    path = (data_dir or "").strip().rstrip("/")
    if not path:
        return path
    suffix = "/processed_articles"
    if path.endswith(suffix):
        return path[: -len(suffix)]
    return path


def default_roster_path(data_dir):
    data_dir = normalize_data_dir(data_dir)
    if data_dir.startswith("gs://"):
        bucket, _ = parse_gcs_path(data_dir)
        if not bucket:
            return ""
        return f"gs://{bucket}/rosters/main-roster.csv"
    base = os.path.dirname(data_dir.rstrip("/"))
    return os.path.join(base, "rosters", "main-roster.csv")


def build_expected_paths(data_dir, dstr, entity_type):
    data_dir = normalize_data_dir(data_dir)
    items = []
    if entity_type in {"brand", "both"}:
        items.append((
            os.path.join(data_dir, "processed_articles", f"{dstr}-brand-articles-modal.csv"),
            dstr,
            "brand",
            "company",
        ))
    if entity_type in {"ceo", "both"}:
        items.append((
            os.path.join(data_dir, "processed_articles", f"{dstr}-ceo-articles-modal.csv"),
            dstr,
            "ceo",
            "ceo",
        ))
    return items


def main():
    args = parse_args()
    args.data_dir = normalize_data_dir(args.data_dir)
    dates = resolve_dates(args)
    roster_path = args.roster_path or default_roster_path(args.data_dir)

    expected = []
    for dstr in dates:
        expected.extend(build_expected_paths(args.data_dir, dstr, args.entity_type))

    missing = [path for path, _, _, _ in expected if not maybe_exists(path)]
    if missing and not args.allow_missing:
        print("❌ Missing expected historical article CSVs:")
        for path in missing:
            print(f"  - {path}")
        raise SystemExit("Refusing partial article backfill. Re-run with --allow-missing to skip gaps.")

    conn = get_conn()
    try:
        if roster_path and maybe_exists(roster_path):
            with open_text(roster_path) as f:
                upsert_companies_ceos(conn, f)
                print(f"[OK] Upserted roster entities from {roster_path}")
        elif roster_path:
            print(f"[WARN] Roster not found: {roster_path}")

        total_rows = 0
        for dstr in dates:
            daily_rows = 0
            processed_files = 0
            skipped_files = 0
            day_items = build_expected_paths(args.data_dir, dstr, args.entity_type)

            for path, _, label, ingest_entity in day_items:
                if not maybe_exists(path):
                    skipped_files += 1
                    print(f"[WARN] Missing {label} article file, skipping: {path}")
                    continue
                with open_text(path) as f:
                    count = ingest_article_mentions(conn, f, ingest_entity, dstr)
                daily_rows += count
                total_rows += count
                processed_files += 1
                print(f"[OK] {label} {dstr}: DB upserted {count} article rows from {path}")

            print(
                f"[DONE] {dstr}: processed {processed_files} file(s), "
                f"skipped {skipped_files}, upserted {daily_rows} article rows "
                f"(running total: {total_rows})"
            )

        print(f"✅ Historical article CSV backfill complete: {total_rows} rows")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
