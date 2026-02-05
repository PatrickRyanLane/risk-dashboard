#!/usr/bin/env python3
import argparse
import json
import os

import psycopg2
from psycopg2.extras import execute_values

from storage_utils import CloudStorageManager


def get_conn():
    dsn = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL not set")
    return psycopg2.connect(dsn)


def ensure_alert_history_table(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                create table if not exists alert_history (
                    alert_key text primary key,
                    sent_at timestamptz not null
                )
                """
            )


def load_history(storage, path: str) -> dict:
    if not storage.file_exists(path):
        raise SystemExit(f"History file not found: {path}")
    raw = storage.read_text(path)
    return json.loads(raw)


def upsert_history(conn, history: dict):
    rows = [(k, v) for k, v in history.items()]
    if not rows:
        return 0
    sql = """
        insert into alert_history (alert_key, sent_at)
        values %s
        on conflict (alert_key) do update set
          sent_at = excluded.sent_at
    """
    with conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
    return len(rows)


def parse_args():
    p = argparse.ArgumentParser(description="Import GCS alert_history.json into DB.")
    p.add_argument("--bucket", default="risk-dashboard", help="GCS bucket name")
    p.add_argument("--path", default="data/alert_history.json", help="Path inside bucket")
    p.add_argument("--dry-run", action="store_true", help="Load and report only")
    return p.parse_args()


def main():
    args = parse_args()
    storage = CloudStorageManager(args.bucket)
    history = load_history(storage, args.path)
    print(f"Loaded {len(history)} alert history entries from gs://{args.bucket}/{args.path}")

    if args.dry_run:
        print("Dry run: no DB changes made.")
        return 0

    conn = get_conn()
    try:
        ensure_alert_history_table(conn)
        count = upsert_history(conn, history)
        print(f"Upserted {count} rows into alert_history.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
