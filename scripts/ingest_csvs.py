#!/usr/bin/env python3
import argparse
import csv
import os
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

from url_utils import url_hash


def parse_args():
    p = argparse.ArgumentParser(description="Ingest modal CSVs into Postgres/Supabase.")
    p.add_argument("--csv", required=True, help="Path to CSV file")
    p.add_argument("--entity-type", required=True, choices=["brand", "ceo"], help="Entity type")
    p.add_argument("--source-type", required=True, choices=["news", "serp"], help="Source type")
    p.add_argument("--date", help="YYYY-MM-DD (if CSV lacks a date column)")
    p.add_argument("--batch-size", type=int, default=1000, help="Batch size for inserts")
    p.add_argument("--map-sentiment-to-risk", action="store_true",
                   help="Set risk_raw from sentiment (negative->risk, else no_risk)")
    p.add_argument("--dry-run", action="store_true", help="Parse CSV and report counts only")
    return p.parse_args()


def get_conn():
    dsn = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not dsn:
        raise SystemExit("Set DATABASE_URL or SUPABASE_DB_URL")
    return psycopg2.connect(dsn)


def parse_bool(val):
    if val is None:
        return None
    s = str(val).strip().lower()
    if s in {"true", "1", "yes", "y", "t"}:
        return True
    if s in {"false", "0", "no", "n", "f"}:
        return False
    return None


def parse_date(val, fallback):
    if val:
        try:
            return datetime.strptime(val, "%Y-%m-%d").date()
        except ValueError:
            pass
    if fallback:
        return datetime.strptime(fallback, "%Y-%m-%d").date()
    raise SystemExit("No valid date found; provide --date or a date column")


def extract_entity_name(row, entity_type):
    if entity_type == "brand":
        return (row.get("company") or "").strip()
    return (row.get("ceo") or "").strip()


def extract_company(row, entity_type):
    if entity_type == "ceo":
        return (row.get("company") or "").strip()
    return ""


def collect_entity_names(csv_path, entity_type):
    names = set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = extract_entity_name(row, entity_type)
            if name:
                names.add(name)
    return names


def upsert_entities(cur, entity_type, names):
    if not names:
        return
    rows = [(entity_type, name) for name in sorted(names)]
    sql = """
        insert into entities (entity_type, name)
        values %s
        on conflict (entity_type, name) do nothing
    """
    execute_values(cur, sql, rows, page_size=1000)


def fetch_entity_map(cur, entity_type):
    cur.execute("select name, id from entities where entity_type = %s", (entity_type,))
    return {name: eid for name, eid in cur.fetchall()}


def build_rows(csv_path, entity_type, fallback_date, map_risk):
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            entity_name = extract_entity_name(row, entity_type)
            if not entity_name:
                continue

            date_val = parse_date(row.get("date"), fallback_date)
            title = (row.get("title") or "").strip()
            url = (row.get("url") or row.get("link") or "").strip()
            if not title or not url:
                continue
            h = url_hash(url)
            if not h:
                continue
            sent = (row.get("sentiment") or "").strip().lower()
            if map_risk:
                if sent == "negative":
                    risk_val = "risk"
                elif sent in {"positive", "neutral"}:
                    risk_val = "no_risk"
                else:
                    risk_val = None
            else:
                risk_val = None

            yield {
                "entity_name": entity_name,
                "date": date_val,
                "title": title,
                "url": url,
                "url_hash": h,
                "snippet": (row.get("snippet") or "").strip(),
                "source": (row.get("source") or "").strip(),
                "position": row.get("position"),
                "sentiment_raw": sent or None,
                "controlled_raw": parse_bool(row.get("controlled")),
                "company": extract_company(row, entity_type),
                "risk_raw": risk_val,
            }



def ingest_csv(
    conn,
    csv_path,
    entity_type,
    source_type,
    date=None,
    map_sentiment_to_risk=False,
    batch_size=1000,
    dry_run=False,
):
    entity_names = collect_entity_names(csv_path, entity_type)
    if not entity_names:
        print(f"{csv_path}: no entities found; skipping.")
        return 0

    if dry_run:
        print(f"{csv_path}: entities {len(entity_names)}")
        return 0

    with conn:
        with conn.cursor() as cur:
            upsert_entities(cur, entity_type, entity_names)

    with conn:
        with conn.cursor() as cur:
            entity_map = fetch_entity_map(cur, entity_type)

    total = 0
    batch = []
    for row in build_rows(csv_path, entity_type, date, map_sentiment_to_risk):
        entity_id = entity_map.get(row["entity_name"])
        if not entity_id:
            continue

        batch.append((
            entity_id,
            source_type,
            row["date"],
            row["title"],
            row["url"],
            row["url_hash"],
            row["snippet"],
            row["source"],
            int(float(row["position"])) if row["position"] not in (None, "") else None,
            row["sentiment_raw"],
            row["risk_raw"],
            row["controlled_raw"],
            row["company"],
        ))

        if len(batch) >= batch_size:
            total += flush_batch(conn, batch)
            batch = []

    if batch:
        total += flush_batch(conn, batch)

    print(f"{csv_path}: upserted {total} rows")
    return total


def main():
    args = parse_args()

    conn = get_conn()
    try:
        return ingest_csv(
            conn,
            args.csv,
            args.entity_type,
            args.source_type,
            date=args.date,
            map_sentiment_to_risk=args.map_sentiment_to_risk,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
        )
    finally:
        conn.close()


def flush_batch(conn, batch):
    sql = """
        insert into items (
          entity_id, source_type, date, title, url, url_hash,
          snippet, source, position, sentiment_raw, risk_raw,
          controlled_raw, company
        ) values %s
        on conflict (entity_id, source_type, date, url_hash) do update set
          title = excluded.title,
          url = excluded.url,
          snippet = excluded.snippet,
          source = excluded.source,
          position = excluded.position,
          sentiment_raw = excluded.sentiment_raw,
          risk_raw = excluded.risk_raw,
          controlled_raw = excluded.controlled_raw,
          company = excluded.company
    """
    with conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, batch, page_size=1000)
    return len(batch)


if __name__ == "__main__":
    raise SystemExit(main())
