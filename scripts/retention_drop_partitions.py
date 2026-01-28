#!/usr/bin/env python3
import argparse
from datetime import date
import os
import re

import psycopg2


PARTITION_RE = re.compile(r"^(?P<prefix>company_article_mentions_daily|ceo_article_mentions_daily)_(?P<ym>\d{4}_\d{2})$")


def _months_ago(anchor: date, months: int) -> date:
    year = anchor.year
    month = anchor.month - months
    while month <= 0:
        month += 12
        year -= 1
    return date(year, month, 1)


def _list_partitions(cur, parent: str):
    cur.execute(
        """
        select inhrelid::regclass::text
        from pg_inherits
        where inhparent = %s::regclass
        """,
        (parent,),
    )
    return [row[0] for row in cur.fetchall()]


def _parse_ym(name: str) -> date | None:
    match = PARTITION_RE.match(name)
    if not match:
        return None
    ym = match.group("ym")
    year, month = ym.split("_", 1)
    return date(int(year), int(month), 1)


def drop_old_partitions(conn, retention_months: int) -> int:
    today = date.today()
    cutoff = _months_ago(today, retention_months)
    dropped = 0
    with conn.cursor() as cur:
        for parent in ("company_article_mentions_daily", "ceo_article_mentions_daily"):
            for part in _list_partitions(cur, parent):
                part_date = _parse_ym(part)
                if not part_date:
                    continue
                if part_date < cutoff:
                    cur.execute(f"drop table if exists {part}")
                    dropped += 1
                    print(f"[OK] Dropped partition {part} (month {part_date})")
    conn.commit()
    return dropped


def main():
    ap = argparse.ArgumentParser(description="Drop daily mention partitions older than retention window.")
    ap.add_argument("--months", type=int, default=int(os.getenv("RETENTION_MONTHS", "24")))
    args = ap.parse_args()
    dsn = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not dsn:
        raise SystemExit("Set DATABASE_URL or SUPABASE_DB_URL")
    if args.months < 1:
        raise SystemExit("Retention months must be >= 1")
    with psycopg2.connect(dsn) as conn:
        dropped = drop_old_partitions(conn, args.months)
    print(f"[DONE] Dropped {dropped} partitions older than {args.months} months.")


if __name__ == "__main__":
    main()
