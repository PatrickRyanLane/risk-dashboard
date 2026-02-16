#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from datetime import datetime

import psycopg2


def get_conn():
    dsn = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is required")
    return psycopg2.connect(dsn)


def backfill_company(cur, date_str: str) -> None:
    cur.execute(
        """
        insert into company_article_mentions_daily
          (date, company_id, article_id, sentiment_label, control_class, finance_routine, uncertain)
        select
          scored_at::date,
          company_id,
          article_id,
          coalesce(llm_sentiment_label, sentiment_label),
          control_class,
          finance_routine,
          uncertain
        from company_article_mentions
        where scored_at::date = %s
        on conflict (date, company_id, article_id) do update set
          sentiment_label = excluded.sentiment_label,
          control_class = excluded.control_class,
          finance_routine = excluded.finance_routine,
          uncertain = excluded.uncertain
        """,
        (date_str,),
    )
    cur.execute(
        "select count(*) from company_article_mentions_daily where date = %s",
        (date_str,),
    )
    count = cur.fetchone()[0]
    print(f"[OK] company_article_mentions_daily {date_str}: {count} rows")


def backfill_ceo(cur, date_str: str) -> None:
    cur.execute(
        """
        insert into ceo_article_mentions_daily
          (date, ceo_id, article_id, sentiment_label, control_class, finance_routine, uncertain)
        select
          scored_at::date,
          ceo_id,
          article_id,
          coalesce(llm_sentiment_label, sentiment_label),
          control_class,
          finance_routine,
          uncertain
        from ceo_article_mentions
        where scored_at::date = %s
        on conflict (date, ceo_id, article_id) do update set
          sentiment_label = excluded.sentiment_label,
          control_class = excluded.control_class,
          finance_routine = excluded.finance_routine,
          uncertain = excluded.uncertain
        """,
        (date_str,),
    )
    cur.execute(
        "select count(*) from ceo_article_mentions_daily where date = %s",
        (date_str,),
    )
    count = cur.fetchone()[0]
    print(f"[OK] ceo_article_mentions_daily {date_str}: {count} rows")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="UTC date (YYYY-MM-DD)")
    parser.add_argument(
        "--entity-type",
        choices=["company", "ceo", "both"],
        default="both",
        help="Which daily table(s) to backfill",
    )
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"Invalid date: {args.date}") from exc

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                if args.entity_type in {"company", "both"}:
                    backfill_company(cur, args.date)
                if args.entity_type in {"ceo", "both"}:
                    backfill_ceo(cur, args.date)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
