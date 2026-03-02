#!/usr/bin/env python3
"""
Backfill rule-based narrative tags on SERP Top Stories items.

Defaults:
  - 90-day lookback
  - both brand/company and ceo entities
"""
from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timedelta

import psycopg2
from psycopg2.extras import execute_batch

from risk_rules import classify_narrative_tags


def get_conn():
    dsn = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not dsn:
        raise SystemExit("Set DATABASE_URL or SUPABASE_DB_URL")
    return psycopg2.connect(dsn)


def ensure_narrative_columns(cur) -> None:
    cur.execute("alter table if exists serp_feature_items add column if not exists narrative_primary_tag text")
    cur.execute("alter table if exists serp_feature_items add column if not exists narrative_primary_group text")
    cur.execute("alter table if exists serp_feature_items add column if not exists narrative_tags text[]")
    cur.execute("alter table if exists serp_feature_items add column if not exists narrative_is_crisis boolean")
    cur.execute("alter table if exists serp_feature_items add column if not exists narrative_rule_version text")
    cur.execute("alter table if exists serp_feature_items add column if not exists narrative_tagged_at timestamptz")


def parse_args():
    p = argparse.ArgumentParser(description="Backfill narrative tags for SERP Top Stories items.")
    p.add_argument("--days", type=int, default=90, help="Lookback window in days (default: 90)")
    p.add_argument(
        "--entity-type",
        choices=["all", "brand", "ceo"],
        default="all",
        help="Filter by entity type (default: all)",
    )
    p.add_argument("--batch-size", type=int, default=2000, help="Update batch size (default: 2000)")
    return p.parse_args()


def entity_type_clause(entity_type: str):
    if entity_type == "brand":
        return "and sfi.entity_type = any(%s)", (["brand", "company"],)
    if entity_type == "ceo":
        return "and sfi.entity_type = %s", ("ceo",)
    return "", tuple()


def main() -> int:
    args = parse_args()
    days = max(1, int(args.days or 90))
    batch_size = max(100, int(args.batch_size or 2000))
    start_date = date.today() - timedelta(days=days)

    with get_conn() as conn:
        with conn.cursor() as cur:
            ensure_narrative_columns(cur)
            clause, clause_params = entity_type_clause(args.entity_type)
            sql = f"""
                select sfi.id,
                       sfi.title,
                       sfi.snippet,
                       sfi.url,
                       sfi.source,
                       coalesce(ov.override_sentiment_label, sfi.llm_sentiment_label, sfi.sentiment_label) as sentiment_label,
                       coalesce(sfi.finance_routine, false) as finance_routine
                from serp_feature_items sfi
                left join serp_feature_item_overrides ov on ov.serp_feature_item_id = sfi.id
                where sfi.date >= %s
                  and sfi.feature_type = 'top_stories_items'
                  {clause}
            """
            params = (start_date,) + clause_params
            cur.execute(sql, params)
            rows = cur.fetchall()
            print(f"[INFO] Loaded {len(rows)} rows to classify from {start_date.isoformat()} onward")

        updates = []
        now = datetime.utcnow()
        for item_id, title, snippet, url, source, sentiment, finance_routine in rows:
            tag = classify_narrative_tags(
                title or "",
                snippet or "",
                url=url or "",
                source=source or "",
                sentiment=sentiment,
                finance_routine=bool(finance_routine),
            )
            primary_tag = tag.get("primary_tag") or None
            primary_group = tag.get("primary_group") or None
            tags = tag.get("tags") or None
            is_crisis = tag.get("is_crisis")
            rule_version = tag.get("rule_version") or None
            tagged_at = now if primary_tag else None
            updates.append(
                (
                    primary_tag,
                    primary_group,
                    tags,
                    is_crisis,
                    rule_version,
                    tagged_at,
                    item_id,
                )
            )

        if not updates:
            print("[INFO] Nothing to update")
            return 0

        with conn.cursor() as cur:
            update_sql = """
                update serp_feature_items
                   set narrative_primary_tag = %s,
                       narrative_primary_group = %s,
                       narrative_tags = %s,
                       narrative_is_crisis = %s,
                       narrative_rule_version = %s,
                       narrative_tagged_at = %s,
                       updated_at = now()
                 where id = %s
            """
            total = len(updates)
            for i in range(0, total, batch_size):
                chunk = updates[i:i + batch_size]
                execute_batch(cur, update_sql, chunk, page_size=500)
                print(f"[INFO] Updated {i + len(chunk)}/{total}")
        conn.commit()
    print("[OK] Narrative backfill complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

