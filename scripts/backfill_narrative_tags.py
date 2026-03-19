#!/usr/bin/env python3
"""
Backfill rule-based narrative tags on SERP Top Stories items.

Defaults:
  - 90-day lookback
  - both brand/company and ceo entities
  - narrative tags require at least 2 negative, non-financial Top Stories URLs
    per entity/day (override with --narrative-min-negative-top-stories)
"""
from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timedelta

import psycopg2
from psycopg2.extras import execute_batch

from narrative_rollups import (
    delete_entity_crisis_tag_daily_window,
    ensure_entity_crisis_tag_daily_table,
    upsert_entity_crisis_tag_daily,
)
from risk_rules import (
    NARRATIVE_MIN_NEG_TOP_STORIES,
    rollup_entity_day_narrative,
)


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
    p.add_argument(
        "--narrative-min-negative-top-stories",
        type=int,
        default=int(os.getenv("NARRATIVE_MIN_NEG_TOP_STORIES", str(NARRATIVE_MIN_NEG_TOP_STORIES))),
        help=(
            "Minimum negative, non-financial Top Stories URLs required per entity/day "
            "before assigning narrative tags (default: env NARRATIVE_MIN_NEG_TOP_STORIES or 2)"
        ),
    )
    return p.parse_args()


def entity_type_clause(entity_type: str):
    if entity_type == "brand":
        return "and sfi.entity_type = any(%s)", (["brand", "company"],)
    if entity_type == "ceo":
        return "and sfi.entity_type = %s", ("ceo",)
    return "", tuple()


def entity_types_for_window(entity_type: str) -> list[str]:
    if entity_type == "brand":
        return ["brand", "company"]
    if entity_type == "ceo":
        return ["ceo"]
    return ["brand", "company", "ceo"]


def main() -> int:
    args = parse_args()
    days = max(1, int(args.days or 90))
    batch_size = max(100, int(args.batch_size or 2000))
    narrative_min_negative_top_stories = max(1, int(args.narrative_min_negative_top_stories or 1))
    start_date = date.today() - timedelta(days=days)
    print(
        f"[INFO] Narrative gate: min_negative_top_stories={narrative_min_negative_top_stories} "
        f"(window={days}d, entity_type={args.entity_type})"
    )

    with get_conn() as conn:
        with conn.cursor() as cur:
            ensure_narrative_columns(cur)
            ensure_entity_crisis_tag_daily_table(cur)
            clause, clause_params = entity_type_clause(args.entity_type)
            sql = f"""
                select sfi.id,
                       sfi.date,
                       sfi.entity_type,
                       sfi.entity_id,
                       sfi.entity_name,
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
        rollup_rows = []
        candidates = 0
        tagged = 0
        suppressed = 0
        tagged_entity_days = 0
        now = datetime.utcnow()
        grouped_rows = {}
        for item_id, row_date, row_entity_type, row_entity_id, row_entity_name, title, snippet, url, source, sentiment, finance_routine in rows:
            key = (
                row_date,
                str(row_entity_type or ""),
                row_entity_id,
                str(row_entity_name or ""),
            )
            grouped_rows.setdefault(key, []).append(
                {
                    "item_id": item_id,
                    "title": title or "",
                    "snippet": snippet or "",
                    "url": url or "",
                    "source": source or "",
                    "sentiment_label": sentiment,
                    "finance_routine": bool(finance_routine),
                }
            )

        for (row_date, row_entity_type, row_entity_id, row_entity_name), items in grouped_rows.items():
            rollup = rollup_entity_day_narrative(
                items,
                min_negative_top_stories=narrative_min_negative_top_stories,
            )
            candidates += int(rollup.get("negative_item_count") or 0)
            tagged += int(rollup.get("tagged_item_count") or 0)
            if not rollup.get("gate_met"):
                suppressed += int(rollup.get("negative_item_count") or 0)
            if rollup.get("primary_tag"):
                tagged_entity_days += 1
            rollup_rows.append(
                (
                    row_date,
                    row_entity_type,
                    row_entity_id,
                    row_entity_name,
                    rollup.get("primary_tag") or None,
                    rollup.get("primary_group") or None,
                    rollup.get("tags") or None,
                    rollup.get("is_crisis"),
                    int(rollup.get("negative_item_count") or 0),
                    int(rollup.get("tagged_item_count") or 0),
                    int(rollup.get("unmatched_negative_items") or 0),
                    int(rollup.get("supporting_negative_items") or 0),
                    rollup.get("tag_counts") or {},
                    rollup.get("rule_version") or None,
                    now if rollup.get("primary_tag") else None,
                )
            )
            for item, tag in zip(items, rollup.get("item_results") or []):
                updates.append(
                    (
                        tag.get("primary_tag") or None,
                        tag.get("primary_group") or None,
                        tag.get("tags") or None,
                        tag.get("is_crisis"),
                        tag.get("rule_version") or None,
                        now if tag.get("primary_tag") else None,
                        item["item_id"],
                    )
                )

        print(
            "[INFO] Narrative tagging summary: "
            f"candidates={candidates} tagged_items={tagged} "
            f"tagged_entity_days={tagged_entity_days} suppressed_by_gate={suppressed}"
        )

        with conn.cursor() as cur:
            delete_entity_crisis_tag_daily_window(
                cur,
                start_date,
                date.today(),
                entity_types_for_window(args.entity_type),
            )
            if rollup_rows:
                upsert_entity_crisis_tag_daily(cur, rollup_rows)
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
