#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import psycopg2


ITEM_FEATURE_TYPES = (
    "aio_citations",
    "paa_items",
    "videos_items",
    "perspectives_items",
)


def get_conn():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is required")
    return psycopg2.connect(dsn)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7, help="Days back to recompute (inclusive)")
    args = parser.parse_args()

    days = max(1, args.days)
    feature_list = ",".join(["%s"] * len(ITEM_FEATURE_TYPES))

    sql = f"""
        insert into serp_feature_daily
          (date, entity_type, entity_id, entity_name, feature_type,
           total_count, positive_count, neutral_count, negative_count, source)
        select
          sfi.date,
          sfi.entity_type,
          sfi.entity_id,
          sfi.entity_name,
          sfi.feature_type,
          count(*) as total_count,
          sum(case when coalesce(ov.override_sentiment_label, sfi.sentiment_label) = 'positive' then 1 else 0 end) as positive_count,
          sum(case when coalesce(ov.override_sentiment_label, sfi.sentiment_label) = 'neutral' then 1 else 0 end) as neutral_count,
          sum(case when coalesce(ov.override_sentiment_label, sfi.sentiment_label) = 'negative' then 1 else 0 end) as negative_count,
          'item_recompute' as source
        from serp_feature_items sfi
        left join serp_feature_item_overrides ov on ov.serp_feature_item_id = sfi.id
        where sfi.date >= (current_date - (%s || ' days')::interval)
          and sfi.feature_type in ({feature_list})
        group by sfi.date, sfi.entity_type, sfi.entity_id, sfi.entity_name, sfi.feature_type
        on conflict (date, entity_type, entity_name, feature_type) do update set
          entity_id = excluded.entity_id,
          total_count = excluded.total_count,
          positive_count = excluded.positive_count,
          neutral_count = excluded.neutral_count,
          negative_count = excluded.negative_count,
          source = excluded.source,
          updated_at = now()
    """

    params = [days, *ITEM_FEATURE_TYPES]
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()

    print(f"Recomputed serp_feature_daily for last {days} days (item features only).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
