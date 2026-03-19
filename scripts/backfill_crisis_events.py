#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from datetime import date, timedelta

import psycopg2

from crisis_event_rollups import (
    ensure_entity_crisis_event_daily_table,
    recompute_entity_crisis_event_window,
)
from risk_rules import NARRATIVE_MIN_NEG_TOP_STORIES


def get_conn():
    dsn = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not dsn:
        raise SystemExit("Set DATABASE_URL or SUPABASE_DB_URL")
    return psycopg2.connect(dsn)


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill mixed-source daily crisis events.")
    parser.add_argument("--days", type=int, default=90, help="Lookback window in days (default: 90)")
    parser.add_argument(
        "--entity-type",
        choices=["all", "brand", "ceo"],
        default="all",
        help="Filter by entity type (default: all)",
    )
    parser.add_argument(
        "--narrative-min-negative-top-stories",
        type=int,
        default=int(os.getenv("NARRATIVE_MIN_NEG_TOP_STORIES", str(NARRATIVE_MIN_NEG_TOP_STORIES))),
        help="Minimum negative Top Stories required for the Top Stories trigger (default: env or 2).",
    )
    return parser.parse_args()


def entity_types_for_window(entity_type: str) -> list[str]:
    if entity_type == "brand":
        return ["brand"]
    if entity_type == "ceo":
        return ["ceo"]
    return ["brand", "ceo"]


def main() -> int:
    args = parse_args()
    days = max(1, int(args.days or 90))
    start_date = date.today() - timedelta(days=days)
    end_date = date.today()
    entity_types = entity_types_for_window(args.entity_type)
    narrative_min_negative_top_stories = max(1, int(args.narrative_min_negative_top_stories or 1))

    print(
        f"[INFO] Crisis event backfill window={start_date.isoformat()}..{end_date.isoformat()} "
        f"entity_types={entity_types} min_negative_top_stories={narrative_min_negative_top_stories}"
    )

    with get_conn() as conn:
        with conn.cursor() as cur:
            ensure_entity_crisis_event_daily_table(cur)
            rows = recompute_entity_crisis_event_window(
                cur,
                start_date,
                end_date,
                entity_types,
                narrative_min_negative_top_stories=narrative_min_negative_top_stories,
            )
        conn.commit()

    print(f"[INFO] Crisis events written: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
