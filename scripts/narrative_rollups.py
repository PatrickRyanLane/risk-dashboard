#!/usr/bin/env python3
from __future__ import annotations

from typing import Iterable

from psycopg2.extras import Json, execute_values


def ensure_entity_crisis_tag_daily_table(cur) -> None:
    cur.execute(
        """
        create table if not exists entity_crisis_tag_daily (
          date date not null,
          entity_type text not null,
          entity_id uuid,
          entity_name text not null,
          primary_tag text,
          primary_group text,
          tags text[],
          is_crisis boolean,
          negative_top_stories_count int not null default 0,
          tagged_item_count int not null default 0,
          unmatched_negative_items int not null default 0,
          supporting_negative_items int not null default 0,
          tag_counts jsonb not null default '{}'::jsonb,
          narrative_rule_version text,
          tagged_at timestamptz,
          created_at timestamptz not null default now(),
          updated_at timestamptz not null default now()
        )
        """
    )
    cur.execute(
        """
        create unique index if not exists entity_crisis_tag_daily_unique_idx
            on entity_crisis_tag_daily (date, entity_type, entity_name)
        """
    )
    cur.execute(
        """
        create index if not exists entity_crisis_tag_daily_entity_id_idx
            on entity_crisis_tag_daily (entity_type, entity_id, date)
        """
    )
    cur.execute(
        """
        create index if not exists entity_crisis_tag_daily_tag_idx
            on entity_crisis_tag_daily (primary_tag, date)
        """
    )


def delete_entity_crisis_tag_daily_window(cur, start_date, end_date, entity_types: Iterable[str]) -> None:
    types = list(entity_types or [])
    if not types:
        return
    cur.execute(
        """
        delete from entity_crisis_tag_daily
         where date between %s and %s
           and entity_type = any(%s)
        """,
        (start_date, end_date, types),
    )


def upsert_entity_crisis_tag_daily(cur, rows: list[tuple]) -> int:
    if not rows:
        return 0
    sql = """
        insert into entity_crisis_tag_daily (
          date,
          entity_type,
          entity_id,
          entity_name,
          primary_tag,
          primary_group,
          tags,
          is_crisis,
          negative_top_stories_count,
          tagged_item_count,
          unmatched_negative_items,
          supporting_negative_items,
          tag_counts,
          narrative_rule_version,
          tagged_at
        )
        values %s
        on conflict (date, entity_type, entity_name) do update set
          entity_id = excluded.entity_id,
          primary_tag = excluded.primary_tag,
          primary_group = excluded.primary_group,
          tags = excluded.tags,
          is_crisis = excluded.is_crisis,
          negative_top_stories_count = excluded.negative_top_stories_count,
          tagged_item_count = excluded.tagged_item_count,
          unmatched_negative_items = excluded.unmatched_negative_items,
          supporting_negative_items = excluded.supporting_negative_items,
          tag_counts = excluded.tag_counts,
          narrative_rule_version = excluded.narrative_rule_version,
          tagged_at = excluded.tagged_at,
          updated_at = now()
    """
    normalized_rows = [
        (
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            row[8],
            row[9],
            row[10],
            row[11],
            Json(row[12] or {}),
            row[13],
            row[14],
        )
        for row in rows
    ]
    execute_values(cur, sql, normalized_rows, page_size=1000)
    return len(rows)
