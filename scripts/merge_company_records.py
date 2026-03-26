#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Iterable, Sequence

import psycopg2
from psycopg2.extras import execute_batch

from crisis_event_rollups import (
    ensure_entity_crisis_event_daily_table,
    recompute_entity_crisis_event_window,
)
from narrative_rollups import (
    ensure_entity_crisis_tag_daily_table,
    upsert_entity_crisis_tag_daily,
)
from risk_rules import (
    NARRATIVE_MIN_NEG_TOP_STORIES,
    rollup_entity_day_narrative,
)


MERGE_LOCK_KEY = int(os.getenv("COMPANY_MERGE_LOCK_KEY", "135792468"))
REFRESH_LOCK_KEY = int(os.getenv("REFRESH_LOCK_KEY", "918273645"))

EXPECTED_COMPANY_FKS = {
    ("boards", "company_id"),
    ("ceos", "company_id"),
    ("company_article_mentions", "company_id"),
    ("company_article_mentions_daily", "company_id"),
    ("company_article_overrides", "company_id"),
    ("serp_runs", "company_id"),
    ("user_company_access", "company_id"),
}

EXPECTED_CEO_FKS = {
    ("boards", "ceo_id"),
    ("ceo_article_mentions", "ceo_id"),
    ("ceo_article_mentions_daily", "ceo_id"),
    ("ceo_article_overrides", "ceo_id"),
    ("serp_runs", "ceo_id"),
}

PARTITION_PARENT_TABLES = (
    "company_article_mentions_daily",
    "ceo_article_mentions_daily",
)


@dataclass(frozen=True)
class CompanyRow:
    id: str
    name: str
    ticker: str | None
    sector: str | None
    websites: str | None
    favorite: bool


@dataclass(frozen=True)
class CeoRow:
    id: str
    name: str
    alias: str | None
    favorite: bool


def log(message: str) -> None:
    print(message, flush=True)


def normalize_relname(relname: str) -> str:
    name = relname.split(".")[-1]
    for parent in PARTITION_PARENT_TABLES:
        if name == parent or name.startswith(f"{parent}_"):
            return parent
    return name


def get_conn(dsn: str | None, app_name: str):
    resolved = dsn or os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not resolved:
        raise SystemExit("Set DATABASE_URL or SUPABASE_DB_URL, or pass --dsn")
    return psycopg2.connect(resolved, application_name=app_name)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge one company record into another, including core references and derived tables.",
    )
    parser.add_argument("--source-name", help="Name of the company to merge from")
    parser.add_argument("--target-name", help="Name of the company to merge into")
    parser.add_argument("--source-id", help="ID of the company to merge from")
    parser.add_argument("--target-id", help="ID of the company to merge into")
    parser.add_argument("--dsn", help="Override DATABASE_URL/SUPABASE_DB_URL")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Commit the merge. Without this flag the script only previews.",
    )
    parser.add_argument(
        "--skip-refresh",
        action="store_true",
        help="Skip refreshing materialized views after a successful merge.",
    )
    parser.add_argument(
        "--skip-derived-rebuilds",
        action="store_true",
        help="Skip rebuilding derived SERP and crisis tables after the merge.",
    )
    parser.add_argument(
        "--narrative-min-negative-top-stories",
        type=int,
        default=int(os.getenv("NARRATIVE_MIN_NEG_TOP_STORIES", str(NARRATIVE_MIN_NEG_TOP_STORIES))),
        help="Narrative tagging threshold used when rebuilding derived tables.",
    )
    args = parser.parse_args()

    if not args.source_name and not args.source_id:
        raise SystemExit("Provide --source-name or --source-id")
    if not args.target_name and not args.target_id:
        raise SystemExit("Provide --target-name or --target-id")
    return args


def fetch_fk_refs(cur, parent_table: str) -> set[tuple[str, str]]:
    cur.execute(
        """
        select con.conrelid::regclass::text as child_table,
               att.attname as child_column
        from pg_constraint con
        join lateral unnest(con.conkey) with ordinality as cols(attnum, ordinality) on true
        join pg_attribute att
          on att.attrelid = con.conrelid
         and att.attnum = cols.attnum
        where con.contype = 'f'
          and con.confrelid = to_regclass(%s)
        order by 1, 2
        """,
        (parent_table,),
    )
    return {(normalize_relname(relname), column) for relname, column in cur.fetchall()}


def ensure_handled_foreign_keys(cur) -> None:
    company_fks = fetch_fk_refs(cur, "companies")
    ceo_fks = fetch_fk_refs(cur, "ceos")
    unexpected_company = sorted(company_fks - EXPECTED_COMPANY_FKS)
    unexpected_ceo = sorted(ceo_fks - EXPECTED_CEO_FKS)
    if unexpected_company or unexpected_ceo:
        raise RuntimeError(
            "Unhandled foreign key references detected. "
            f"companies={unexpected_company or '[]'} ceos={unexpected_ceo or '[]'}"
        )


def _resolve_company_by_query(cur, sql_text: str, value: str, label: str) -> CompanyRow:
    cur.execute(sql_text, (value,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"{label} company not found for value={value}")
    return CompanyRow(*row)


def resolve_company(
    cur,
    *,
    row_id: str | None,
    name: str | None,
    label: str,
) -> CompanyRow:
    row_by_id = None
    row_by_name = None
    select_sql = """
        select id::text, name, ticker, sector, websites, favorite
        from companies
        where {predicate}
    """
    if row_id:
        row_by_id = _resolve_company_by_query(
            cur,
            select_sql.format(predicate="id = %s::uuid"),
            row_id,
            label,
        )
    if name:
        row_by_name = _resolve_company_by_query(
            cur,
            select_sql.format(predicate="name = %s"),
            name,
            label,
        )
    if row_by_id and row_by_name and row_by_id.id != row_by_name.id:
        raise RuntimeError(
            f"{label} company mismatch: id={row_id} resolves to {row_by_id.name}, "
            f"name={name} resolves to {row_by_name.id}"
        )
    resolved = row_by_id or row_by_name
    if not resolved:
        raise RuntimeError(f"Unable to resolve {label} company")
    return resolved


def list_company_ceos(cur, company_id: str) -> list[CeoRow]:
    cur.execute(
        """
        select id::text, name, alias, favorite
        from ceos
        where company_id = %s::uuid
        order by lower(name), created_at, id
        """,
        (company_id,),
    )
    return [CeoRow(*row) for row in cur.fetchall()]


def fetch_company_counts(cur, company: CompanyRow) -> dict[str, int]:
    queries = {
        "ceos": ("select count(*) from ceos where company_id = %s::uuid", (company.id,)),
        "company_mentions": (
            "select count(*) from company_article_mentions where company_id = %s::uuid",
            (company.id,),
        ),
        "company_mentions_daily": (
            "select count(*) from company_article_mentions_daily where company_id = %s::uuid",
            (company.id,),
        ),
        "company_overrides": (
            "select count(*) from company_article_overrides where company_id = %s::uuid",
            (company.id,),
        ),
        "access_rows": (
            "select count(*) from user_company_access where company_id = %s::uuid",
            (company.id,),
        ),
        "board_rows": (
            "select count(*) from boards where company_id = %s::uuid",
            (company.id,),
        ),
        "company_serp_runs": (
            "select count(*) from serp_runs where entity_type = 'company' and company_id = %s::uuid",
            (company.id,),
        ),
        "company_serp_results": (
            """
            select count(*)
            from serp_results sr
            join serp_runs run on run.id = sr.serp_run_id
            where run.entity_type = 'company'
              and run.company_id = %s::uuid
            """,
            (company.id,),
        ),
        "serp_feature_items": (
            """
            select count(*)
            from serp_feature_items
            where entity_type = any(%s)
              and (entity_id = %s::uuid or entity_name = %s)
            """,
            (["brand", "company"], company.id, company.name),
        ),
        "serp_feature_summaries": (
            """
            select count(*)
            from serp_feature_summaries
            where entity_type = any(%s)
              and entity_id = %s::uuid
            """,
            (["brand", "company"], company.id),
        ),
    }
    counts: dict[str, int] = {}
    for key, (sql_text, params) in queries.items():
        cur.execute(sql_text, params)
        counts[key] = int(cur.fetchone()[0])
    return counts


def preview_overlaps(cur, source: CompanyRow, target: CompanyRow) -> dict[str, int]:
    overlap_queries = {
        "ceo_name_overlaps": (
            """
            select count(*)
            from ceos src
            join ceos tgt
              on lower(tgt.name) = lower(src.name)
             and tgt.company_id = %s::uuid
            where src.company_id = %s::uuid
            """,
            (target.id, source.id),
        ),
        "article_overlaps": (
            """
            select count(*)
            from company_article_mentions src
            join company_article_mentions tgt
              on tgt.article_id = src.article_id
             and tgt.company_id = %s::uuid
            where src.company_id = %s::uuid
            """,
            (target.id, source.id),
        ),
        "daily_article_overlaps": (
            """
            select count(*)
            from company_article_mentions_daily src
            join company_article_mentions_daily tgt
              on tgt.date = src.date
             and tgt.article_id = src.article_id
             and tgt.company_id = %s::uuid
            where src.company_id = %s::uuid
            """,
            (target.id, source.id),
        ),
        "company_run_overlaps": (
            """
            select count(*)
            from serp_runs src
            join serp_runs tgt
              on tgt.entity_type = 'company'
             and tgt.company_id = %s::uuid
             and tgt.run_at = src.run_at
            where src.entity_type = 'company'
              and src.company_id = %s::uuid
            """,
            (target.id, source.id),
        ),
        "feature_item_overlaps": (
            """
            select count(*)
            from serp_feature_items src
            join serp_feature_items tgt
              on tgt.entity_type = src.entity_type
             and tgt.date = src.date
             and tgt.feature_type = src.feature_type
             and tgt.url_hash is not distinct from src.url_hash
             and (tgt.entity_id = %s::uuid or (tgt.entity_id is null and tgt.entity_name = %s))
            where src.entity_type = any(%s)
              and (src.entity_id = %s::uuid or (src.entity_id is null and src.entity_name = %s))
              and tgt.id <> src.id
            """,
            (target.id, target.name, ["brand", "company"], source.id, source.name),
        ),
        "trend_day_overlaps": (
            """
            select count(*)
            from trends_daily src
            join trends_daily tgt
              on tgt.date = src.date
             and tgt.company = %s
            where src.company = %s
            """,
            (target.name, source.name),
        ),
    }
    overlaps: dict[str, int] = {}
    for key, (sql_text, params) in overlap_queries.items():
        cur.execute(sql_text, params)
        overlaps[key] = int(cur.fetchone()[0])
    return overlaps


def print_preview(cur, source: CompanyRow, target: CompanyRow) -> None:
    log("[PREVIEW] merge source -> target")
    log(f"  source: {source.name} ({source.id})")
    log(f"  target: {target.name} ({target.id})")
    source_counts = fetch_company_counts(cur, source)
    target_counts = fetch_company_counts(cur, target)
    for label, company, counts in (
        ("source", source, source_counts),
        ("target", target, target_counts),
    ):
        summary = " ".join(f"{key}={value}" for key, value in counts.items())
        log(f"[PREVIEW] {label} {company.name}: {summary}")
    overlaps = preview_overlaps(cur, source, target)
    summary = " ".join(f"{key}={value}" for key, value in overlaps.items())
    log(f"[PREVIEW] likely collisions: {summary}")

    source_ceos = list_company_ceos(cur, source.id)
    target_ceos = list_company_ceos(cur, target.id)
    target_by_name = {ceo.name.casefold(): ceo for ceo in target_ceos}
    log(f"[PREVIEW] source CEOs={len(source_ceos)} target CEOs={len(target_ceos)}")
    for ceo in source_ceos:
        match = target_by_name.get(ceo.name.casefold())
        if match:
            log(f"  duplicate CEO: {ceo.name} source={ceo.id} target={match.id}")
        else:
            log(f"  move CEO only: {ceo.name} id={ceo.id}")


def merge_text_company_tables(cur, source: CompanyRow, target: CompanyRow) -> None:
    log("[STEP] merge text-keyed company tables")

    cur.execute(
        """
        insert into roster (ceo, company, ceo_alias, websites, stock, sector, created_at)
        select ceo, %s, ceo_alias, websites, stock, sector, created_at
        from roster
        where company = %s
        on conflict (company) do nothing
        """,
        (target.name, source.name),
    )
    cur.execute("delete from roster where company = %s", (source.name,))

    cur.execute(
        """
        insert into stock_prices_daily (ticker, company, date, price, created_at)
        select ticker, %s, date, price, created_at
        from stock_prices_daily
        where company = %s
        on conflict (ticker, date) do nothing
        """,
        (target.name, source.name),
    )
    cur.execute("delete from stock_prices_daily where company = %s", (source.name,))

    cur.execute(
        """
        insert into stock_price_snapshots (
          ticker, company, as_of_date, opening_price, daily_change_pct,
          seven_day_change_pct, last_updated, created_at
        )
        select
          ticker, %s, as_of_date, opening_price, daily_change_pct,
          seven_day_change_pct, last_updated, created_at
        from stock_price_snapshots
        where company = %s
        on conflict (ticker, last_updated) do nothing
        """,
        (target.name, source.name),
    )
    cur.execute("delete from stock_price_snapshots where company = %s", (source.name,))

    cur.execute(
        """
        insert into trends_daily (company, date, interest, created_at)
        select %s, date, interest, created_at
        from trends_daily
        where company = %s
        on conflict (company, date) do nothing
        """,
        (target.name, source.name),
    )
    cur.execute("delete from trends_daily where company = %s", (source.name,))

    cur.execute(
        """
        insert into trends_snapshots (company, avg_interest, last_updated, created_at)
        select %s, avg_interest, last_updated, created_at
        from trends_snapshots
        where company = %s
        on conflict (company, last_updated) do nothing
        """,
        (target.name, source.name),
    )
    cur.execute("delete from trends_snapshots where company = %s", (source.name,))

    cur.execute("update items set company = %s where company = %s", (target.name, source.name))


def merge_company_article_tables(cur, source: CompanyRow, target: CompanyRow) -> None:
    log("[STEP] merge company article/access tables")

    cur.execute(
        """
        insert into company_article_mentions_daily (
          date, company_id, article_id, sentiment_label, control_class,
          finance_routine, uncertain, created_at
        )
        select
          date, %s::uuid, article_id, sentiment_label, control_class,
          finance_routine, uncertain, created_at
        from company_article_mentions_daily
        where company_id = %s::uuid
        on conflict (date, company_id, article_id) do nothing
        """,
        (target.id, source.id),
    )
    cur.execute("delete from company_article_mentions_daily where company_id = %s::uuid", (source.id,))

    cur.execute(
        """
        insert into company_article_mentions (
          company_id, article_id, model_sentiment_score, sentiment_label,
          model_relevant, control_class, finance_routine, uncertain,
          uncertain_reason, llm_label, llm_sentiment_label, llm_risk_label,
          llm_control_class, llm_severity, llm_reason, model_version,
          run_id, scored_at
        )
        select
          %s::uuid, article_id, model_sentiment_score, sentiment_label,
          model_relevant, control_class, finance_routine, uncertain,
          uncertain_reason, llm_label, llm_sentiment_label, llm_risk_label,
          llm_control_class, llm_severity, llm_reason, model_version,
          run_id, scored_at
        from company_article_mentions
        where company_id = %s::uuid
        on conflict (company_id, article_id) do nothing
        """,
        (target.id, source.id),
    )
    cur.execute("delete from company_article_mentions where company_id = %s::uuid", (source.id,))

    cur.execute(
        """
        insert into company_article_overrides (
          company_id, article_id, override_sentiment_score, override_sentiment_label,
          override_relevant, override_control_class, note, edited_by, edited_at
        )
        select
          %s::uuid, article_id, override_sentiment_score, override_sentiment_label,
          override_relevant, override_control_class, note, edited_by, edited_at
        from company_article_overrides
        where company_id = %s::uuid
        on conflict (company_id, article_id) do nothing
        """,
        (target.id, source.id),
    )
    cur.execute("delete from company_article_overrides where company_id = %s::uuid", (source.id,))

    cur.execute(
        """
        insert into user_company_access (user_id, company_id, created_at)
        select user_id, %s::uuid, created_at
        from user_company_access
        where company_id = %s::uuid
        on conflict (user_id, company_id) do nothing
        """,
        (target.id, source.id),
    )
    cur.execute("delete from user_company_access where company_id = %s::uuid", (source.id,))


def merge_company_feature_items(cur, source: CompanyRow, target: CompanyRow) -> None:
    log("[STEP] merge company SERP feature items")
    cur.execute("drop table if exists tmp_company_feature_item_map")
    cur.execute(
        """
        create temp table tmp_company_feature_item_map on commit drop as
        select src.id as source_item_id,
               tgt.id as target_item_id
        from serp_feature_items src
        join serp_feature_items tgt
          on tgt.entity_type = src.entity_type
         and tgt.date = src.date
         and tgt.feature_type = src.feature_type
         and tgt.url_hash is not distinct from src.url_hash
         and (tgt.entity_id = %s::uuid or (tgt.entity_id is null and tgt.entity_name = %s))
         and tgt.id <> src.id
        where src.entity_type = any(%s)
          and (src.entity_id = %s::uuid or (src.entity_id is null and src.entity_name = %s))
        """,
        (target.id, target.name, ["brand", "company"], source.id, source.name),
    )
    cur.execute(
        """
        insert into serp_feature_item_overrides (
          serp_feature_item_id, override_sentiment_label, override_control_class,
          note, edited_by, edited_at, created_at
        )
        select
          map.target_item_id,
          ov.override_sentiment_label,
          ov.override_control_class,
          ov.note,
          ov.edited_by,
          ov.edited_at,
          ov.created_at
        from tmp_company_feature_item_map map
        join serp_feature_item_overrides ov
          on ov.serp_feature_item_id = map.source_item_id
        on conflict (serp_feature_item_id) do nothing
        """
    )
    cur.execute(
        """
        update serp_feature_items sfi
           set entity_id = %s::uuid,
               entity_name = %s,
               updated_at = now()
         where sfi.entity_type = any(%s)
           and (sfi.entity_id = %s::uuid or (sfi.entity_id is null and sfi.entity_name = %s))
           and not exists (
             select 1
             from tmp_company_feature_item_map map
             where map.source_item_id = sfi.id
           )
        """,
        (target.id, target.name, ["brand", "company"], source.id, source.name),
    )
    cur.execute(
        """
        delete from serp_feature_items
        where id in (select source_item_id from tmp_company_feature_item_map)
        """
    )


def merge_company_feature_summaries(cur, source: CompanyRow, target: CompanyRow) -> None:
    log("[STEP] merge company SERP feature summaries")
    cur.execute(
        """
        insert into serp_feature_summaries (
          date, entity_type, entity_id, entity_name, feature_type, summary_text,
          provider, model, created_at, updated_at
        )
        select
          date, entity_type, %s::uuid, %s, feature_type, summary_text,
          provider, model, created_at, updated_at
        from serp_feature_summaries
        where entity_type = any(%s)
          and entity_id = %s::uuid
        on conflict (date, entity_type, entity_id, feature_type) do nothing
        """,
        (target.id, target.name, ["brand", "company"], source.id),
    )
    cur.execute(
        """
        delete from serp_feature_summaries
        where entity_type = any(%s)
          and entity_id = %s::uuid
        """,
        (["brand", "company"], source.id),
    )


def merge_company_runs(cur, source: CompanyRow, target: CompanyRow) -> None:
    log("[STEP] merge company SERP runs/results")
    cur.execute("drop table if exists tmp_company_run_map")
    cur.execute(
        """
        create temp table tmp_company_run_map on commit drop as
        select src.id as source_run_id,
               tgt.id as target_run_id
        from serp_runs src
        join serp_runs tgt
          on tgt.entity_type = 'company'
         and tgt.company_id = %s::uuid
         and tgt.run_at = src.run_at
        where src.entity_type = 'company'
          and src.company_id = %s::uuid
        """,
        (target.id, source.id),
    )

    cur.execute("drop table if exists tmp_company_result_map")
    cur.execute(
        """
        create temp table tmp_company_result_map on commit drop as
        select src.id as source_result_id,
               tgt.id as target_result_id
        from serp_results src
        join tmp_company_run_map map
          on map.source_run_id = src.serp_run_id
        join serp_results tgt
          on tgt.serp_run_id = map.target_run_id
         and tgt.rank is not distinct from src.rank
         and tgt.url_hash is not distinct from src.url_hash
        """
    )

    cur.execute(
        """
        insert into serp_result_overrides (
          serp_result_id, override_sentiment_label, override_control_class,
          note, edited_by, edited_at
        )
        select
          map.target_result_id,
          ov.override_sentiment_label,
          ov.override_control_class,
          ov.note,
          ov.edited_by,
          ov.edited_at
        from tmp_company_result_map map
        join serp_result_overrides ov
          on ov.serp_result_id = map.source_result_id
        on conflict (serp_result_id) do nothing
        """
    )

    cur.execute(
        """
        update serp_results sr
           set serp_run_id = map.target_run_id
          from tmp_company_run_map map
         where sr.serp_run_id = map.source_run_id
           and not exists (
             select 1
             from serp_results tgt
             where tgt.serp_run_id = map.target_run_id
               and tgt.rank is not distinct from sr.rank
               and tgt.url_hash is not distinct from sr.url_hash
           )
        """
    )

    cur.execute(
        """
        delete from serp_results sr
        using tmp_company_run_map map
        where sr.serp_run_id = map.source_run_id
        """
    )
    cur.execute(
        """
        delete from serp_runs
        where id in (select source_run_id from tmp_company_run_map)
        """
    )

    cur.execute(
        """
        update serp_runs
           set company_id = %s::uuid,
               query_text = %s
         where entity_type = 'company'
           and company_id = %s::uuid
        """,
        (target.id, target.name, source.id),
    )


def merge_ceo_article_tables(cur, source_ceo: CeoRow, target_ceo: CeoRow) -> None:
    cur.execute(
        """
        insert into ceo_article_mentions_daily (
          date, ceo_id, article_id, sentiment_label, control_class,
          finance_routine, uncertain, created_at
        )
        select
          date, %s::uuid, article_id, sentiment_label, control_class,
          finance_routine, uncertain, created_at
        from ceo_article_mentions_daily
        where ceo_id = %s::uuid
        on conflict (date, ceo_id, article_id) do nothing
        """,
        (target_ceo.id, source_ceo.id),
    )
    cur.execute("delete from ceo_article_mentions_daily where ceo_id = %s::uuid", (source_ceo.id,))

    cur.execute(
        """
        insert into ceo_article_mentions (
          ceo_id, article_id, model_sentiment_score, sentiment_label,
          model_relevant, control_class, finance_routine, uncertain,
          uncertain_reason, llm_label, llm_sentiment_label, llm_risk_label,
          llm_control_class, llm_severity, llm_reason, model_version,
          run_id, scored_at
        )
        select
          %s::uuid, article_id, model_sentiment_score, sentiment_label,
          model_relevant, control_class, finance_routine, uncertain,
          uncertain_reason, llm_label, llm_sentiment_label, llm_risk_label,
          llm_control_class, llm_severity, llm_reason, model_version,
          run_id, scored_at
        from ceo_article_mentions
        where ceo_id = %s::uuid
        on conflict (ceo_id, article_id) do nothing
        """,
        (target_ceo.id, source_ceo.id),
    )
    cur.execute("delete from ceo_article_mentions where ceo_id = %s::uuid", (source_ceo.id,))

    cur.execute(
        """
        insert into ceo_article_overrides (
          ceo_id, article_id, override_sentiment_score, override_sentiment_label,
          override_relevant, override_control_class, note, edited_by, edited_at
        )
        select
          %s::uuid, article_id, override_sentiment_score, override_sentiment_label,
          override_relevant, override_control_class, note, edited_by, edited_at
        from ceo_article_overrides
        where ceo_id = %s::uuid
        on conflict (ceo_id, article_id) do nothing
        """,
        (target_ceo.id, source_ceo.id),
    )
    cur.execute("delete from ceo_article_overrides where ceo_id = %s::uuid", (source_ceo.id,))


def merge_ceo_feature_items(cur, source_ceo: CeoRow, target_ceo: CeoRow) -> None:
    cur.execute("drop table if exists tmp_ceo_feature_item_map")
    cur.execute(
        """
        create temp table tmp_ceo_feature_item_map on commit drop as
        select src.id as source_item_id,
               tgt.id as target_item_id
        from serp_feature_items src
        join serp_feature_items tgt
          on tgt.entity_type = 'ceo'
         and tgt.date = src.date
         and tgt.feature_type = src.feature_type
         and tgt.url_hash is not distinct from src.url_hash
         and (tgt.entity_id = %s::uuid or (tgt.entity_id is null and tgt.entity_name = %s))
         and tgt.id <> src.id
        where src.entity_type = 'ceo'
          and (src.entity_id = %s::uuid or (src.entity_id is null and src.entity_name = %s))
        """,
        (target_ceo.id, target_ceo.name, source_ceo.id, source_ceo.name),
    )
    cur.execute(
        """
        insert into serp_feature_item_overrides (
          serp_feature_item_id, override_sentiment_label, override_control_class,
          note, edited_by, edited_at, created_at
        )
        select
          map.target_item_id,
          ov.override_sentiment_label,
          ov.override_control_class,
          ov.note,
          ov.edited_by,
          ov.edited_at,
          ov.created_at
        from tmp_ceo_feature_item_map map
        join serp_feature_item_overrides ov
          on ov.serp_feature_item_id = map.source_item_id
        on conflict (serp_feature_item_id) do nothing
        """
    )
    cur.execute(
        """
        update serp_feature_items sfi
           set entity_id = %s::uuid,
               entity_name = %s,
               updated_at = now()
         where sfi.entity_type = 'ceo'
           and (sfi.entity_id = %s::uuid or (sfi.entity_id is null and sfi.entity_name = %s))
           and not exists (
             select 1
             from tmp_ceo_feature_item_map map
             where map.source_item_id = sfi.id
           )
        """,
        (target_ceo.id, target_ceo.name, source_ceo.id, source_ceo.name),
    )
    cur.execute(
        """
        delete from serp_feature_items
        where id in (select source_item_id from tmp_ceo_feature_item_map)
        """
    )


def merge_ceo_feature_summaries(cur, source_ceo: CeoRow, target_ceo: CeoRow) -> None:
    cur.execute(
        """
        insert into serp_feature_summaries (
          date, entity_type, entity_id, entity_name, feature_type, summary_text,
          provider, model, created_at, updated_at
        )
        select
          date, entity_type, %s::uuid, %s, feature_type, summary_text,
          provider, model, created_at, updated_at
        from serp_feature_summaries
        where entity_type = 'ceo'
          and entity_id = %s::uuid
        on conflict (date, entity_type, entity_id, feature_type) do nothing
        """,
        (target_ceo.id, target_ceo.name, source_ceo.id),
    )
    cur.execute(
        """
        delete from serp_feature_summaries
        where entity_type = 'ceo'
          and entity_id = %s::uuid
        """,
        (source_ceo.id,),
    )


def merge_ceo_runs(cur, source_ceo: CeoRow, target_ceo: CeoRow) -> None:
    cur.execute("drop table if exists tmp_ceo_run_map")
    cur.execute(
        """
        create temp table tmp_ceo_run_map on commit drop as
        select src.id as source_run_id,
               tgt.id as target_run_id
        from serp_runs src
        join serp_runs tgt
          on tgt.entity_type = 'ceo'
         and tgt.ceo_id = %s::uuid
         and tgt.run_at = src.run_at
        where src.entity_type = 'ceo'
          and src.ceo_id = %s::uuid
        """,
        (target_ceo.id, source_ceo.id),
    )

    cur.execute("drop table if exists tmp_ceo_result_map")
    cur.execute(
        """
        create temp table tmp_ceo_result_map on commit drop as
        select src.id as source_result_id,
               tgt.id as target_result_id
        from serp_results src
        join tmp_ceo_run_map map
          on map.source_run_id = src.serp_run_id
        join serp_results tgt
          on tgt.serp_run_id = map.target_run_id
         and tgt.rank is not distinct from src.rank
         and tgt.url_hash is not distinct from src.url_hash
        """
    )

    cur.execute(
        """
        insert into serp_result_overrides (
          serp_result_id, override_sentiment_label, override_control_class,
          note, edited_by, edited_at
        )
        select
          map.target_result_id,
          ov.override_sentiment_label,
          ov.override_control_class,
          ov.note,
          ov.edited_by,
          ov.edited_at
        from tmp_ceo_result_map map
        join serp_result_overrides ov
          on ov.serp_result_id = map.source_result_id
        on conflict (serp_result_id) do nothing
        """
    )

    cur.execute(
        """
        update serp_results sr
           set serp_run_id = map.target_run_id
          from tmp_ceo_run_map map
         where sr.serp_run_id = map.source_run_id
           and not exists (
             select 1
             from serp_results tgt
             where tgt.serp_run_id = map.target_run_id
               and tgt.rank is not distinct from sr.rank
               and tgt.url_hash is not distinct from sr.url_hash
           )
        """
    )

    cur.execute(
        """
        delete from serp_results sr
        using tmp_ceo_run_map map
        where sr.serp_run_id = map.source_run_id
        """
    )
    cur.execute(
        """
        delete from serp_runs
        where id in (select source_run_id from tmp_ceo_run_map)
        """
    )

    cur.execute(
        """
        update serp_runs
           set ceo_id = %s::uuid
         where entity_type = 'ceo'
           and ceo_id = %s::uuid
        """,
        (target_ceo.id, source_ceo.id),
    )


def collect_feature_window(cur, entity_types: Sequence[str], entity_id: str) -> tuple[date | None, date | None]:
    cur.execute(
        """
        select min(date), max(date)
        from serp_feature_items
        where entity_type = any(%s)
          and entity_id = %s::uuid
        """,
        (list(entity_types), entity_id),
    )
    return cur.fetchone()


def collect_crisis_window(
    cur,
    *,
    entity_kind: str,
    entity_id: str,
    feature_types: Sequence[str],
) -> tuple[date | None, date | None]:
    if entity_kind == "company":
        cur.execute(
            """
            select min(min_date), max(max_date)
            from (
              select min(date) as min_date, max(date) as max_date
              from company_article_mentions_daily
              where company_id = %s::uuid
              union all
              select min(date) as min_date, max(date) as max_date
              from serp_feature_items
              where entity_type = any(%s)
                and entity_id = %s::uuid
            ) windowed
            """,
            (entity_id, list(feature_types), entity_id),
        )
        return cur.fetchone()
    cur.execute(
        """
        select min(min_date), max(max_date)
        from (
          select min(date) as min_date, max(date) as max_date
          from ceo_article_mentions_daily
          where ceo_id = %s::uuid
          union all
          select min(date) as min_date, max(date) as max_date
          from serp_feature_items
          where entity_type = 'ceo'
            and entity_id = %s::uuid
        ) windowed
        """,
        (entity_id, entity_id),
    )
    return cur.fetchone()


def delete_named_entity_rows(
    cur,
    *,
    table: str,
    start_date: date,
    end_date: date,
    entity_types: Sequence[str],
    source_id: str,
    source_name: str,
    target_id: str,
    target_name: str,
) -> None:
    cur.execute(
        f"""
        delete from {table}
        where date between %s and %s
          and entity_type = any(%s)
          and (
            entity_id = %s::uuid
            or entity_id = %s::uuid
            or entity_name = %s
            or entity_name = %s
          )
        """,
        (start_date, end_date, list(entity_types), source_id, target_id, source_name, target_name),
    )


def rebuild_serp_feature_daily(
    cur,
    *,
    entity_types: Sequence[str],
    entity_id: str,
    start_date: date,
    end_date: date,
) -> int:
    cur.execute(
        """
        insert into serp_feature_daily (
          date, entity_type, entity_id, entity_name, feature_type,
          total_count, positive_count, neutral_count, negative_count, source
        )
        select
          sfi.date,
          sfi.entity_type,
          min(sfi.entity_id::text)::uuid as entity_id,
          sfi.entity_name,
          sfi.feature_type,
          count(*) as total_count,
          sum(case when coalesce(ov.override_sentiment_label, sfi.sentiment_label) = 'positive' then 1 else 0 end) as positive_count,
          sum(case when coalesce(ov.override_sentiment_label, sfi.sentiment_label) = 'neutral' then 1 else 0 end) as neutral_count,
          sum(case when coalesce(ov.override_sentiment_label, sfi.sentiment_label) = 'negative' then 1 else 0 end) as negative_count,
          'merge_recompute' as source
        from serp_feature_items sfi
        left join serp_feature_item_overrides ov
          on ov.serp_feature_item_id = sfi.id
        where sfi.date between %s and %s
          and sfi.entity_type = any(%s)
          and sfi.entity_id = %s::uuid
        group by sfi.date, sfi.entity_type, sfi.entity_name, sfi.feature_type
        on conflict (date, entity_type, entity_name, feature_type) do update set
          entity_id = excluded.entity_id,
          total_count = excluded.total_count,
          positive_count = excluded.positive_count,
          neutral_count = excluded.neutral_count,
          negative_count = excluded.negative_count,
          source = excluded.source,
          updated_at = now()
        """,
        (start_date, end_date, list(entity_types), entity_id),
    )
    return cur.rowcount


def rebuild_narrative_tags(
    cur,
    *,
    entity_types: Sequence[str],
    entity_id: str,
    start_date: date,
    end_date: date,
    narrative_min_negative_top_stories: int,
) -> int:
    ensure_entity_crisis_tag_daily_table(cur)
    cur.execute(
        """
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
        left join serp_feature_item_overrides ov
          on ov.serp_feature_item_id = sfi.id
        where sfi.date between %s and %s
          and sfi.feature_type = 'top_stories_items'
          and sfi.entity_type = any(%s)
          and sfi.entity_id = %s::uuid
        order by sfi.date, sfi.entity_type, sfi.entity_name, sfi.id
        """,
        (start_date, end_date, list(entity_types), entity_id),
    )
    rows = cur.fetchall()
    if not rows:
        return 0

    grouped: dict[tuple[date, str, str, str], list[dict[str, object]]] = defaultdict(list)
    for (
        item_id,
        row_date,
        row_entity_type,
        row_entity_id,
        row_entity_name,
        title,
        snippet,
        url,
        source,
        sentiment_label,
        finance_routine,
    ) in rows:
        grouped[(row_date, row_entity_type, str(row_entity_id), row_entity_name)].append(
            {
                "item_id": item_id,
                "title": title or "",
                "snippet": snippet or "",
                "url": url or "",
                "source": source or "",
                "sentiment_label": sentiment_label,
                "finance_routine": bool(finance_routine),
            }
        )

    now_ts = time.strftime("%Y-%m-%d %H:%M:%S+00")
    rollup_rows: list[tuple] = []
    item_updates: list[tuple] = []
    for (row_date, row_entity_type, row_entity_id, row_entity_name), items in grouped.items():
        rollup = rollup_entity_day_narrative(
            items,
            min_negative_top_stories=narrative_min_negative_top_stories,
        )
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
                now_ts if rollup.get("primary_tag") else None,
            )
        )
        for item, tag in zip(items, rollup.get("item_results") or []):
            item_updates.append(
                (
                    tag.get("primary_tag") or None,
                    tag.get("primary_group") or None,
                    tag.get("tags") or None,
                    tag.get("is_crisis"),
                    tag.get("rule_version") or None,
                    now_ts if tag.get("primary_tag") else None,
                    item["item_id"],
                )
            )

    upsert_entity_crisis_tag_daily(cur, rollup_rows)
    execute_batch(
        cur,
        """
        update serp_feature_items
           set narrative_primary_tag = %s,
               narrative_primary_group = %s,
               narrative_tags = %s,
               narrative_is_crisis = %s,
               narrative_rule_version = %s,
               narrative_tagged_at = %s,
               updated_at = now()
         where id = %s
        """,
        item_updates,
        page_size=500,
    )
    return len(rollup_rows)


def rebuild_company_derived_tables(
    cur,
    *,
    source: CompanyRow,
    target: CompanyRow,
    narrative_min_negative_top_stories: int,
) -> None:
    feature_start, feature_end = collect_feature_window(cur, ["brand", "company"], target.id)
    if feature_start and feature_end:
        log(
            "[STEP] rebuild company serp_feature_daily/narrative "
            f"window={feature_start}..{feature_end}"
        )
        delete_named_entity_rows(
            cur,
            table="serp_feature_daily",
            start_date=feature_start,
            end_date=feature_end,
            entity_types=["brand", "company"],
            source_id=source.id,
            source_name=source.name,
            target_id=target.id,
            target_name=target.name,
        )
        rebuild_serp_feature_daily(
            cur,
            entity_types=["brand", "company"],
            entity_id=target.id,
            start_date=feature_start,
            end_date=feature_end,
        )
        delete_named_entity_rows(
            cur,
            table="entity_crisis_tag_daily",
            start_date=feature_start,
            end_date=feature_end,
            entity_types=["brand", "company"],
            source_id=source.id,
            source_name=source.name,
            target_id=target.id,
            target_name=target.name,
        )
        rebuild_narrative_tags(
            cur,
            entity_types=["brand", "company"],
            entity_id=target.id,
            start_date=feature_start,
            end_date=feature_end,
            narrative_min_negative_top_stories=narrative_min_negative_top_stories,
        )

    crisis_start, crisis_end = collect_crisis_window(
        cur,
        entity_kind="company",
        entity_id=target.id,
        feature_types=["brand", "company"],
    )
    if crisis_start and crisis_end:
        log(f"[STEP] rebuild company crisis events window={crisis_start}..{crisis_end}")
        delete_named_entity_rows(
            cur,
            table="entity_crisis_event_daily",
            start_date=crisis_start,
            end_date=crisis_end,
            entity_types=["brand"],
            source_id=source.id,
            source_name=source.name,
            target_id=target.id,
            target_name=target.name,
        )
        ensure_entity_crisis_event_daily_table(cur)
        recompute_entity_crisis_event_window(
            cur,
            crisis_start,
            crisis_end,
            ["brand"],
            entity_id=target.id,
            narrative_min_negative_top_stories=narrative_min_negative_top_stories,
        )


def rebuild_ceo_derived_tables(
    cur,
    *,
    source_ceo: CeoRow,
    target_ceo: CeoRow,
    narrative_min_negative_top_stories: int,
) -> None:
    feature_start, feature_end = collect_feature_window(cur, ["ceo"], target_ceo.id)
    if feature_start and feature_end:
        log(
            "[STEP] rebuild ceo serp_feature_daily/narrative "
            f"ceo={target_ceo.name} window={feature_start}..{feature_end}"
        )
        delete_named_entity_rows(
            cur,
            table="serp_feature_daily",
            start_date=feature_start,
            end_date=feature_end,
            entity_types=["ceo"],
            source_id=source_ceo.id,
            source_name=source_ceo.name,
            target_id=target_ceo.id,
            target_name=target_ceo.name,
        )
        rebuild_serp_feature_daily(
            cur,
            entity_types=["ceo"],
            entity_id=target_ceo.id,
            start_date=feature_start,
            end_date=feature_end,
        )
        delete_named_entity_rows(
            cur,
            table="entity_crisis_tag_daily",
            start_date=feature_start,
            end_date=feature_end,
            entity_types=["ceo"],
            source_id=source_ceo.id,
            source_name=source_ceo.name,
            target_id=target_ceo.id,
            target_name=target_ceo.name,
        )
        rebuild_narrative_tags(
            cur,
            entity_types=["ceo"],
            entity_id=target_ceo.id,
            start_date=feature_start,
            end_date=feature_end,
            narrative_min_negative_top_stories=narrative_min_negative_top_stories,
        )

    crisis_start, crisis_end = collect_crisis_window(
        cur,
        entity_kind="ceo",
        entity_id=target_ceo.id,
        feature_types=["ceo"],
    )
    if crisis_start and crisis_end:
        log(
            "[STEP] rebuild ceo crisis events "
            f"ceo={target_ceo.name} window={crisis_start}..{crisis_end}"
        )
        delete_named_entity_rows(
            cur,
            table="entity_crisis_event_daily",
            start_date=crisis_start,
            end_date=crisis_end,
            entity_types=["ceo"],
            source_id=source_ceo.id,
            source_name=source_ceo.name,
            target_id=target_ceo.id,
            target_name=target_ceo.name,
        )
        ensure_entity_crisis_event_daily_table(cur)
        recompute_entity_crisis_event_window(
            cur,
            crisis_start,
            crisis_end,
            ["ceo"],
            entity_id=target_ceo.id,
            narrative_min_negative_top_stories=narrative_min_negative_top_stories,
        )


def merge_duplicate_ceo(
    cur,
    *,
    source_ceo: CeoRow,
    target_ceo: CeoRow,
    target_company: CompanyRow,
    rebuild_derived: bool,
    narrative_min_negative_top_stories: int,
) -> None:
    log(f"[STEP] merge duplicate CEO {source_ceo.name}")
    merge_ceo_article_tables(cur, source_ceo, target_ceo)
    merge_ceo_feature_items(cur, source_ceo, target_ceo)
    merge_ceo_feature_summaries(cur, source_ceo, target_ceo)
    merge_ceo_runs(cur, source_ceo, target_ceo)

    cur.execute(
        """
        insert into boards (
          ceo_id, company_id, url, domain, source, last_updated, created_at
        )
        select
          %s::uuid, %s::uuid, url, domain, source, last_updated, created_at
        from boards
        where ceo_id = %s::uuid
        on conflict (ceo_id, url) do nothing
        """,
        (target_ceo.id, target_company.id, source_ceo.id),
    )
    cur.execute("delete from boards where ceo_id = %s::uuid", (source_ceo.id,))

    cur.execute(
        """
        update ceos tgt
           set alias = case
                 when coalesce(nullif(tgt.alias, ''), '') = '' then src.alias
                 else tgt.alias
               end,
               favorite = tgt.favorite or src.favorite
          from ceos src
         where tgt.id = %s::uuid
           and src.id = %s::uuid
        """,
        (target_ceo.id, source_ceo.id),
    )

    if rebuild_derived:
        rebuild_ceo_derived_tables(
            cur,
            source_ceo=source_ceo,
            target_ceo=target_ceo,
            narrative_min_negative_top_stories=narrative_min_negative_top_stories,
        )

    cur.execute("delete from ceos where id = %s::uuid", (source_ceo.id,))


def move_unique_ceo_to_target_company(cur, ceo: CeoRow, target_company: CompanyRow) -> None:
    log(f"[STEP] move CEO to target company {ceo.name}")
    cur.execute(
        """
        update ceos
           set company_id = %s::uuid
         where id = %s::uuid
        """,
        (target_company.id, ceo.id),
    )


def merge_company_metadata(cur, source: CompanyRow, target: CompanyRow) -> None:
    cur.execute(
        """
        update companies tgt
           set ticker = coalesce(nullif(tgt.ticker, ''), nullif(src.ticker, '')),
               sector = coalesce(nullif(tgt.sector, ''), nullif(src.sector, '')),
               websites = coalesce(nullif(tgt.websites, ''), nullif(src.websites, '')),
               favorite = tgt.favorite or src.favorite
          from companies src
         where tgt.id = %s::uuid
           and src.id = %s::uuid
        """,
        (target.id, source.id),
    )


def merge_boards_company_id(cur, source: CompanyRow, target: CompanyRow) -> None:
    cur.execute(
        """
        update boards
           set company_id = %s::uuid
         where company_id = %s::uuid
        """,
        (target.id, source.id),
    )


def execute_merge(
    conn,
    *,
    source: CompanyRow,
    target: CompanyRow,
    rebuild_derived: bool,
    narrative_min_negative_top_stories: int,
) -> None:
    with conn.cursor() as cur:
        ensure_handled_foreign_keys(cur)
        cur.execute("select pg_advisory_lock(%s)", (MERGE_LOCK_KEY,))
        log(f"[STEP] advisory lock acquired key={MERGE_LOCK_KEY}")

        merge_text_company_tables(cur, source, target)
        merge_company_article_tables(cur, source, target)
        merge_company_feature_items(cur, source, target)
        merge_company_feature_summaries(cur, source, target)
        merge_company_runs(cur, source, target)

        source_ceos = list_company_ceos(cur, source.id)
        target_ceos = list_company_ceos(cur, target.id)
        target_by_name = {ceo.name.casefold(): ceo for ceo in target_ceos}
        for source_ceo in source_ceos:
            target_ceo = target_by_name.get(source_ceo.name.casefold())
            if target_ceo:
                merge_duplicate_ceo(
                    cur,
                    source_ceo=source_ceo,
                    target_ceo=target_ceo,
                    target_company=target,
                    rebuild_derived=rebuild_derived,
                    narrative_min_negative_top_stories=narrative_min_negative_top_stories,
                )
            else:
                move_unique_ceo_to_target_company(cur, source_ceo, target)

        merge_boards_company_id(cur, source, target)
        merge_company_metadata(cur, source, target)

        if rebuild_derived:
            rebuild_company_derived_tables(
                cur,
                source=source,
                target=target,
                narrative_min_negative_top_stories=narrative_min_negative_top_stories,
            )

        cur.execute("delete from companies where id = %s::uuid", (source.id,))
        cur.execute("select pg_advisory_unlock(%s)", (MERGE_LOCK_KEY,))
        log("[STEP] source company deleted")


def refresh_materialized_views(dsn: str) -> None:
    refresh_conn = psycopg2.connect(dsn, application_name="merge_company_records_refresh")
    got_lock = False
    try:
        refresh_conn.autocommit = True
        with refresh_conn.cursor() as cur:
            cur.execute("select pg_try_advisory_lock(%s)", (REFRESH_LOCK_KEY,))
            got_lock = bool(cur.fetchone()[0])
        if not got_lock:
            log("[WARN] refresh skipped because another refresh lock is active")
            return
        with refresh_conn.cursor() as cur:
            for name in (
                "serp_feature_daily_mv",
                "serp_feature_control_daily_mv",
                "serp_feature_daily_index_mv",
                "serp_feature_control_daily_index_mv",
                "article_daily_counts_mv",
                "serp_daily_counts_mv",
            ):
                start = time.perf_counter()
                mode = "concurrent"
                try:
                    cur.execute(f"refresh materialized view concurrently {name}")
                except Exception:
                    mode = "plain"
                    cur.execute(f"refresh materialized view {name}")
                elapsed = time.perf_counter() - start
                log(f"[STEP] refreshed {name} mode={mode} elapsed={elapsed:.2f}s")
    finally:
        if got_lock:
            try:
                with refresh_conn.cursor() as cur:
                    cur.execute("select pg_advisory_unlock(%s)", (REFRESH_LOCK_KEY,))
            except Exception:
                pass
        refresh_conn.close()


def main() -> int:
    args = parse_args()
    dsn = args.dsn or os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    with get_conn(dsn, "merge_company_records_preview") as conn:
        with conn.cursor() as cur:
            source = resolve_company(
                cur,
                row_id=args.source_id,
                name=args.source_name,
                label="source",
            )
            target = resolve_company(
                cur,
                row_id=args.target_id,
                name=args.target_name,
                label="target",
            )
            if source.id == target.id:
                raise SystemExit("Source and target resolve to the same company")
            print_preview(cur, source, target)

        if not args.execute:
            log("[DRY RUN] No changes committed. Re-run with --execute to apply.")
            return 0

    with get_conn(dsn, "merge_company_records") as conn:
        try:
            execute_merge(
                conn,
                source=source,
                target=target,
                rebuild_derived=not args.skip_derived_rebuilds,
                narrative_min_negative_top_stories=max(
                    1, int(args.narrative_min_negative_top_stories or 1)
                ),
            )
            conn.commit()
            log("[OK] merge committed")
        except Exception:
            conn.rollback()
            raise

    if not args.skip_refresh:
        refresh_materialized_views(dsn)

    with get_conn(dsn, "merge_company_records_verify") as conn:
        with conn.cursor() as cur:
            target_after = resolve_company(
                cur,
                row_id=target.id,
                name=target.name,
                label="target",
            )
            counts = fetch_company_counts(cur, target_after)
            summary = " ".join(f"{key}={value}" for key, value in counts.items())
            log(f"[VERIFY] {target_after.name} {target_after.id} {summary}")
            cur.execute("select count(*) from companies where id = %s::uuid", (source.id,))
            remaining = int(cur.fetchone()[0])
            log(f"[VERIFY] source_company_remaining={remaining}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
