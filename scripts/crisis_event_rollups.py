#!/usr/bin/env python3
from __future__ import annotations

import os
from datetime import date as date_cls, datetime, timedelta
from decimal import Decimal
from typing import Iterable

from psycopg2.extras import Json, execute_values

from risk_rules import (
    NARRATIVE_MIN_NEG_TOP_STORIES,
    rollup_crisis_event_items,
)

CRISIS_EVENT_RULE_VERSION = "event_v1"
CRISIS_EVENT_NEWSFEED_DELTA_PCT = Decimal(
    str(os.getenv("CRISIS_EVENT_NEWSFEED_DELTA_PCT", "0.20") or "0.20")
)
CRISIS_EVENT_MIN_ARTICLE_TOTAL = max(1, int(os.getenv("CRISIS_EVENT_MIN_ARTICLE_TOTAL", "5") or 5))
CRISIS_EVENT_MIN_NEGATIVE_COUNT_DELTA = max(
    1,
    int(os.getenv("CRISIS_EVENT_MIN_NEGATIVE_COUNT_DELTA", "2") or 2),
)
CRISIS_EVENT_CONTINUATION_MIN_RECENT_NEGATIVE_ARTICLES = max(
    1,
    int(os.getenv("CRISIS_EVENT_CONTINUATION_MIN_RECENT_NEGATIVE_ARTICLES", "2") or 2),
)
CRISIS_EVENT_CONTINUATION_MIN_NEGATIVE_PCT = Decimal(
    str(os.getenv("CRISIS_EVENT_CONTINUATION_MIN_NEGATIVE_PCT", "0.50") or "0.50")
)


def canonical_entity_type(entity_type: str) -> str:
    return "brand" if str(entity_type or "").strip().lower() in {"brand", "company"} else "ceo"


def normalize_entity_types(entity_types: Iterable[str]) -> list[str]:
    normalized = []
    seen = set()
    for entity_type in entity_types or []:
        value = canonical_entity_type(entity_type)
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _coerce_event_date(value, *, field_name: str) -> date_cls:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date_cls):
        return value
    if isinstance(value, str):
        try:
            return date_cls.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an ISO date string, got {value!r}") from exc
    raise TypeError(f"{field_name} must be a date, datetime, or ISO date string, got {type(value).__name__}")


def ensure_entity_crisis_event_daily_table(cur) -> None:
    cur.execute(
        """
        create table if not exists entity_crisis_event_daily (
          date date not null,
          entity_type text not null,
          entity_id uuid,
          entity_name text not null,
          primary_tag text,
          primary_group text,
          tags text[],
          is_crisis boolean,
          trigger_sources text[] not null default '{}'::text[],
          trigger_top_stories boolean not null default false,
          trigger_newsfeed_delta boolean not null default false,
          trigger_continued_coverage boolean not null default false,
          negative_top_stories_count int not null default 0,
          recent_negative_article_count int not null default 0,
          article_negative_count int not null default 0,
          article_total_count int not null default 0,
          article_negative_pct numeric not null default 0,
          prior_article_negative_count int not null default 0,
          prior_article_total_count int not null default 0,
          prior_article_negative_pct numeric not null default 0,
          article_negative_pct_delta numeric not null default 0,
          negative_evidence_count int not null default 0,
          tagged_item_count int not null default 0,
          unmatched_negative_items int not null default 0,
          supporting_negative_items int not null default 0,
          tag_counts jsonb not null default '{}'::jsonb,
          narrative_rule_version text,
          crisis_event_rule_version text,
          tagged_at timestamptz,
          created_at timestamptz not null default now(),
          updated_at timestamptz not null default now()
        )
        """
    )
    cur.execute(
        """
        create unique index if not exists entity_crisis_event_daily_unique_idx
          on entity_crisis_event_daily (date, entity_type, entity_name)
        """
    )
    cur.execute(
        """
        create index if not exists entity_crisis_event_daily_entity_id_idx
          on entity_crisis_event_daily (entity_type, entity_id, date)
        """
    )
    cur.execute(
        """
        create index if not exists entity_crisis_event_daily_tag_idx
          on entity_crisis_event_daily (primary_tag, date)
        """
    )


def delete_entity_crisis_event_daily_window(
    cur,
    start_date,
    end_date,
    entity_types: Iterable[str],
    *,
    entity_id=None,
) -> None:
    types = normalize_entity_types(entity_types)
    if not types:
        return
    if entity_id is None:
        cur.execute(
            """
            delete from entity_crisis_event_daily
             where date between %s and %s
               and entity_type = any(%s)
            """,
            (start_date, end_date, types),
        )
        return
    cur.execute(
        """
        delete from entity_crisis_event_daily
         where date between %s and %s
           and entity_type = any(%s)
           and entity_id = %s
        """,
        (start_date, end_date, types, entity_id),
    )


def upsert_entity_crisis_event_daily(cur, rows: list[tuple]) -> int:
    if not rows:
        return 0
    sql = """
        insert into entity_crisis_event_daily (
          date,
          entity_type,
          entity_id,
          entity_name,
          primary_tag,
          primary_group,
          tags,
          is_crisis,
          trigger_sources,
          trigger_top_stories,
          trigger_newsfeed_delta,
          trigger_continued_coverage,
          negative_top_stories_count,
          recent_negative_article_count,
          article_negative_count,
          article_total_count,
          article_negative_pct,
          prior_article_negative_count,
          prior_article_total_count,
          prior_article_negative_pct,
          article_negative_pct_delta,
          negative_evidence_count,
          tagged_item_count,
          unmatched_negative_items,
          supporting_negative_items,
          tag_counts,
          narrative_rule_version,
          crisis_event_rule_version,
          tagged_at
        )
        values %s
        on conflict (date, entity_type, entity_name) do update set
          entity_id = excluded.entity_id,
          primary_tag = excluded.primary_tag,
          primary_group = excluded.primary_group,
          tags = excluded.tags,
          is_crisis = excluded.is_crisis,
          trigger_sources = excluded.trigger_sources,
          trigger_top_stories = excluded.trigger_top_stories,
          trigger_newsfeed_delta = excluded.trigger_newsfeed_delta,
          trigger_continued_coverage = excluded.trigger_continued_coverage,
          negative_top_stories_count = excluded.negative_top_stories_count,
          recent_negative_article_count = excluded.recent_negative_article_count,
          article_negative_count = excluded.article_negative_count,
          article_total_count = excluded.article_total_count,
          article_negative_pct = excluded.article_negative_pct,
          prior_article_negative_count = excluded.prior_article_negative_count,
          prior_article_total_count = excluded.prior_article_total_count,
          prior_article_negative_pct = excluded.prior_article_negative_pct,
          article_negative_pct_delta = excluded.article_negative_pct_delta,
          negative_evidence_count = excluded.negative_evidence_count,
          tagged_item_count = excluded.tagged_item_count,
          unmatched_negative_items = excluded.unmatched_negative_items,
          supporting_negative_items = excluded.supporting_negative_items,
          tag_counts = excluded.tag_counts,
          narrative_rule_version = excluded.narrative_rule_version,
          crisis_event_rule_version = excluded.crisis_event_rule_version,
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
            row[12],
            row[13],
            row[14],
            row[15],
            row[16],
            row[17],
            row[18],
            row[19],
            row[20],
            row[21],
            row[22],
            row[23],
            row[24],
            Json(row[25] or {}),
            row[26],
            row[27],
            row[28],
        )
        for row in rows
    ]
    execute_values(cur, sql, normalized_rows, page_size=1000)
    return len(rows)


def _decimal_or_zero(value) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _int_or_zero(value) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _dedupe_event_items(items: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for item in items:
        url = str(item.get("url") or "").strip().casefold()
        title = str(item.get("title") or "").strip().casefold()
        source = str(item.get("source") or "").strip().casefold()
        key = url or f"{title}::{source}"
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _load_article_metrics(cur, start_date, end_date, entity_types: list[str], entity_id=None) -> dict:
    metrics: dict[tuple[date_cls, str, object], dict] = {}
    if "brand" in entity_types:
        params = [start_date, end_date]
        entity_sql = ""
        if entity_id is not None:
            params.append(entity_id)
            entity_sql = " and c.id = %s"
        cur.execute(
            f"""
            select cad.date,
                   'brand'::text as entity_type,
                   c.id as entity_id,
                   c.name as entity_name,
                   sum(case when coalesce(ov.override_sentiment_label, cad.sentiment_label) = 'negative' then 1 else 0 end) as article_negative_count,
                   count(*) as article_total_count,
                   case when count(*) > 0
                        then sum(case when coalesce(ov.override_sentiment_label, cad.sentiment_label) = 'negative' then 1 else 0 end)::numeric / count(*)
                        else 0::numeric
                   end as article_negative_pct
            from company_article_mentions_daily cad
            join companies c on c.id = cad.company_id
            left join company_article_overrides ov
              on ov.company_id = cad.company_id
             and ov.article_id = cad.article_id
            where cad.date between %s and %s
              {entity_sql}
            group by cad.date, c.id, c.name
            """,
            tuple(params),
        )
        for row_date, row_entity_type, row_entity_id, entity_name, neg_count, total_count, neg_pct in cur.fetchall():
            metrics[(row_date, row_entity_type, row_entity_id)] = {
                "entity_name": entity_name or "",
                "article_negative_count": _int_or_zero(neg_count),
                "article_total_count": _int_or_zero(total_count),
                "article_negative_pct": _decimal_or_zero(neg_pct),
            }
    if "ceo" in entity_types:
        params = [start_date, end_date]
        entity_sql = ""
        if entity_id is not None:
            params.append(entity_id)
            entity_sql = " and ceo.id = %s"
        cur.execute(
            f"""
            select cad.date,
                   'ceo'::text as entity_type,
                   ceo.id as entity_id,
                   ceo.name as entity_name,
                   sum(case when coalesce(ov.override_sentiment_label, cad.sentiment_label) = 'negative' then 1 else 0 end) as article_negative_count,
                   count(*) as article_total_count,
                   case when count(*) > 0
                        then sum(case when coalesce(ov.override_sentiment_label, cad.sentiment_label) = 'negative' then 1 else 0 end)::numeric / count(*)
                        else 0::numeric
                   end as article_negative_pct
            from ceo_article_mentions_daily cad
            join ceos ceo on ceo.id = cad.ceo_id
            left join ceo_article_overrides ov
              on ov.ceo_id = cad.ceo_id
             and ov.article_id = cad.article_id
            where cad.date between %s and %s
              {entity_sql}
            group by cad.date, ceo.id, ceo.name
            """,
            tuple(params),
        )
        for row_date, row_entity_type, row_entity_id, entity_name, neg_count, total_count, neg_pct in cur.fetchall():
            metrics[(row_date, row_entity_type, row_entity_id)] = {
                "entity_name": entity_name or "",
                "article_negative_count": _int_or_zero(neg_count),
                "article_total_count": _int_or_zero(total_count),
                "article_negative_pct": _decimal_or_zero(neg_pct),
            }
    return metrics


def _load_top_stories_items(cur, start_date, end_date, entity_types: list[str], entity_id=None) -> dict:
    items: dict[tuple[date_cls, str, object], list[dict]] = {}
    if "brand" in entity_types:
        params = [start_date, end_date, ["brand", "company"]]
        entity_sql = ""
        if entity_id is not None:
            params.append(entity_id)
            entity_sql = " and c.id = %s"
        cur.execute(
            f"""
            select sfi.date,
                   'brand'::text as entity_type,
                   c.id as entity_id,
                   c.name as entity_name,
                   sfi.title,
                   sfi.snippet,
                   sfi.url,
                   sfi.source,
                   coalesce(ov.override_sentiment_label, sfi.llm_sentiment_label, sfi.sentiment_label) as sentiment_label,
                   coalesce(sfi.finance_routine, false) as finance_routine
            from serp_feature_items sfi
            join companies c on c.id = sfi.entity_id
            left join serp_feature_item_overrides ov on ov.serp_feature_item_id = sfi.id
            where sfi.date between %s and %s
              and sfi.entity_type = any(%s)
              and sfi.feature_type = 'top_stories_items'
              and coalesce(ov.override_sentiment_label, sfi.llm_sentiment_label, sfi.sentiment_label) = 'negative'
              and coalesce(sfi.finance_routine, false) = false
              {entity_sql}
            order by sfi.date, c.name, sfi.position nulls last, sfi.id
            """,
            tuple(params),
        )
        for row_date, row_entity_type, row_entity_id, entity_name, title, snippet, url, source, sentiment_label, finance_routine in cur.fetchall():
            key = (row_date, row_entity_type, row_entity_id)
            items.setdefault(key, []).append({
                "entity_name": entity_name or "",
                "title": title or "",
                "snippet": snippet or "",
                "url": url or "",
                "source": source or "",
                "sentiment_label": sentiment_label,
                "finance_routine": bool(finance_routine),
                "evidence_source": "top_stories",
            })
    if "ceo" in entity_types:
        params = [start_date, end_date]
        entity_sql = ""
        if entity_id is not None:
            params.append(entity_id)
            entity_sql = " and ceo.id = %s"
        cur.execute(
            f"""
            select sfi.date,
                   'ceo'::text as entity_type,
                   ceo.id as entity_id,
                   ceo.name as entity_name,
                   sfi.title,
                   sfi.snippet,
                   sfi.url,
                   sfi.source,
                   coalesce(ov.override_sentiment_label, sfi.llm_sentiment_label, sfi.sentiment_label) as sentiment_label,
                   coalesce(sfi.finance_routine, false) as finance_routine
            from serp_feature_items sfi
            join ceos ceo on ceo.id = sfi.entity_id
            left join serp_feature_item_overrides ov on ov.serp_feature_item_id = sfi.id
            where sfi.date between %s and %s
              and sfi.entity_type = 'ceo'
              and sfi.feature_type = 'top_stories_items'
              and coalesce(ov.override_sentiment_label, sfi.llm_sentiment_label, sfi.sentiment_label) = 'negative'
              and coalesce(sfi.finance_routine, false) = false
              {entity_sql}
            order by sfi.date, ceo.name, sfi.position nulls last, sfi.id
            """,
            tuple(params),
        )
        for row_date, row_entity_type, row_entity_id, entity_name, title, snippet, url, source, sentiment_label, finance_routine in cur.fetchall():
            key = (row_date, row_entity_type, row_entity_id)
            items.setdefault(key, []).append({
                "entity_name": entity_name or "",
                "title": title or "",
                "snippet": snippet or "",
                "url": url or "",
                "source": source or "",
                "sentiment_label": sentiment_label,
                "finance_routine": bool(finance_routine),
                "evidence_source": "top_stories",
            })
    return items


def _load_recent_negative_articles(cur, start_date, end_date, entity_types: list[str], entity_id=None) -> dict:
    items: dict[tuple[date_cls, str, object], list[dict]] = {}
    if "brand" in entity_types:
        params = [start_date, end_date]
        entity_sql = ""
        if entity_id is not None:
            params.append(entity_id)
            entity_sql = " and c.id = %s"
        cur.execute(
            f"""
            select cad.date,
                   'brand'::text as entity_type,
                   c.id as entity_id,
                   c.name as entity_name,
                   a.title,
                   a.snippet,
                   a.canonical_url as url,
                   a.publisher as source,
                   coalesce(ov.override_sentiment_label, cad.sentiment_label) as sentiment_label,
                   coalesce(cad.finance_routine, false) as finance_routine
            from company_article_mentions_daily cad
            join companies c on c.id = cad.company_id
            join articles a on a.id = cad.article_id
            left join company_article_overrides ov
              on ov.company_id = cad.company_id
             and ov.article_id = cad.article_id
            where cad.date between %s and %s
              and coalesce(ov.override_sentiment_label, cad.sentiment_label) = 'negative'
              and coalesce(cad.finance_routine, false) = false
              and a.published_at is not null
              and (a.published_at at time zone 'UTC')::date between (cad.date - interval '1 day')::date and cad.date
              {entity_sql}
            order by cad.date, c.name, a.published_at desc nulls last, a.id
            """,
            tuple(params),
        )
        for row_date, row_entity_type, row_entity_id, entity_name, title, snippet, url, source, sentiment_label, finance_routine in cur.fetchall():
            key = (row_date, row_entity_type, row_entity_id)
            items.setdefault(key, []).append({
                "entity_name": entity_name or "",
                "title": title or "",
                "snippet": snippet or "",
                "url": url or "",
                "source": source or "",
                "sentiment_label": sentiment_label,
                "finance_routine": bool(finance_routine),
                "evidence_source": "newsfeed",
            })
    if "ceo" in entity_types:
        params = [start_date, end_date]
        entity_sql = ""
        if entity_id is not None:
            params.append(entity_id)
            entity_sql = " and ceo.id = %s"
        cur.execute(
            f"""
            select cad.date,
                   'ceo'::text as entity_type,
                   ceo.id as entity_id,
                   ceo.name as entity_name,
                   a.title,
                   a.snippet,
                   a.canonical_url as url,
                   a.publisher as source,
                   coalesce(ov.override_sentiment_label, cad.sentiment_label) as sentiment_label,
                   coalesce(cad.finance_routine, false) as finance_routine
            from ceo_article_mentions_daily cad
            join ceos ceo on ceo.id = cad.ceo_id
            join articles a on a.id = cad.article_id
            left join ceo_article_overrides ov
              on ov.ceo_id = cad.ceo_id
             and ov.article_id = cad.article_id
            where cad.date between %s and %s
              and coalesce(ov.override_sentiment_label, cad.sentiment_label) = 'negative'
              and coalesce(cad.finance_routine, false) = false
              and a.published_at is not null
              and (a.published_at at time zone 'UTC')::date between (cad.date - interval '1 day')::date and cad.date
              {entity_sql}
            order by cad.date, ceo.name, a.published_at desc nulls last, a.id
            """,
            tuple(params),
        )
        for row_date, row_entity_type, row_entity_id, entity_name, title, snippet, url, source, sentiment_label, finance_routine in cur.fetchall():
            key = (row_date, row_entity_type, row_entity_id)
            items.setdefault(key, []).append({
                "entity_name": entity_name or "",
                "title": title or "",
                "snippet": snippet or "",
                "url": url or "",
                "source": source or "",
                "sentiment_label": sentiment_label,
                "finance_routine": bool(finance_routine),
                "evidence_source": "newsfeed",
            })
    return items


def _load_prior_event_state(cur, event_date, entity_types: list[str], entity_id=None) -> dict:
    state = {}
    params = [event_date, entity_types]
    entity_sql = ""
    if entity_id is not None:
        params.append(entity_id)
        entity_sql = " and entity_id = %s"
    cur.execute(
        f"""
        select entity_type, entity_id
        from entity_crisis_event_daily
        where date = %s
          and entity_type = any(%s)
          {entity_sql}
          and primary_tag is not null
        """,
        tuple(params),
    )
    for row_entity_type, row_entity_id in cur.fetchall():
        state[(row_entity_type, row_entity_id)] = True
    return state


def build_entity_crisis_event_rows(
    cur,
    start_date,
    end_date,
    entity_types: Iterable[str],
    *,
    entity_id=None,
    narrative_min_negative_top_stories: int = NARRATIVE_MIN_NEG_TOP_STORIES,
    newsfeed_delta_pct: Decimal = CRISIS_EVENT_NEWSFEED_DELTA_PCT,
    min_article_total: int = CRISIS_EVENT_MIN_ARTICLE_TOTAL,
    min_negative_count_delta: int = CRISIS_EVENT_MIN_NEGATIVE_COUNT_DELTA,
    continuation_min_recent_negative_articles: int = CRISIS_EVENT_CONTINUATION_MIN_RECENT_NEGATIVE_ARTICLES,
    continuation_min_negative_pct: Decimal = CRISIS_EVENT_CONTINUATION_MIN_NEGATIVE_PCT,
) -> list[tuple]:
    start_date = _coerce_event_date(start_date, field_name="start_date")
    end_date = _coerce_event_date(end_date, field_name="end_date")
    canonical_types = normalize_entity_types(entity_types)
    if not canonical_types:
        return []

    metrics = _load_article_metrics(
        cur,
        start_date - timedelta(days=1),
        end_date,
        canonical_types,
        entity_id=entity_id,
    )
    top_story_items = _load_top_stories_items(
        cur,
        start_date,
        end_date,
        canonical_types,
        entity_id=entity_id,
    )
    recent_article_items = _load_recent_negative_articles(
        cur,
        start_date,
        end_date,
        canonical_types,
        entity_id=entity_id,
    )
    prior_state = _load_prior_event_state(
        cur,
        start_date - timedelta(days=1),
        canonical_types,
        entity_id=entity_id,
    )

    keys: dict[tuple[str, object], dict] = {}
    for (row_date, row_entity_type, row_entity_id), value in metrics.items():
        if row_date < start_date:
            continue
        bucket = keys.setdefault(
            (row_entity_type, row_entity_id),
            {"entity_name": value.get("entity_name") or "", "dates": set()},
        )
        if value.get("entity_name"):
            bucket["entity_name"] = value["entity_name"]
        bucket["dates"].add(row_date)
    for source_map in (top_story_items, recent_article_items):
        for (row_date, row_entity_type, row_entity_id), values in source_map.items():
            bucket = keys.setdefault(
                (row_entity_type, row_entity_id),
                {"entity_name": "", "dates": set()},
            )
            if values and values[0].get("entity_name"):
                bucket["entity_name"] = values[0]["entity_name"]
            bucket["dates"].add(row_date)

    rows: list[tuple] = []
    for (row_entity_type, row_entity_id), bucket in sorted(
        keys.items(),
        key=lambda item: (item[0][0], str(item[1].get("entity_name") or "").casefold()),
    ):
        entity_name = bucket.get("entity_name") or ""
        prev_active = bool(prior_state.get((row_entity_type, row_entity_id)))
        prev_processed_date = start_date - timedelta(days=1)
        for row_date in sorted(bucket.get("dates") or set()):
            if row_date < start_date or row_date > end_date:
                continue
            if row_date != prev_processed_date + timedelta(days=1):
                prev_active = False

            metric = metrics.get((row_date, row_entity_type, row_entity_id), {})
            prior_metric = metrics.get((row_date - timedelta(days=1), row_entity_type, row_entity_id), {})
            day_top_stories = top_story_items.get((row_date, row_entity_type, row_entity_id), [])
            day_articles = recent_article_items.get((row_date, row_entity_type, row_entity_id), [])
            evidence_items = _dedupe_event_items(day_top_stories + day_articles)
            rollup = rollup_crisis_event_items(evidence_items)

            article_negative_count = _int_or_zero(metric.get("article_negative_count"))
            article_total_count = _int_or_zero(metric.get("article_total_count"))
            article_negative_pct = _decimal_or_zero(metric.get("article_negative_pct"))
            prior_article_negative_count = _int_or_zero(prior_metric.get("article_negative_count"))
            prior_article_total_count = _int_or_zero(prior_metric.get("article_total_count"))
            prior_article_negative_pct = _decimal_or_zero(prior_metric.get("article_negative_pct"))
            article_negative_pct_delta = article_negative_pct - prior_article_negative_pct
            negative_count_delta = article_negative_count - prior_article_negative_count

            negative_top_stories_count = len(day_top_stories)
            recent_negative_article_count = len(day_articles)

            trigger_sources: list[str] = []
            if negative_top_stories_count >= max(1, int(narrative_min_negative_top_stories or 1)):
                trigger_sources.append("top_stories")
            if (
                article_total_count >= max(1, int(min_article_total or 1))
                and article_negative_pct_delta >= _decimal_or_zero(newsfeed_delta_pct)
                and negative_count_delta >= max(1, int(min_negative_count_delta or 1))
                and recent_negative_article_count >= max(1, int(continuation_min_recent_negative_articles or 1))
            ):
                trigger_sources.append("newsfeed_delta")
            if (
                prev_active
                and recent_negative_article_count >= max(1, int(continuation_min_recent_negative_articles or 1))
                and article_negative_pct >= _decimal_or_zero(continuation_min_negative_pct)
            ):
                trigger_sources.append("continued_coverage")

            is_active = bool(trigger_sources) and bool(rollup.get("primary_tag"))
            if is_active:
                now = datetime.utcnow()
                rows.append(
                    (
                        row_date,
                        row_entity_type,
                        row_entity_id,
                        entity_name,
                        rollup.get("primary_tag") or None,
                        rollup.get("primary_group") or None,
                        rollup.get("tags") or None,
                        rollup.get("is_crisis"),
                        trigger_sources,
                        "top_stories" in trigger_sources,
                        "newsfeed_delta" in trigger_sources,
                        "continued_coverage" in trigger_sources,
                        negative_top_stories_count,
                        recent_negative_article_count,
                        article_negative_count,
                        article_total_count,
                        article_negative_pct,
                        prior_article_negative_count,
                        prior_article_total_count,
                        prior_article_negative_pct,
                        article_negative_pct_delta,
                        int(rollup.get("negative_item_count") or 0),
                        int(rollup.get("tagged_item_count") or 0),
                        int(rollup.get("unmatched_negative_items") or 0),
                        int(rollup.get("supporting_negative_items") or 0),
                        rollup.get("tag_counts") or {},
                        rollup.get("rule_version") or None,
                        CRISIS_EVENT_RULE_VERSION,
                        now,
                    )
                )

            prev_active = is_active
            prev_processed_date = row_date
    return rows


def recompute_entity_crisis_event_window(
    cur,
    start_date,
    end_date,
    entity_types: Iterable[str],
    *,
    entity_id=None,
    narrative_min_negative_top_stories: int = NARRATIVE_MIN_NEG_TOP_STORIES,
) -> list[tuple]:
    start_date = _coerce_event_date(start_date, field_name="start_date")
    end_date = _coerce_event_date(end_date, field_name="end_date")
    ensure_entity_crisis_event_daily_table(cur)
    delete_entity_crisis_event_daily_window(
        cur,
        start_date,
        end_date,
        entity_types,
        entity_id=entity_id,
    )
    rows = build_entity_crisis_event_rows(
        cur,
        start_date,
        end_date,
        entity_types,
        entity_id=entity_id,
        narrative_min_negative_top_stories=narrative_min_negative_top_stories,
    )
    if rows:
        upsert_entity_crisis_event_daily(cur, rows)
    return rows
