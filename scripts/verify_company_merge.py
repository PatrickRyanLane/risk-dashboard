#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass

import psycopg2


@dataclass(frozen=True)
class CompanyRow:
    id: str
    name: str
    ticker: str | None
    sector: str | None
    websites: str | None
    favorite: bool


def log(message: str = "") -> None:
    print(message, flush=True)


def get_conn(dsn: str | None, app_name: str):
    resolved = dsn or os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not resolved:
        raise SystemExit("Set DATABASE_URL or SUPABASE_DB_URL, or pass --dsn")
    return psycopg2.connect(resolved, application_name=app_name)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify a company merge by checking company-linked rows, old-name remnants, and date coverage.",
    )
    parser.add_argument("--source-name", required=True, help="Original company name, e.g. 'Fanatics Inc'")
    parser.add_argument("--target-name", required=True, help="Surviving company name, e.g. 'Fanatics'")
    parser.add_argument("--source-id", help="Optional original company ID")
    parser.add_argument("--target-id", help="Optional surviving company ID")
    parser.add_argument("--article-date", help="Optional YYYY-MM-DD to inspect company article rows")
    parser.add_argument("--serp-date", help="Optional YYYY-MM-DD to inspect company SERP rows")
    parser.add_argument("--dsn", help="Override DATABASE_URL/SUPABASE_DB_URL")
    return parser.parse_args()


def fetch_company_by_id(cur, company_id: str) -> CompanyRow | None:
    cur.execute(
        """
        select id::text, name, ticker, sector, websites, favorite
        from companies
        where id = %s::uuid
        """,
        (company_id,),
    )
    row = cur.fetchone()
    return CompanyRow(*row) if row else None


def fetch_company_by_name(cur, company_name: str) -> CompanyRow | None:
    cur.execute(
        """
        select id::text, name, ticker, sector, websites, favorite
        from companies
        where name = %s
        """,
        (company_name,),
    )
    row = cur.fetchone()
    return CompanyRow(*row) if row else None


def resolve_company(cur, *, company_id: str | None, company_name: str, label: str) -> CompanyRow | None:
    row_by_id = fetch_company_by_id(cur, company_id) if company_id else None
    row_by_name = fetch_company_by_name(cur, company_name)
    if row_by_id and row_by_name and row_by_id.id != row_by_name.id:
        raise RuntimeError(
            f"{label} mismatch: id {company_id} resolves to {row_by_id.name}, "
            f"name {company_name!r} resolves to {row_by_name.id}"
        )
    return row_by_id or row_by_name


def fetch_scalar(cur, sql: str, params: tuple = ()) -> int:
    cur.execute(sql, params)
    return int(cur.fetchone()[0] or 0)


def fetch_window(cur, sql: str, params: tuple = ()) -> tuple[int, str | None, str | None]:
    cur.execute(sql, params)
    count_value, min_value, max_value = cur.fetchone()
    return int(count_value or 0), str(min_value) if min_value is not None else None, str(max_value) if max_value is not None else None


def print_company_row(label: str, company: CompanyRow | None) -> None:
    if company is None:
        log(f"{label}: missing")
        return
    log(
        f"{label}: {company.name} id={company.id} ticker={company.ticker or '-'} "
        f"sector={company.sector or '-'} favorite={company.favorite}"
    )


def print_company_linked_stats(cur, label: str, company: CompanyRow) -> None:
    log(f"\n[{label}] company-linked rows")

    ceo_count = fetch_scalar(cur, "select count(*) from ceos where company_id = %s::uuid", (company.id,))
    log(f"ceos: {ceo_count}")

    article_daily = fetch_window(
        cur,
        """
        select count(*), min(date), max(date)
        from company_article_mentions_daily
        where company_id = %s::uuid
        """,
        (company.id,),
    )
    log(
        "company_article_mentions_daily: "
        f"count={article_daily[0]} min_date={article_daily[1] or '-'} max_date={article_daily[2] or '-'}"
    )

    article_mentions = fetch_window(
        cur,
        """
        select count(*), min(a.published_at::date), max(a.published_at::date)
        from company_article_mentions cam
        left join articles a on a.id = cam.article_id
        where cam.company_id = %s::uuid
        """,
        (company.id,),
    )
    log(
        "company_article_mentions: "
        f"count={article_mentions[0]} min_published={article_mentions[1] or '-'} max_published={article_mentions[2] or '-'}"
    )

    article_overrides = fetch_scalar(
        cur,
        "select count(*) from company_article_overrides where company_id = %s::uuid",
        (company.id,),
    )
    log(f"company_article_overrides: {article_overrides}")

    serp_runs = fetch_window(
        cur,
        """
        select count(*), min(run_at::date), max(run_at::date)
        from serp_runs
        where entity_type = 'company'
          and company_id = %s::uuid
        """,
        (company.id,),
    )
    log(
        "serp_runs (company): "
        f"count={serp_runs[0]} min_run_date={serp_runs[1] or '-'} max_run_date={serp_runs[2] or '-'}"
    )

    serp_results = fetch_scalar(
        cur,
        """
        select count(*)
        from serp_results sr
        join serp_runs run on run.id = sr.serp_run_id
        where run.entity_type = 'company'
          and run.company_id = %s::uuid
        """,
        (company.id,),
    )
    log(f"serp_results (company): {serp_results}")

    feature_items = fetch_window(
        cur,
        """
        select count(*), min(date), max(date)
        from serp_feature_items
        where entity_type = any(%s)
          and entity_id = %s::uuid
        """,
        (["brand", "company"], company.id),
    )
    log(
        "serp_feature_items (company): "
        f"count={feature_items[0]} min_date={feature_items[1] or '-'} max_date={feature_items[2] or '-'}"
    )

    feature_summaries = fetch_window(
        cur,
        """
        select count(*), min(date), max(date)
        from serp_feature_summaries
        where entity_type = any(%s)
          and entity_id = %s::uuid
        """,
        (["brand", "company"], company.id),
    )
    log(
        "serp_feature_summaries (company): "
        f"count={feature_summaries[0]} min_date={feature_summaries[1] or '-'} max_date={feature_summaries[2] or '-'}"
    )


def print_source_name_remnants(cur, source_name: str) -> None:
    log(f"\n[old-name remnants] rows still carrying {source_name!r}")
    queries = [
        ("companies", "select count(*) from companies where name = %s", (source_name,)),
        ("roster", "select count(*) from roster where company = %s", (source_name,)),
        ("items", "select count(*) from items where company = %s", (source_name,)),
        ("stock_prices_daily", "select count(*) from stock_prices_daily where company = %s", (source_name,)),
        ("stock_price_snapshots", "select count(*) from stock_price_snapshots where company = %s", (source_name,)),
        ("trends_daily", "select count(*) from trends_daily where company = %s", (source_name,)),
        ("trends_snapshots", "select count(*) from trends_snapshots where company = %s", (source_name,)),
        (
            "serp_feature_items",
            """
            select count(*)
            from serp_feature_items
            where entity_type = any(%s)
              and entity_name = %s
            """,
            (["brand", "company"], source_name),
        ),
        (
            "serp_feature_daily",
            """
            select count(*)
            from serp_feature_daily
            where entity_type = any(%s)
              and entity_name = %s
            """,
            (["brand", "company"], source_name),
        ),
        (
            "serp_feature_summaries",
            """
            select count(*)
            from serp_feature_summaries
            where entity_type = any(%s)
              and entity_name = %s
            """,
            (["brand", "company"], source_name),
        ),
        (
            "entity_crisis_tag_daily",
            """
            select count(*)
            from entity_crisis_tag_daily
            where entity_type = any(%s)
              and entity_name = %s
            """,
            (["brand", "company"], source_name),
        ),
        (
            "entity_crisis_event_daily",
            """
            select count(*)
            from entity_crisis_event_daily
            where entity_type = any(%s)
              and entity_name = %s
            """,
            (["brand", "company"], source_name),
        ),
    ]
    total = 0
    for label, sql, params in queries:
        count = fetch_scalar(cur, sql, params)
        total += count
        log(f"{label}: {count}")
    log(f"total old-name remnants: {total}")


def print_article_date_check(cur, target: CompanyRow, article_date: str) -> None:
    log(f"\n[article-date check] {target.name} on {article_date}")

    cur.execute(
        """
        select
          count(*) as total_rows,
          count(*) filter (
            where coalesce(ov.override_sentiment_label, cam.sentiment_label, cad.sentiment_label) = 'positive'
          ) as positive_rows,
          count(*) filter (
            where coalesce(ov.override_sentiment_label, cam.sentiment_label, cad.sentiment_label) = 'neutral'
          ) as neutral_rows,
          count(*) filter (
            where coalesce(ov.override_sentiment_label, cam.sentiment_label, cad.sentiment_label) = 'negative'
          ) as negative_rows
        from company_article_mentions_daily cad
        left join company_article_mentions cam
          on cam.company_id = cad.company_id
         and cam.article_id = cad.article_id
        left join company_article_overrides ov
          on ov.company_id = cad.company_id
         and ov.article_id = cad.article_id
        where cad.company_id = %s::uuid
          and cad.date = %s::date
        """,
        (target.id, article_date),
    )
    total_rows, positive_rows, neutral_rows, negative_rows = cur.fetchone()
    log(
        f"rows={int(total_rows or 0)} positive={int(positive_rows or 0)} "
        f"neutral={int(neutral_rows or 0)} negative={int(negative_rows or 0)}"
    )

    cur.execute(
        """
        select
          coalesce(a.published_at::date, cad.date) as published_date,
          a.title,
          a.publisher,
          cad.sentiment_label as daily_sentiment,
          cam.sentiment_label as mention_sentiment,
          ov.override_sentiment_label as override_sentiment,
          coalesce(ov.override_sentiment_label, cam.sentiment_label, cad.sentiment_label) as effective_sentiment,
          a.canonical_url
        from company_article_mentions_daily cad
        left join company_article_mentions cam
          on cam.company_id = cad.company_id
         and cam.article_id = cad.article_id
        left join company_article_overrides ov
          on ov.company_id = cad.company_id
         and ov.article_id = cad.article_id
        join articles a on a.id = cad.article_id
        where cad.company_id = %s::uuid
          and cad.date = %s::date
        order by a.published_at desc nulls last, a.title
        limit 15
        """,
        (target.id, article_date),
    )
    rows = cur.fetchall()
    if not rows:
        log("no article rows found for that date")
        return
    log("sample article rows:")
    for published_date, title, publisher, daily_sentiment, mention_sentiment, override_sentiment, effective_sentiment, canonical_url in rows:
        log(
            f"- {published_date} | {effective_sentiment or '-'} "
            f"(daily={daily_sentiment or '-'} mention={mention_sentiment or '-'} override={override_sentiment or '-'})"
        )
        log(f"  {title or '-'} | {publisher or '-'}")
        log(f"  {canonical_url}")


def print_serp_date_check(cur, target: CompanyRow, serp_date: str) -> None:
    log(f"\n[serp-date check] {target.name} on {serp_date}")

    cur.execute(
        """
        select
          count(distinct run.id) as runs,
          count(sr.id) as results
        from serp_runs run
        left join serp_results sr on sr.serp_run_id = run.id
        where run.entity_type = 'company'
          and run.company_id = %s::uuid
          and run.run_at::date = %s::date
        """,
        (target.id, serp_date),
    )
    runs, results = cur.fetchone()
    log(f"runs={int(runs or 0)} results={int(results or 0)}")

    cur.execute(
        """
        select
          run.run_at::date as run_date,
          sr.rank,
          coalesce(sro.override_sentiment_label, sr.sentiment_label) as effective_sentiment,
          sr.title,
          sr.domain,
          sr.url
        from serp_runs run
        join serp_results sr on sr.serp_run_id = run.id
        left join serp_result_overrides sro on sro.serp_result_id = sr.id
        where run.entity_type = 'company'
          and run.company_id = %s::uuid
          and run.run_at::date = %s::date
        order by run.run_at, sr.rank nulls last
        limit 15
        """,
        (target.id, serp_date),
    )
    rows = cur.fetchall()
    if not rows:
        log("no SERP rows found for that date")
        return
    log("sample SERP rows:")
    for run_date, rank, effective_sentiment, title, domain, url in rows:
        log(f"- {run_date} rank={rank} sentiment={effective_sentiment or '-'} | {title or '-'} | {domain or '-'}")
        log(f"  {url or '-'}")


def print_ceo_rollup(cur, target: CompanyRow, source_name: str) -> None:
    log(f"\n[target CEOs] under {target.name}")
    cur.execute(
        """
        select
          ceo.name,
          count(distinct cad.article_id) as article_daily_rows,
          count(distinct cam.article_id) as article_rows,
          count(distinct case when run.entity_type = 'ceo' then run.id end) as serp_runs
        from ceos ceo
        left join ceo_article_mentions_daily cad on cad.ceo_id = ceo.id
        left join ceo_article_mentions cam on cam.ceo_id = ceo.id
        left join serp_runs run on run.ceo_id = ceo.id
        where ceo.company_id = %s::uuid
        group by ceo.name
        order by lower(ceo.name)
        """,
        (target.id,),
    )
    rows = cur.fetchall()
    if not rows:
        log("no CEOs found")
    for name, article_daily_rows, article_rows, serp_runs in rows:
        log(
            f"- {name}: ceo_article_mentions_daily={int(article_daily_rows or 0)} "
            f"ceo_article_mentions={int(article_rows or 0)} serp_runs={int(serp_runs or 0)}"
        )

    cur.execute(
        """
        select count(*)
        from ceos ceo
        join companies c on c.id = ceo.company_id
        where c.name = %s
        """,
        (source_name,),
    )
    source_ceos = int(cur.fetchone()[0] or 0)
    log(f"CEOs still attached to source company name {source_name!r}: {source_ceos}")


def main() -> int:
    args = parse_args()

    with get_conn(args.dsn, "verify_company_merge") as conn:
        with conn.cursor() as cur:
            source = resolve_company(
                cur,
                company_id=args.source_id,
                company_name=args.source_name,
                label="source",
            )
            target = resolve_company(
                cur,
                company_id=args.target_id,
                company_name=args.target_name,
                label="target",
            )

            if target is None:
                raise SystemExit(f"Target company {args.target_name!r} not found")

            log("[company rows]")
            print_company_row("source", source)
            print_company_row("target", target)

            if source is not None:
                print_company_linked_stats(cur, "source", source)
            else:
                log("\n[source] company row is gone, which is expected after a successful merge")

            print_company_linked_stats(cur, "target", target)
            print_source_name_remnants(cur, args.source_name)
            print_ceo_rollup(cur, target, args.source_name)

            if args.article_date:
                print_article_date_check(cur, target, args.article_date)

            if args.serp_date:
                print_serp_date_check(cur, target, args.serp_date)

    return 0


if __name__ == "__main__":
    sys.exit(main())
