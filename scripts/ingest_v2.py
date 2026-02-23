import csv
import re
import time
import zlib
from datetime import datetime, timezone
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import execute_values

from url_utils import normalize_url, url_hash
from risk_rules import (
    classify_control,
    is_financial_routine,
    parse_company_domains,
    should_neutralize_finance_routine,
)


def parse_bool(value):
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def parse_optional_bool(value):
    if value is None:
        return None
    val = str(value).strip().lower()
    if val == "":
        return None
    if val in {"true", "1", "yes", "y"}:
        return True
    if val in {"false", "0", "no", "n"}:
        return False
    return None




def parse_control_class(value: str | None) -> str | None:
    val = (value or "").strip().lower()
    if val in {"controlled", "true", "1", "yes", "y"}:
        return "controlled"
    if val in {"uncontrolled", "false", "0", "no", "n"}:
        return "uncontrolled"
    return None


def fetch_company_domains(conn) -> dict[str, set[str]]:
    with conn.cursor() as cur:
        cur.execute("select name, websites from companies")
        return {name: parse_company_domains(websites or "") for name, websites in cur.fetchall()}


ARTICLES_LOCK_KEY = zlib.crc32(b"risk_dashboard_articles") & 0x7FFFFFFF


def run_with_deadlock_retry(conn, fn, retries: int = 3, base_delay: float = 0.5):
    for attempt in range(retries):
        try:
            return fn()
        except (psycopg2.errors.DeadlockDetected, psycopg2.errors.SerializationFailure):
            try:
                conn.rollback()
            except Exception:
                pass
            if attempt >= retries - 1:
                raise
            time.sleep(base_delay * (2 ** attempt))


def upsert_companies_ceos(conn, roster_file_obj):
    reader = csv.DictReader(roster_file_obj)
    companies_map = {}
    ceos_map = {}

    for row in reader:
        company = (row.get('Company') or row.get('company') or '').strip()
        if not company:
            continue
        ticker = (row.get('Stock') or row.get('stock') or '').strip()
        sector = (row.get('Sector') or row.get('sector') or '').strip()
        websites = (row.get('Websites') or row.get('websites') or row.get('Website') or row.get('website') or '').strip()
        favorite = parse_bool(
            row.get('Favorite')
            or row.get('favorite')
            or row.get('Favorites')
            or row.get('favorites')
            or row.get('Company Favorite')
            or row.get('company_favorite')
            or row.get('Favorite Company')
            or row.get('favorite_company')
        )
        companies_map[company] = (company, ticker, sector, websites, favorite)

    companies = list(companies_map.values())
    if companies:
        sql = """
            insert into companies (name, ticker, sector, websites, favorite)
            values %s
            on conflict (name) do update set
              ticker = excluded.ticker,
              sector = excluded.sector,
              websites = excluded.websites,
              favorite = excluded.favorite
        """
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, companies, page_size=1000)

    roster_file_obj.seek(0)
    reader = csv.DictReader(roster_file_obj)

    for row in reader:
        company = (row.get('Company') or row.get('company') or '').strip()
        ceo = (row.get('CEO') or row.get('ceo') or '').strip()
        alias = (row.get('CEO Alias') or row.get('ceo alias') or row.get('alias') or '').strip()
        favorite = parse_bool(
            row.get('CEO Favorite')
            or row.get('ceo_favorite')
            or row.get('CEO Favorites')
            or row.get('ceo_favorites')
            or row.get('Favorites')
            or row.get('favorites')
            or row.get('Favorite CEO')
            or row.get('favorite_ceo')
            or row.get('Favorite (CEO)')
        )
        if not company or not ceo:
            continue
        ceos_map[(ceo, company)] = (ceo, company, alias, favorite)

    ceos = list(ceos_map.values())
    if ceos:
        sql = """
            insert into ceos (name, company_id, alias, favorite)
            select v.ceo, c.id, v.alias, v.favorite
            from (values %s) as v(ceo, company, alias, favorite)
            join companies c on c.name = v.company
            on conflict (name, company_id) do update set
              alias = excluded.alias,
              favorite = excluded.favorite
        """
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, ceos, page_size=1000)


def fetch_company_map(conn):
    with conn.cursor() as cur:
        cur.execute("select name, id from companies")
        return {name: cid for name, cid in cur.fetchall()}


def fetch_ceo_map(conn):
    with conn.cursor() as cur:
        cur.execute("select name, company_id, id from ceos")
        return {(name, company_id): cid for name, company_id, cid in cur.fetchall()}


def upsert_articles(conn, rows):
    if not rows:
        return
    sql = """
        insert into articles (canonical_url, title, publisher, snippet, published_at, first_seen_at, last_seen_at, source)
        values %s
        on conflict (canonical_url) do update set
          title = coalesce(excluded.title, articles.title),
          publisher = coalesce(excluded.publisher, articles.publisher),
          snippet = coalesce(excluded.snippet, articles.snippet),
          published_at = coalesce(excluded.published_at, articles.published_at),
          last_seen_at = excluded.last_seen_at
    """
    def _do_upsert():
        with conn:
            with conn.cursor() as cur:
                cur.execute("select pg_advisory_xact_lock(%s)", (ARTICLES_LOCK_KEY,))
                execute_values(cur, sql, rows, page_size=1000)
    run_with_deadlock_retry(conn, _do_upsert)


def fetch_article_map(conn, urls):
    if not urls:
        return {}
    sql = "select canonical_url, id from articles where canonical_url = any(%s)"
    with conn.cursor() as cur:
        cur.execute(sql, (list(urls),))
        return {u: aid for u, aid in cur.fetchall()}


def ensure_daily_partitions(cur, dt: datetime) -> None:
    start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    suffix = start.strftime("%Y_%m")
    for table in ("company_article_mentions_daily", "ceo_article_mentions_daily"):
        part = f"{table}_{suffix}"
        cur.execute(
            f"""
            create table if not exists {part}
            partition of {table}
            for values from (%s) to (%s)
            """,
            (start.date(), end.date()),
        )


def ingest_article_mentions(conn, file_obj, entity_type, date_str):
    reader = csv.DictReader(file_obj)
    now = datetime.now(timezone.utc)

    articles = {}
    mentions = []
    article_urls = []

    company_map = fetch_company_map(conn)
    ceo_map = fetch_ceo_map(conn)
    company_domains = fetch_company_domains(conn)

    for row in reader:
        title = (row.get('title') or '').strip()
        url = (row.get('url') or '').strip()
        if not title or not url:
            continue
        canonical = normalize_url(url)
        if not canonical:
            continue

        publisher = (row.get('source') or '').strip()
        snippet = (row.get('snippet') or '').strip()
        sentiment = (row.get('sentiment') or '').strip().lower() or None
        control_class = parse_control_class(row.get('controlled') or row.get('control_class'))
        finance_routine = parse_optional_bool(row.get('finance_routine'))
        if finance_routine is None:
            finance_routine = is_financial_routine(title, snippet, canonical, publisher)
        if should_neutralize_finance_routine(sentiment, title, snippet, canonical, publisher, finance_routine):
            sentiment = "neutral"
        uncertain = parse_bool(row.get('uncertain'))
        uncertain_reason = (row.get('uncertain_reason') or '').strip() or None
        llm_label = (row.get('llm_label') or '').strip() or None
        llm_severity = (row.get('llm_severity') or '').strip() or None
        llm_reason = (row.get('llm_reason') or '').strip() or None

        articles[canonical] = (canonical, title, publisher, None, None, now, now, 'google_rss')
        article_urls.append(canonical)

        if entity_type == 'company':
            company = (row.get('company') or '').strip()
            company_id = company_map.get(company)
            if not company_id:
                continue
            if control_class is None:
                control_class = "controlled" if classify_control(
                    company,
                    canonical,
                    company_domains,
                    publisher=publisher,
                ) else "uncontrolled"
            mentions.append((
                company_id, canonical, sentiment, control_class, finance_routine, uncertain, uncertain_reason,
                llm_label, llm_severity, llm_reason, date_str
            ))
        else:
            ceo = (row.get('ceo') or '').strip()
            company = (row.get('company') or '').strip()
            company_id = company_map.get(company)
            if not company_id:
                continue
            ceo_id = ceo_map.get((ceo, company_id))
            if not ceo_id:
                continue
            if control_class is None:
                control_class = "controlled" if classify_control(
                    company,
                    canonical,
                    company_domains,
                    entity_type="ceo",
                    person_name=ceo,
                    publisher=publisher,
                ) else "uncontrolled"
            mentions.append((
                ceo_id, canonical, sentiment, control_class, finance_routine, uncertain, uncertain_reason,
                llm_label, llm_severity, llm_reason, date_str
            ))

    upsert_articles(conn, list(articles.values()))
    article_map = fetch_article_map(conn, article_urls)

    scored_at = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)

    if entity_type == 'company':
        insert_rows = []
        daily_rows = []
        for company_id, canonical, sentiment, control_class, finance_routine, uncertain, uncertain_reason, llm_label, llm_severity, llm_reason, dstr in mentions:
            article_id = article_map.get(canonical)
            if not article_id:
                continue
            insert_rows.append((
                company_id, article_id, sentiment, control_class, finance_routine, uncertain, uncertain_reason,
                llm_label, llm_severity, llm_reason, scored_at, 'vader'
            ))
            daily_rows.append((
                scored_at.date(), company_id, article_id, sentiment, control_class, finance_routine, uncertain
            ))
        if insert_rows:
            sql = """
                insert into company_article_mentions (
                  company_id, article_id, sentiment_label, control_class, finance_routine, uncertain, uncertain_reason,
                  llm_label, llm_severity, llm_reason, scored_at, model_version
                )
                values %s
                on conflict (company_id, article_id) do update set
                  sentiment_label = excluded.sentiment_label,
                  control_class = excluded.control_class,
                  finance_routine = excluded.finance_routine,
                  uncertain = excluded.uncertain,
                  uncertain_reason = excluded.uncertain_reason,
                  llm_label = coalesce(excluded.llm_label, company_article_mentions.llm_label),
                  llm_severity = coalesce(excluded.llm_severity, company_article_mentions.llm_severity),
                  llm_reason = coalesce(excluded.llm_reason, company_article_mentions.llm_reason),
                  scored_at = excluded.scored_at,
                  model_version = excluded.model_version
            """
            with conn:
                with conn.cursor() as cur:
                    ensure_daily_partitions(cur, scored_at)
                    execute_values(cur, sql, insert_rows, page_size=1000)
        if daily_rows:
            sql = """
                insert into company_article_mentions_daily (
                  date, company_id, article_id, sentiment_label, control_class, finance_routine, uncertain
                )
                values %s
                on conflict (date, company_id, article_id) do update set
                  sentiment_label = excluded.sentiment_label,
                  control_class = excluded.control_class,
                  finance_routine = excluded.finance_routine,
                  uncertain = excluded.uncertain
            """
            with conn:
                with conn.cursor() as cur:
                    ensure_daily_partitions(cur, scored_at)
                    execute_values(cur, sql, daily_rows, page_size=1000)
    else:
        insert_rows = []
        daily_rows = []
        for ceo_id, canonical, sentiment, control_class, finance_routine, uncertain, uncertain_reason, llm_label, llm_severity, llm_reason, dstr in mentions:
            article_id = article_map.get(canonical)
            if not article_id:
                continue
            insert_rows.append((
                ceo_id, article_id, sentiment, control_class, finance_routine, uncertain, uncertain_reason,
                llm_label, llm_severity, llm_reason, scored_at, 'vader'
            ))
            daily_rows.append((
                scored_at.date(), ceo_id, article_id, sentiment, control_class, finance_routine, uncertain
            ))
        if insert_rows:
            sql = """
                insert into ceo_article_mentions (
                  ceo_id, article_id, sentiment_label, control_class, finance_routine, uncertain, uncertain_reason,
                  llm_label, llm_severity, llm_reason, scored_at, model_version
                )
                values %s
                on conflict (ceo_id, article_id) do update set
                  sentiment_label = excluded.sentiment_label,
                  control_class = excluded.control_class,
                  finance_routine = excluded.finance_routine,
                  uncertain = excluded.uncertain,
                  uncertain_reason = excluded.uncertain_reason,
                  llm_label = coalesce(excluded.llm_label, ceo_article_mentions.llm_label),
                  llm_severity = coalesce(excluded.llm_severity, ceo_article_mentions.llm_severity),
                  llm_reason = coalesce(excluded.llm_reason, ceo_article_mentions.llm_reason),
                  scored_at = excluded.scored_at,
                  model_version = excluded.model_version
            """
            with conn:
                with conn.cursor() as cur:
                    ensure_daily_partitions(cur, scored_at)
                    execute_values(cur, sql, insert_rows, page_size=1000)
        if daily_rows:
            sql = """
                insert into ceo_article_mentions_daily (
                  date, ceo_id, article_id, sentiment_label, control_class, finance_routine, uncertain
                )
                values %s
                on conflict (date, ceo_id, article_id) do update set
                  sentiment_label = excluded.sentiment_label,
                  control_class = excluded.control_class,
                  finance_routine = excluded.finance_routine,
                  uncertain = excluded.uncertain
            """
            with conn:
                with conn.cursor() as cur:
                    ensure_daily_partitions(cur, scored_at)
                    execute_values(cur, sql, daily_rows, page_size=1000)


def ingest_serp_results(conn, file_obj, entity_type, date_str):
    reader = csv.DictReader(file_obj)
    now = datetime.now(timezone.utc)

    company_map = fetch_company_map(conn)
    ceo_map = fetch_ceo_map(conn)

    run_rows = {}
    result_rows = []

    for row in reader:
        company = (row.get('company') or '').strip()
        ceo = (row.get('ceo') or '').strip()
        title = (row.get('title') or '').strip()
        snippet = (row.get('snippet') or '').strip()
        source = (row.get('source') or '').strip()
        url = (row.get('url') or row.get('link') or '').strip()
        if not title or not url:
            continue

        rank = row.get('position')
        try:
            rank = int(float(rank)) if rank not in (None, '') else None
        except Exception:
            rank = None

        canonical = normalize_url(url)
        domain = ''
        try:
            from urllib.parse import urlparse
            domain = (urlparse(url).hostname or '').replace('www.', '')
        except Exception:
            domain = ''

        sentiment = (row.get('sentiment') or '').strip().lower() or None
        finance_routine = parse_optional_bool(row.get('finance_routine'))
        if finance_routine is None:
            finance_routine = is_financial_routine(title, snippet, canonical, source)
        if should_neutralize_finance_routine(sentiment, title, snippet, canonical, source, finance_routine):
            sentiment = 'neutral'
        uncertain = parse_bool(row.get('uncertain'))
        uncertain_reason = (row.get('uncertain_reason') or '').strip() or None
        llm_label = (row.get('llm_label') or '').strip() or None
        llm_severity = (row.get('llm_severity') or '').strip() or None
        llm_reason = (row.get('llm_reason') or '').strip() or None
        controlled = (row.get('controlled') or '').strip().lower()
        control_class = None
        if controlled in {'true', '1', 'controlled'}:
            control_class = 'controlled'
        elif controlled in {'false', '0', 'uncontrolled'}:
            control_class = 'uncontrolled'

        if entity_type == 'company':
            company_id = company_map.get(company)
            if not company_id:
                continue
            run_key = ('company', company_id)
        else:
            company_id = company_map.get(company)
            if not company_id:
                continue
            ceo_id = ceo_map.get((ceo, company_id))
            if not ceo_id:
                continue
            run_key = ('ceo', ceo_id)

        if run_key not in run_rows:
            run_rows[run_key] = {
                'entity_type': run_key[0],
                'company_id': company_id if run_key[0] == 'company' else None,
                'ceo_id': run_key[1] if run_key[0] == 'ceo' else None,
                'query_text': company if run_key[0] == 'company' else f"{ceo} {company}",
                'provider': 'google_serp',
                'run_at': datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            }

        result_rows.append((
            run_key, rank, url, url_hash(canonical), title, snippet, domain,
            sentiment, control_class, finance_routine, uncertain, uncertain_reason, llm_label, llm_severity, llm_reason
        ))

    # Insert runs
    if run_rows:
        run_id_map = {}
        company_values = []
        ceo_values = []
        for key, data in run_rows.items():
            row = (data['entity_type'], data['company_id'], data['ceo_id'],
                   data['query_text'], data['provider'], data['run_at'])
            if data['entity_type'] == 'company':
                company_values.append(row)
            else:
                ceo_values.append(row)
        with conn:
            with conn.cursor() as cur:
                if company_values:
                    sql = """
                        insert into serp_runs (entity_type, company_id, ceo_id, query_text, provider, run_at)
                        values %s
                        on conflict (entity_type, company_id, run_at)
                          where entity_type = 'company' and company_id is not null
                        do update set
                          query_text = excluded.query_text,
                          provider = excluded.provider
                        returning id, entity_type, company_id, ceo_id
                    """
                    execute_values(cur, sql, company_values, page_size=1000)
                    for run_id, etype, company_id, ceo_id in cur.fetchall():
                        run_id_map[('company', company_id)] = run_id
                if ceo_values:
                    sql = """
                        insert into serp_runs (entity_type, company_id, ceo_id, query_text, provider, run_at)
                        values %s
                        on conflict (entity_type, ceo_id, run_at)
                          where entity_type = 'ceo' and ceo_id is not null
                        do update set
                          query_text = excluded.query_text,
                          provider = excluded.provider
                        returning id, entity_type, company_id, ceo_id
                    """
                    execute_values(cur, sql, ceo_values, page_size=1000)
                    for run_id, etype, company_id, ceo_id in cur.fetchall():
                        run_id_map[('ceo', ceo_id)] = run_id
    else:
        run_id_map = {}

    if result_rows:
        insert_map = {}
        for run_key, rank, url, uhash, title, snippet, domain, sentiment, control_class, finance_routine, uncertain, uncertain_reason, llm_label, llm_severity, llm_reason in result_rows:
            run_id = run_id_map.get(run_key)
            if not run_id:
                continue
            key = (run_id, rank, uhash)
            insert_map[key] = (
                run_id, rank, url, uhash, title, snippet, domain, sentiment, control_class,
                finance_routine, uncertain, uncertain_reason, llm_label, llm_severity, llm_reason
            )

        insert_rows = list(insert_map.values())
        if insert_rows:
            sql = """
                insert into serp_results (
                  serp_run_id, rank, url, url_hash, title, snippet, domain, sentiment_label, control_class,
                  finance_routine, uncertain, uncertain_reason, llm_label, llm_severity, llm_reason
                )
                values %s
                on conflict (serp_run_id, rank, url_hash) do update set
                  url = excluded.url,
                  title = excluded.title,
                  snippet = excluded.snippet,
                  domain = excluded.domain,
                  sentiment_label = excluded.sentiment_label,
                  control_class = excluded.control_class,
                  finance_routine = excluded.finance_routine,
                  uncertain = excluded.uncertain,
                  uncertain_reason = excluded.uncertain_reason,
                  llm_label = coalesce(excluded.llm_label, serp_results.llm_label),
                  llm_severity = coalesce(excluded.llm_severity, serp_results.llm_severity),
                  llm_reason = coalesce(excluded.llm_reason, serp_results.llm_reason)
            """
            with conn:
                with conn.cursor() as cur:
                    execute_values(cur, sql, insert_rows, page_size=1000)
