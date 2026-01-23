import os
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Tuple
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import execute_values


def get_conn():
    dsn = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not dsn:
        return None
    return psycopg2.connect(dsn)


def normalize_url(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    if not url:
        return ""
    from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
    import re
    parsed = urlparse(url)
    scheme = (parsed.scheme or "http").lower()
    netloc = (parsed.netloc or "").lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = re.sub(r"//+", "/", parsed.path or "")
    tracking = {
        "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
        "gclid", "fbclid", "igshid", "mc_cid", "mc_eid", "vero_id",
    }
    query_pairs = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k not in tracking]
    query_pairs.sort()
    query = urlencode(query_pairs, doseq=True)
    return urlunparse((scheme, netloc, path, "", query, ""))


def url_hash(url: str) -> str:
    import hashlib
    normalized = normalize_url(url)
    if not normalized:
        return ""
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def parse_bool(val):
    return str(val).strip().lower() in {"true", "1", "yes", "y", "t", "controlled"}


def fetch_company_map(cur) -> Dict[str, str]:
    cur.execute("select name, id from companies")
    return {name: cid for name, cid in cur.fetchall()}


def fetch_ceo_map(cur) -> Dict[Tuple[str, str], str]:
    cur.execute("select name, company_id, id from ceos")
    return {(name, company_id): cid for name, company_id, cid in cur.fetchall()}


def upsert_articles_mentions(df, entity_type: str, date_str: str) -> int:
    conn = get_conn()
    if conn is None or df is None or df.empty:
        return 0

    now = datetime.now(timezone.utc)
    articles = {}
    mentions = []
    article_urls = []

    with conn:
        with conn.cursor() as cur:
            company_map = fetch_company_map(cur)
            ceo_map = fetch_ceo_map(cur)

    for _, row in df.iterrows():
        title = str(row.get("title", "") or "").strip()
        url = str(row.get("url", "") or "").strip()
        if not title or not url:
            continue
        canonical = normalize_url(url)
        if not canonical:
            continue
        publisher = str(row.get("source", "") or "").strip()
        sentiment = (row.get("sentiment") or "").strip().lower() or None
        finance_routine = parse_bool(row.get("finance_routine"))
        uncertain = parse_bool(row.get("uncertain"))
        uncertain_reason = (row.get("uncertain_reason") or "").strip() or None
        llm_label = (row.get("llm_label") or "").strip() or None
        llm_severity = (row.get("llm_severity") or "").strip() or None
        llm_reason = (row.get("llm_reason") or "").strip() or None

        articles[canonical] = (canonical, title, publisher, None, None, now, now, "google_rss")
        article_urls.append(canonical)

        if entity_type == "company":
            company = str(row.get("company", "") or "").strip()
            company_id = company_map.get(company)
            if not company_id:
                continue
            mentions.append((company_id, canonical, sentiment, finance_routine, uncertain,
                             uncertain_reason, llm_label, llm_severity, llm_reason))
        else:
            ceo = str(row.get("ceo", "") or "").strip()
            company = str(row.get("company", "") or "").strip()
            company_id = company_map.get(company)
            if not company_id:
                continue
            ceo_id = ceo_map.get((ceo, company_id))
            if not ceo_id:
                continue
            mentions.append((ceo_id, canonical, sentiment, finance_routine, uncertain,
                             uncertain_reason, llm_label, llm_severity, llm_reason))

    if not articles or not mentions:
        conn.close()
        return 0

    scored_at = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    with conn:
        with conn.cursor() as cur:
            execute_values(cur, """
                insert into articles (canonical_url, title, publisher, snippet, published_at, first_seen_at, last_seen_at, source)
                values %s
                on conflict (canonical_url) do update set
                  title = coalesce(excluded.title, articles.title),
                  publisher = coalesce(excluded.publisher, articles.publisher),
                  snippet = coalesce(excluded.snippet, articles.snippet),
                  published_at = coalesce(excluded.published_at, articles.published_at),
                  last_seen_at = excluded.last_seen_at
            """, list(articles.values()), page_size=1000)

            cur.execute("select canonical_url, id from articles where canonical_url = any(%s)", (list(set(article_urls)),))
            article_map = {u: aid for u, aid in cur.fetchall()}

            if entity_type == "company":
                insert_rows = []
                for company_id, canonical, sentiment, finance_routine, uncertain, uncertain_reason, llm_label, llm_severity, llm_reason in mentions:
                    article_id = article_map.get(canonical)
                    if not article_id:
                        continue
                    insert_rows.append((
                        company_id, article_id, sentiment, finance_routine, uncertain, uncertain_reason,
                        llm_label, llm_severity, llm_reason, scored_at, "vader"
                    ))
                if insert_rows:
                    execute_values(cur, """
                        insert into company_article_mentions (
                          company_id, article_id, sentiment_label, finance_routine, uncertain, uncertain_reason,
                          llm_label, llm_severity, llm_reason, scored_at, model_version
                        )
                        values %s
                        on conflict (company_id, article_id) do update set
                          sentiment_label = excluded.sentiment_label,
                          finance_routine = excluded.finance_routine,
                          uncertain = excluded.uncertain,
                          uncertain_reason = excluded.uncertain_reason,
                          llm_label = coalesce(excluded.llm_label, company_article_mentions.llm_label),
                          llm_severity = coalesce(excluded.llm_severity, company_article_mentions.llm_severity),
                          llm_reason = coalesce(excluded.llm_reason, company_article_mentions.llm_reason),
                          scored_at = excluded.scored_at,
                          model_version = excluded.model_version
                    """, insert_rows, page_size=1000)
            else:
                insert_rows = []
                for ceo_id, canonical, sentiment, finance_routine, uncertain, uncertain_reason, llm_label, llm_severity, llm_reason in mentions:
                    article_id = article_map.get(canonical)
                    if not article_id:
                        continue
                    insert_rows.append((
                        ceo_id, article_id, sentiment, finance_routine, uncertain, uncertain_reason,
                        llm_label, llm_severity, llm_reason, scored_at, "vader"
                    ))
                if insert_rows:
                    execute_values(cur, """
                        insert into ceo_article_mentions (
                          ceo_id, article_id, sentiment_label, finance_routine, uncertain, uncertain_reason,
                          llm_label, llm_severity, llm_reason, scored_at, model_version
                        )
                        values %s
                        on conflict (ceo_id, article_id) do update set
                          sentiment_label = excluded.sentiment_label,
                          finance_routine = excluded.finance_routine,
                          uncertain = excluded.uncertain,
                          uncertain_reason = excluded.uncertain_reason,
                          llm_label = coalesce(excluded.llm_label, ceo_article_mentions.llm_label),
                          llm_severity = coalesce(excluded.llm_severity, ceo_article_mentions.llm_severity),
                          llm_reason = coalesce(excluded.llm_reason, ceo_article_mentions.llm_reason),
                          scored_at = excluded.scored_at,
                          model_version = excluded.model_version
                    """, insert_rows, page_size=1000)

    conn.close()
    return len(mentions)


def upsert_serp_results(df, entity_type: str, date_str: str) -> int:
    conn = get_conn()
    if conn is None or df is None or df.empty:
        return 0

    now = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    with conn:
        with conn.cursor() as cur:
            company_map = fetch_company_map(cur)
            ceo_map = fetch_ceo_map(cur)

    run_rows = {}
    result_rows = []

    for _, row in df.iterrows():
        company = str(row.get("company", "") or "").strip()
        ceo = str(row.get("ceo", "") or "").strip()
        title = str(row.get("title", "") or "").strip()
        url = str(row.get("url", "") or row.get("link", "") or "").strip()
        if not title or not url:
            continue
        try:
            rank = int(float(row.get("position", 0) or 0))
        except Exception:
            rank = 0

        canonical = normalize_url(url)
        if not canonical:
            continue
        domain = ""
        try:
            domain = (urlparse(url).hostname or "").replace("www.", "")
        except Exception:
            domain = ""

        sentiment = (row.get("sentiment") or "").strip().lower() or None
        controlled = (row.get("controlled") or "").strip().lower()
        control_class = None
        if controlled in {"true", "1", "controlled"}:
            control_class = "controlled"
        elif controlled in {"false", "0", "uncontrolled"}:
            control_class = "uncontrolled"

        finance_routine = parse_bool(row.get("finance_routine"))
        uncertain = parse_bool(row.get("uncertain"))
        uncertain_reason = (row.get("uncertain_reason") or "").strip() or None
        llm_label = (row.get("llm_label") or "").strip() or None
        llm_severity = (row.get("llm_severity") or "").strip() or None
        llm_reason = (row.get("llm_reason") or "").strip() or None

        if entity_type == "company":
            company_id = company_map.get(company)
            if not company_id:
                continue
            run_key = ("company", company_id)
        else:
            company_id = company_map.get(company)
            if not company_id:
                continue
            ceo_id = ceo_map.get((ceo, company_id))
            if not ceo_id:
                continue
            run_key = ("ceo", ceo_id)

        if run_key not in run_rows:
            run_rows[run_key] = {
                "entity_type": run_key[0],
                "company_id": company_id if run_key[0] == "company" else None,
                "ceo_id": run_key[1] if run_key[0] == "ceo" else None,
                "query_text": company if run_key[0] == "company" else f"{ceo} {company}",
                "provider": "google_serp",
                "run_at": now,
            }

        result_rows.append((
            run_key, rank, url, url_hash(canonical), title, row.get("snippet") or "",
            domain, sentiment, control_class, finance_routine, uncertain,
            uncertain_reason, llm_label, llm_severity, llm_reason
        ))

    if not run_rows or not result_rows:
        conn.close()
        return 0

    run_id_map = {}
    with conn:
        with conn.cursor() as cur:
            run_values = [
                (v["entity_type"], v["company_id"], v["ceo_id"], v["query_text"], v["provider"], v["run_at"])
                for v in run_rows.values()
            ]
            execute_values(cur, """
                insert into serp_runs (entity_type, company_id, ceo_id, query_text, provider, run_at)
                values %s
                on conflict (entity_type, company_id, ceo_id, run_at) do update set
                  query_text = excluded.query_text,
                  provider = excluded.provider
                returning id, entity_type, company_id, ceo_id
            """, run_values, page_size=1000)
            rows = cur.fetchall()
            for run_id, etype, company_id, ceo_id in rows:
                if etype == "company":
                    run_id_map[("company", company_id)] = run_id
                else:
                    run_id_map[("ceo", ceo_id)] = run_id

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
                execute_values(cur, """
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
                """, insert_rows, page_size=1000)

    conn.close()
    return len(result_rows)


def _parse_dates_and_values(date_history: str, value_history: str) -> List[Tuple]:
    dates = [d.strip() for d in (date_history or "").split("|") if d.strip()]
    values = [v.strip() for v in (value_history or "").split("|") if v.strip()]
    if not dates or not values or len(dates) != len(values):
        return []
    out = []
    for d, v in zip(dates, values):
        try:
            dval = datetime.strptime(d, "%Y-%m-%d").date()
        except ValueError:
            continue
        try:
            fval = float(v)
        except ValueError:
            fval = None
        out.append((dval, fval))
    return out


def upsert_stock_df(df) -> int:
    conn = get_conn()
    if conn is None or df is None or df.empty:
        return 0

    daily_rows = []
    snapshot_rows = []

    for _, row in df.iterrows():
        ticker = str(row.get("ticker", "") or "").strip()
        company = str(row.get("company", "") or "").strip()
        if not ticker or not company:
            continue
        series = _parse_dates_and_values(row.get("date_history"), row.get("price_history"))
        for dval, pval in series:
            if pval is None:
                continue
            daily_rows.append((ticker, company, dval, pval))

        last_updated = row.get("last_updated")
        try:
            last_ts = datetime.fromisoformat(str(last_updated)) if last_updated else None
        except ValueError:
            last_ts = None

        def to_float(val):
            try:
                return float(val) if val not in (None, "") else None
            except ValueError:
                return None

        opening_price = to_float(row.get("opening_price"))
        daily_change_pct = to_float(row.get("daily_change"))
        seven_day_change_pct = to_float(row.get("seven_day_change"))

        prices_only = [p for _, p in series if p is not None]
        if daily_change_pct is None and len(prices_only) >= 2:
            prev = prices_only[-2]
            last = prices_only[-1]
            if prev:
                daily_change_pct = ((last - prev) / prev) * 100
        if seven_day_change_pct is None and len(prices_only) >= 8:
            prev7 = prices_only[-8]
            last = prices_only[-1]
            if prev7:
                seven_day_change_pct = ((last - prev7) / prev7) * 100

        snapshot_rows.append((
            ticker, company, last_ts.date() if last_ts else None, opening_price,
            daily_change_pct, seven_day_change_pct, last_ts
        ))

    if not daily_rows and not snapshot_rows:
        conn.close()
        return 0

    with conn:
        with conn.cursor() as cur:
            if daily_rows:
                execute_values(cur, """
                    insert into stock_prices_daily (ticker, company, date, price)
                    values %s
                    on conflict (ticker, date) do update set
                      price = excluded.price,
                      company = excluded.company
                """, daily_rows, page_size=1000)
            if snapshot_rows:
                execute_values(cur, """
                    insert into stock_price_snapshots (
                      ticker, company, as_of_date, opening_price,
                      daily_change_pct, seven_day_change_pct, last_updated
                    ) values %s
                    on conflict (ticker, last_updated) do update set
                      opening_price = excluded.opening_price,
                      daily_change_pct = excluded.daily_change_pct,
                      seven_day_change_pct = excluded.seven_day_change_pct,
                      as_of_date = excluded.as_of_date,
                      company = excluded.company
                """, snapshot_rows, page_size=1000)

    conn.close()
    return len(daily_rows)


def upsert_trends_df(df) -> int:
    conn = get_conn()
    if conn is None or df is None or df.empty:
        return 0

    daily_rows = []
    snapshot_rows = []

    for _, row in df.iterrows():
        company = str(row.get("company", "") or "").strip()
        if not company:
            continue
        date_history = row.get("date_history")
        trends_history = row.get("trends_history")
        last_updated = row.get("last_updated")
        avg_interest = row.get("avg_interest")
        if avg_interest in (None, ""):
            avg_interest = row.get("average_interest")

        for dval, ival in _parse_dates_and_values(date_history, trends_history):
            if ival is None:
                continue
            daily_rows.append((company, dval, int(ival)))

        try:
            last_ts = datetime.fromisoformat(str(last_updated)) if last_updated else None
        except ValueError:
            last_ts = None
        try:
            avg_val = float(avg_interest) if avg_interest not in (None, "") else None
        except ValueError:
            avg_val = None
        snapshot_rows.append((company, avg_val, last_ts))

    if not daily_rows and not snapshot_rows:
        conn.close()
        return 0

    with conn:
        with conn.cursor() as cur:
            if daily_rows:
                deduped = {}
                for company, dval, ival in daily_rows:
                    deduped[(company, dval)] = ival
                daily_rows = [(c, d, i) for (c, d), i in deduped.items()]
                execute_values(cur, """
                    insert into trends_daily (company, date, interest)
                    values %s
                    on conflict (company, date) do update set
                      interest = excluded.interest
                """, daily_rows, page_size=1000)
            if snapshot_rows:
                deduped = {}
                for company, avg_val, last_ts in snapshot_rows:
                    if last_ts is None:
                        continue
                    deduped[(company, last_ts)] = avg_val
                snapshot_rows = [(c, v, ts) for (c, ts), v in deduped.items()]
                execute_values(cur, """
                    insert into trends_snapshots (company, avg_interest, last_updated)
                    values %s
                    on conflict (company, last_updated) do update set
                      avg_interest = excluded.avg_interest
                """, snapshot_rows, page_size=1000)

    conn.close()
    return len(daily_rows)
