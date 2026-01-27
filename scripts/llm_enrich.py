#!/usr/bin/env python3
"""
LLM enrichment for DB rows (uncertain + missing llm_sentiment_label).

Required env:
  DATABASE_URL or SUPABASE_DB_URL
  LLM_API_KEY

Optional env:
  LLM_PROVIDER (openai|gemini)
  LLM_MODEL
  LLM_MAX_CALLS

Usage:
  python3 scripts/llm_enrich.py --max-calls 200
"""
import argparse
import os
from typing import List, Tuple

import psycopg2

from llm_utils import build_risk_prompt, call_llm_json


def get_conn():
    dsn = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not dsn:
        raise SystemExit("Set DATABASE_URL or SUPABASE_DB_URL")
    return psycopg2.connect(dsn)


def fetch_company_articles(cur, limit: int) -> List[Tuple]:
    sql = """
        select m.company_id, m.article_id, a.canonical_url, a.title, a.snippet, a.publisher, c.name
        from company_article_mentions m
        join articles a on a.id = m.article_id
        join companies c on c.id = m.company_id
        left join company_article_overrides ov on ov.company_id = m.company_id and ov.article_id = m.article_id
        where m.uncertain is true
          and m.llm_sentiment_label is null
          and ov.override_sentiment_label is null
          and ov.override_control_class is null
        order by
          case when m.sentiment_label = 'negative' then 1 else 0 end desc,
          case
            when m.uncertain_reason = 'low_compound' then 2
            when m.uncertain_reason = 'short_title' then 1
            else 0
          end desc,
          length(coalesce(a.title, '')) asc,
          m.scored_at desc nulls last
        limit %s
        for update of m skip locked
    """
    cur.execute(sql, (limit,))
    return cur.fetchall()


def fetch_ceo_articles(cur, limit: int) -> List[Tuple]:
    sql = """
        select m.ceo_id, m.article_id, a.canonical_url, a.title, a.snippet, a.publisher, ce.name
        from ceo_article_mentions m
        join articles a on a.id = m.article_id
        join ceos ce on ce.id = m.ceo_id
        left join ceo_article_overrides ov on ov.ceo_id = m.ceo_id and ov.article_id = m.article_id
        where m.uncertain is true
          and m.llm_sentiment_label is null
          and ov.override_sentiment_label is null
          and ov.override_control_class is null
        order by
          case when m.sentiment_label = 'negative' then 1 else 0 end desc,
          case
            when m.uncertain_reason = 'low_compound' then 2
            when m.uncertain_reason = 'short_title' then 1
            else 0
          end desc,
          length(coalesce(a.title, '')) asc,
          m.scored_at desc nulls last
        limit %s
        for update of m skip locked
    """
    cur.execute(sql, (limit,))
    return cur.fetchall()


def fetch_serp_results(cur, entity_type: str, limit: int) -> List[Tuple]:
    sql = """
        select r.serp_run_id, r.rank, r.url_hash, r.url, r.title, r.snippet,
               sr.company_id, sr.ceo_id,
               sr.entity_type, c.name as company, ce.name as ceo
        from serp_results r
        join serp_runs sr on sr.id = r.serp_run_id
        left join companies c on c.id = sr.company_id
        left join ceos ce on ce.id = sr.ceo_id
        left join serp_result_overrides ov on ov.serp_result_id = r.id
        where r.uncertain is true
          and r.llm_sentiment_label is null
          and sr.entity_type = %s
          and ov.override_sentiment_label is null
          and ov.override_control_class is null
        order by
          case when r.sentiment_label = 'negative' then 1 else 0 end desc,
          case
            when r.uncertain_reason = 'low_compound' then 2
            when r.uncertain_reason = 'short_title' then 1
            else 0
          end desc,
          length(coalesce(r.title, '')) asc,
          sr.run_at desc nulls last
        limit %s
        for update of r skip locked
    """
    cur.execute(sql, (entity_type, limit))
    return cur.fetchall()


def fetch_serp_feature_items(cur, entity_type: str, limit: int) -> List[Tuple]:
    sql = """
        select sfi.id, sfi.entity_id, sfi.entity_name, sfi.url_hash, sfi.url, sfi.title, sfi.snippet, sfi.source
        from serp_feature_items sfi
        left join serp_feature_item_overrides ov on ov.serp_feature_item_id = sfi.id
        where sfi.entity_type = %s
          and sfi.llm_sentiment_label is null
          and (ov.override_sentiment_label is null and ov.override_control_class is null)
        order by
          case when sfi.sentiment_label = 'negative' then 1 else 0 end desc,
          length(coalesce(sfi.title, '')) asc,
          sfi.date desc
        limit %s
        for update of sfi skip locked
    """
    cur.execute(sql, (entity_type, limit))
    return cur.fetchall()


def lookup_feature_item_cache(cur, entity_type: str, entity_id: str, url_hash: str):
    sql = """
        select sfi.llm_sentiment_label, sfi.llm_risk_label, sfi.llm_control_class,
               sfi.llm_severity, sfi.llm_reason
        from serp_feature_items sfi
        where sfi.entity_type = %s
          and sfi.entity_id = %s
          and sfi.url_hash = %s
          and sfi.llm_sentiment_label is not null
        order by sfi.date desc nulls last
        limit 1
    """
    cur.execute(sql, (entity_type, entity_id, url_hash))
    return cur.fetchone()


def update_serp_feature_item(cur, item_id, llm_sentiment_label, llm_risk_label,
                             llm_control_class, llm_severity, llm_reason):
    sql = """
        update serp_feature_items
        set llm_sentiment_label = %s, llm_risk_label = %s,
            llm_control_class = %s, llm_severity = %s, llm_reason = %s
        where id = %s
    """
    cur.execute(
        sql,
        (llm_sentiment_label, llm_risk_label, llm_control_class, llm_severity, llm_reason, item_id)
    )

def lookup_serp_cache(cur, entity_type: str, entity_id: str, url_hash: str):
    if entity_type == "company":
        sql = """
            select r.llm_sentiment_label, r.llm_risk_label, r.llm_control_class,
                   r.llm_severity, r.llm_reason
            from serp_results r
            join serp_runs sr on sr.id = r.serp_run_id
            where sr.entity_type = 'company'
              and sr.company_id = %s
              and r.url_hash = %s
              and r.llm_sentiment_label is not null
            order by sr.run_at desc nulls last
            limit 1
        """
        cur.execute(sql, (entity_id, url_hash))
    else:
        sql = """
            select r.llm_sentiment_label, r.llm_risk_label, r.llm_control_class,
                   r.llm_severity, r.llm_reason
            from serp_results r
            join serp_runs sr on sr.id = r.serp_run_id
            where sr.entity_type = 'ceo'
              and sr.ceo_id = %s
              and r.url_hash = %s
              and r.llm_sentiment_label is not null
            order by sr.run_at desc nulls last
            limit 1
        """
        cur.execute(sql, (entity_id, url_hash))
    return cur.fetchone()


def update_company_article(cur, company_id, article_id, llm_sentiment_label, llm_risk_label,
                           llm_control_class, llm_severity, llm_reason):
    sql = """
        update company_article_mentions
        set llm_sentiment_label = %s, llm_risk_label = %s,
            llm_control_class = %s, llm_severity = %s, llm_reason = %s
        where company_id = %s and article_id = %s
    """
    cur.execute(
        sql,
        (llm_sentiment_label, llm_risk_label, llm_control_class, llm_severity, llm_reason,
         company_id, article_id)
    )


def update_ceo_article(cur, ceo_id, article_id, llm_sentiment_label, llm_risk_label,
                       llm_control_class, llm_severity, llm_reason):
    sql = """
        update ceo_article_mentions
        set llm_sentiment_label = %s, llm_risk_label = %s,
            llm_control_class = %s, llm_severity = %s, llm_reason = %s
        where ceo_id = %s and article_id = %s
    """
    cur.execute(
        sql,
        (llm_sentiment_label, llm_risk_label, llm_control_class, llm_severity, llm_reason,
         ceo_id, article_id)
    )


def update_serp(cur, serp_run_id, rank, url_hash, llm_sentiment_label, llm_risk_label,
                llm_control_class, llm_severity, llm_reason):
    sql = """
        update serp_results
        set llm_sentiment_label = %s, llm_risk_label = %s,
            llm_control_class = %s, llm_severity = %s, llm_reason = %s
        where serp_run_id = %s and rank = %s and url_hash = %s
    """
    cur.execute(
        sql,
        (llm_sentiment_label, llm_risk_label, llm_control_class, llm_severity, llm_reason,
         serp_run_id, rank, url_hash)
    )


def lookup_article_cache(cur, entity_type: str, entity_id: str, article_id: str):
    if entity_type == "company":
        sql = """
            select llm_sentiment_label, llm_risk_label, llm_control_class,
                   llm_severity, llm_reason
            from company_article_mentions
            where company_id = %s
              and article_id = %s
              and llm_sentiment_label is not null
            limit 1
        """
        cur.execute(sql, (entity_id, article_id))
    else:
        sql = """
            select llm_sentiment_label, llm_risk_label, llm_control_class,
                   llm_severity, llm_reason
            from ceo_article_mentions
            where ceo_id = %s
              and article_id = %s
              and llm_sentiment_label is not null
            limit 1
        """
        cur.execute(sql, (entity_id, article_id))
    return cur.fetchone()


def main() -> int:
    parser = argparse.ArgumentParser(description="LLM-enrich uncertain rows in Postgres.")
    parser.add_argument("--max-calls", type=int, default=0, help="Total LLM call cap (overrides env).")
    parser.add_argument("--batch-size", type=int, default=200, help="Max rows per category query.")
    args = parser.parse_args()

    llm_api_key = os.getenv("LLM_API_KEY", "")
    llm_model = os.getenv("LLM_MODEL", "gpt-4o-mini")
    max_calls = args.max_calls or int(os.getenv("LLM_MAX_CALLS", "0"))

    if not llm_api_key:
        print("LLM_API_KEY not set. Exiting.")
        return 1
    if max_calls <= 0:
        print("LLM_MAX_CALLS is 0. Exiting.")
        return 0

    commit_every = max(1, int(os.getenv("LLM_COMMIT_EVERY", "50") or 50))

    calls = 0
    updated = 0
    cache_hits = 0
    cache_hits_articles = 0
    updates_since_commit = 0
    log_every = max(1, min(25, max_calls // 10))
    article_cache = {}
    serp_cache = {}
    feature_cache = {}

    with get_conn() as conn:
        with conn.cursor() as cur:
            for rows, handler in [
                (fetch_company_articles(cur, args.batch_size), "company_articles"),
                (fetch_ceo_articles(cur, args.batch_size), "ceo_articles"),
            ]:
                for row in rows:
                    if calls >= max_calls:
                        break
                    entity_id, article_id, url, title, snippet, source, name = row
                    article_key = f"{handler}:{entity_id}:{article_id}"
                    cached = article_cache.get(article_key)
                    if cached is None and article_id and entity_id:
                        cached = lookup_article_cache(
                            cur,
                            "company" if handler == "company_articles" else "ceo",
                            entity_id,
                            article_id
                        )
                        article_cache[article_key] = cached
                    if cached:
                        llm_sentiment_label, llm_risk_label, llm_control_class, llm_severity, llm_reason = cached
                        if handler == "company_articles":
                            update_company_article(
                                cur, entity_id, article_id, llm_sentiment_label, llm_risk_label,
                                llm_control_class, llm_severity, llm_reason
                            )
                        else:
                            update_ceo_article(
                                cur, entity_id, article_id, llm_sentiment_label, llm_risk_label,
                                llm_control_class, llm_severity, llm_reason
                            )
                        updated += 1
                        cache_hits_articles += 1
                        updates_since_commit += 1
                        if updates_since_commit >= commit_every:
                            conn.commit()
                            updates_since_commit = 0
                        continue
                    prompt = build_risk_prompt("brand" if handler == "company_articles" else "ceo",
                                               name, title, snippet or "", source or "", url or "")
                    resp = call_llm_json(prompt, llm_api_key, llm_model)
                    if not isinstance(resp, dict) or not resp:
                        continue
                    llm_sentiment_label = resp.get("sentiment_label") or resp.get("label")
                    if llm_sentiment_label not in ("positive", "neutral", "negative"):
                        llm_sentiment_label = None
                    llm_risk_label = resp.get("risk_label")
                    if llm_risk_label not in ("crisis_risk", "routine_financial", "not_risk"):
                        llm_risk_label = None
                    llm_severity = resp.get("severity")
                    llm_reason = resp.get("reason")
                    llm_control_class = resp.get("control_class")
                    if llm_control_class not in ("controlled", "uncontrolled"):
                        llm_control_class = None
                    if handler == "company_articles":
                        update_company_article(
                            cur, entity_id, article_id, llm_sentiment_label, llm_risk_label,
                            llm_control_class, llm_severity, llm_reason
                        )
                    else:
                        update_ceo_article(
                            cur, entity_id, article_id, llm_sentiment_label, llm_risk_label,
                            llm_control_class, llm_severity, llm_reason
                        )
                    calls += 1
                    updated += 1
                    updates_since_commit += 1
                    if updates_since_commit >= commit_every:
                        conn.commit()
                        updates_since_commit = 0
                    if calls % log_every == 0 or calls == max_calls:
                        print(f"… LLM progress: {calls}/{max_calls} calls")
                if calls >= max_calls:
                    break

            if calls < max_calls:
                for entity_type in ("company", "ceo"):
                    rows = fetch_serp_results(cur, entity_type, args.batch_size)
                    for row in rows:
                        if calls >= max_calls:
                            break
                        serp_run_id, rank, url_hash, url, title, snippet, company_id, ceo_id, _, company, ceo = row
                        entity_id = company_id if entity_type == "company" else ceo_id
                        name = company if entity_type == "company" else ceo
                        prompt = build_risk_prompt("brand" if entity_type == "company" else "ceo",
                                                   name or "", title or "", snippet or "", "", url or "")
                        cache_key = (entity_type, str(entity_id or ""), str(url_hash or ""))
                        cached = serp_cache.get(cache_key)
                        if cached is None and entity_id and url_hash:
                            cached = lookup_serp_cache(cur, entity_type, entity_id, url_hash)
                            serp_cache[cache_key] = cached
                        if cached:
                            llm_sentiment_label, llm_risk_label, llm_control_class, llm_severity, llm_reason = cached
                            update_serp(
                                cur, serp_run_id, rank, url_hash, llm_sentiment_label, llm_risk_label,
                                llm_control_class, llm_severity, llm_reason
                            )
                            updated += 1
                            cache_hits += 1
                            updates_since_commit += 1
                            if updates_since_commit >= commit_every:
                                conn.commit()
                                updates_since_commit = 0
                            continue
                        resp = call_llm_json(prompt, llm_api_key, llm_model)
                        if not isinstance(resp, dict) or not resp:
                            continue
                        llm_sentiment_label = resp.get("sentiment_label") or resp.get("label")
                        if llm_sentiment_label not in ("positive", "neutral", "negative"):
                            llm_sentiment_label = None
                        llm_risk_label = resp.get("risk_label")
                        if llm_risk_label not in ("crisis_risk", "routine_financial", "not_risk"):
                            llm_risk_label = None
                        llm_severity = resp.get("severity")
                        llm_reason = resp.get("reason")
                        llm_control_class = resp.get("control_class")
                        if llm_control_class not in ("controlled", "uncontrolled"):
                            llm_control_class = None
                        update_serp(
                            cur, serp_run_id, rank, url_hash, llm_sentiment_label, llm_risk_label,
                            llm_control_class, llm_severity, llm_reason
                        )
                        calls += 1
                        updated += 1
                        updates_since_commit += 1
                        if updates_since_commit >= commit_every:
                            conn.commit()
                            updates_since_commit = 0
                        if calls % log_every == 0 or calls == max_calls:
                            print(f"… LLM progress: {calls}/{max_calls} calls")
                    if calls >= max_calls:
                        break

            if calls < max_calls:
                for entity_type in ("company", "ceo"):
                    rows = fetch_serp_feature_items(cur, entity_type, args.batch_size)
                    for row in rows:
                        if calls >= max_calls:
                            break
                        item_id, entity_id, entity_name, url_hash, url, title, snippet, source = row
                        prompt = build_risk_prompt(
                            "brand" if entity_type == "company" else "ceo",
                            entity_name or "", title or "", snippet or "", source or "", url or ""
                        )
                        cache_key = (entity_type, str(entity_id or ""), str(url_hash or ""))
                        cached = feature_cache.get(cache_key)
                        if cached is None and entity_id and url_hash:
                            cached = lookup_feature_item_cache(cur, entity_type, entity_id, url_hash)
                            feature_cache[cache_key] = cached
                        if cached:
                            llm_sentiment_label, llm_risk_label, llm_control_class, llm_severity, llm_reason = cached
                            update_serp_feature_item(
                                cur, item_id, llm_sentiment_label, llm_risk_label,
                                llm_control_class, llm_severity, llm_reason
                            )
                            updated += 1
                            cache_hits += 1
                            updates_since_commit += 1
                            if updates_since_commit >= commit_every:
                                conn.commit()
                                updates_since_commit = 0
                            continue
                        resp = call_llm_json(prompt, llm_api_key, llm_model)
                        if not isinstance(resp, dict) or not resp:
                            continue
                        llm_sentiment_label = resp.get("sentiment_label") or resp.get("label")
                        if llm_sentiment_label not in ("positive", "neutral", "negative"):
                            llm_sentiment_label = None
                        llm_risk_label = resp.get("risk_label")
                        if llm_risk_label not in ("crisis_risk", "routine_financial", "not_risk"):
                            llm_risk_label = None
                        llm_severity = resp.get("severity")
                        llm_reason = resp.get("reason")
                        llm_control_class = resp.get("control_class")
                        if llm_control_class not in ("controlled", "uncontrolled"):
                            llm_control_class = None
                        update_serp_feature_item(
                            cur, item_id, llm_sentiment_label, llm_risk_label,
                            llm_control_class, llm_severity, llm_reason
                        )
                        calls += 1
                        updated += 1
                        updates_since_commit += 1
                        if updates_since_commit >= commit_every:
                            conn.commit()
                            updates_since_commit = 0
                        if calls % log_every == 0 or calls == max_calls:
                            print(f"… LLM progress: {calls}/{max_calls} calls")
                    if calls >= max_calls:
                        break

        if updates_since_commit:
            conn.commit()

    print(f"LLM calls used: {calls} / {max_calls}")
    print(f"Rows updated: {updated}")
    print(f"Cache hits (SERP): {cache_hits}")
    print(f"Cache hits (articles): {cache_hits_articles}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
