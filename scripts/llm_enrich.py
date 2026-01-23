#!/usr/bin/env python3
"""
LLM enrichment for DB rows (uncertain + missing llm_label).

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
        where m.uncertain is true and m.llm_label is null
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
    """
    cur.execute(sql, (limit,))
    return cur.fetchall()


def fetch_ceo_articles(cur, limit: int) -> List[Tuple]:
    sql = """
        select m.ceo_id, m.article_id, a.canonical_url, a.title, a.snippet, a.publisher, ce.name
        from ceo_article_mentions m
        join articles a on a.id = m.article_id
        join ceos ce on ce.id = m.ceo_id
        where m.uncertain is true and m.llm_label is null
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
    """
    cur.execute(sql, (limit,))
    return cur.fetchall()


def fetch_serp_results(cur, entity_type: str, limit: int) -> List[Tuple]:
    sql = """
        select r.serp_run_id, r.rank, r.url_hash, r.url, r.title, r.snippet,
               sr.entity_type, c.name as company, ce.name as ceo
        from serp_results r
        join serp_runs sr on sr.id = r.serp_run_id
        left join companies c on c.id = sr.company_id
        left join ceos ce on ce.id = sr.ceo_id
        where r.uncertain is true and r.llm_label is null and sr.entity_type = %s
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
    """
    cur.execute(sql, (entity_type, limit))
    return cur.fetchall()


def update_company_article(cur, company_id, article_id, llm_label, llm_severity, llm_reason):
    sql = """
        update company_article_mentions
        set llm_label = %s, llm_severity = %s, llm_reason = %s
        where company_id = %s and article_id = %s
    """
    cur.execute(sql, (llm_label, llm_severity, llm_reason, company_id, article_id))


def update_ceo_article(cur, ceo_id, article_id, llm_label, llm_severity, llm_reason):
    sql = """
        update ceo_article_mentions
        set llm_label = %s, llm_severity = %s, llm_reason = %s
        where ceo_id = %s and article_id = %s
    """
    cur.execute(sql, (llm_label, llm_severity, llm_reason, ceo_id, article_id))


def update_serp(cur, serp_run_id, rank, url_hash, llm_label, llm_severity, llm_reason):
    sql = """
        update serp_results
        set llm_label = %s, llm_severity = %s, llm_reason = %s
        where serp_run_id = %s and rank = %s and url_hash = %s
    """
    cur.execute(sql, (llm_label, llm_severity, llm_reason, serp_run_id, rank, url_hash))


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

    calls = 0
    updated = 0

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
                    prompt = build_risk_prompt("brand" if handler == "company_articles" else "ceo",
                                               name, title, snippet or "", source or "", url or "")
                    resp = call_llm_json(prompt, llm_api_key, llm_model)
                    if not isinstance(resp, dict) or not resp:
                        continue
                    llm_label = resp.get("label")
                    llm_severity = resp.get("severity")
                    llm_reason = resp.get("reason")
                    if handler == "company_articles":
                        update_company_article(cur, entity_id, article_id, llm_label, llm_severity, llm_reason)
                    else:
                        update_ceo_article(cur, entity_id, article_id, llm_label, llm_severity, llm_reason)
                    calls += 1
                    updated += 1
                if calls >= max_calls:
                    break

            if calls < max_calls:
                for entity_type in ("company", "ceo"):
                    rows = fetch_serp_results(cur, entity_type, args.batch_size)
                    for row in rows:
                        if calls >= max_calls:
                            break
                        serp_run_id, rank, url_hash, url, title, snippet, _, company, ceo = row
                        name = company if entity_type == "company" else ceo
                        prompt = build_risk_prompt("brand" if entity_type == "company" else "ceo",
                                                   name or "", title or "", snippet or "", "", url or "")
                        resp = call_llm_json(prompt, llm_api_key, llm_model)
                        if not isinstance(resp, dict) or not resp:
                            continue
                        llm_label = resp.get("label")
                        llm_severity = resp.get("severity")
                        llm_reason = resp.get("reason")
                        update_serp(cur, serp_run_id, rank, url_hash, llm_label, llm_severity, llm_reason)
                        calls += 1
                        updated += 1
                    if calls >= max_calls:
                        break

        conn.commit()

    print(f"LLM calls used: {calls} / {max_calls}")
    print(f"Rows updated: {updated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
