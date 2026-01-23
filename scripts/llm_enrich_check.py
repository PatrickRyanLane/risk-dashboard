#!/usr/bin/env python3
"""
Check how many DB rows are pending LLM enrichment.
"""
import os
import psycopg2


def get_conn():
    dsn = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not dsn:
        raise SystemExit("Set DATABASE_URL or SUPABASE_DB_URL")
    return psycopg2.connect(dsn)


def main() -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                select count(*)
                from company_article_mentions
                where uncertain is true and llm_sentiment_label is null
            """)
            company_articles = cur.fetchone()[0]
            cur.execute("""
                select count(*)
                from ceo_article_mentions
                where uncertain is true and llm_sentiment_label is null
            """)
            ceo_articles = cur.fetchone()[0]
            cur.execute("""
                select count(*)
                from serp_results
                where uncertain is true and llm_sentiment_label is null
            """)
            serp_results = cur.fetchone()[0]

    total = company_articles + ceo_articles + serp_results
    print(f"Pending LLM enrichment:")
    print(f"- company articles: {company_articles}")
    print(f"- CEO articles: {ceo_articles}")
    print(f"- SERP results: {serp_results}")
    print(f"- total: {total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
