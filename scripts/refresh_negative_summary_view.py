#!/usr/bin/env python3
import argparse
import os
import sys
import time

import psycopg2

REFRESH_LOCK_KEY = int(os.getenv("REFRESH_LOCK_KEY", "918273645"))


def _refresh_one(cur, name: str, concurrent_sql: str, fallback_sql: str) -> None:
    print(f"[STEP] refresh {name}...")
    start = time.perf_counter()
    mode = "concurrent"
    try:
        cur.execute(concurrent_sql)
    except Exception:
        mode = "plain"
        cur.execute(fallback_sql)
    elapsed = time.perf_counter() - start
    print(f"[STEP] refresh {name} done in {elapsed:.2f}s (mode={mode})")


def refresh_view(selected) -> str:
    dsn = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set")
    conn = psycopg2.connect(dsn)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("select pg_try_advisory_lock(%s)", (REFRESH_LOCK_KEY,))
            got_lock = bool(cur.fetchone()[0])
            if not got_lock:
                print("[WARN] Refresh already running, skipping")
                return "skipped"

            if selected["negative_summary"]:
                _refresh_one(
                    cur,
                    "negative_articles_summary_mv",
                    "refresh materialized view concurrently negative_articles_summary_mv",
                    "refresh materialized view negative_articles_summary_mv",
                )

            if selected["serp_features"]:
                _refresh_one(
                    cur,
                    "serp_feature_daily_mv",
                    "refresh materialized view concurrently serp_feature_daily_mv",
                    "refresh materialized view serp_feature_daily_mv",
                )
                _refresh_one(
                    cur,
                    "serp_feature_control_daily_mv",
                    "refresh materialized view concurrently serp_feature_control_daily_mv",
                    "refresh materialized view serp_feature_control_daily_mv",
                )
                _refresh_one(
                    cur,
                    "serp_feature_daily_index_mv",
                    "refresh materialized view concurrently serp_feature_daily_index_mv",
                    "refresh materialized view serp_feature_daily_index_mv",
                )
                _refresh_one(
                    cur,
                    "serp_feature_control_daily_index_mv",
                    "refresh materialized view concurrently serp_feature_control_daily_index_mv",
                    "refresh materialized view serp_feature_control_daily_index_mv",
                )

            if selected["article_counts"]:
                _refresh_one(
                    cur,
                    "article_daily_counts_mv",
                    "refresh materialized view concurrently article_daily_counts_mv",
                    "refresh materialized view article_daily_counts_mv",
                )

            if selected["serp_counts"]:
                _refresh_one(
                    cur,
                    "serp_daily_counts_mv",
                    "refresh materialized view concurrently serp_daily_counts_mv",
                    "refresh materialized view serp_daily_counts_mv",
                )
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute("select pg_advisory_unlock(%s)", (REFRESH_LOCK_KEY,))
        except Exception:
            pass
        conn.close()
    return "ok"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--negative-summary", action="store_true")
    parser.add_argument("--serp-features", action="store_true")
    parser.add_argument("--article-counts", action="store_true")
    parser.add_argument("--serp-counts", action="store_true")
    args = parser.parse_args()

    selected = {
        "negative_summary": args.negative_summary,
        "serp_features": args.serp_features,
        "article_counts": args.article_counts,
        "serp_counts": args.serp_counts,
    }
    if not any(selected.values()):
        selected = {k: True for k in selected}

    try:
        status = refresh_view(selected)
    except Exception as exc:
        print(f"[WARN] Failed to refresh materialized views: {exc}")
        return 1
    if status == "skipped":
        print("[WARN] Refresh skipped due to active lock")
        return 0
    print("[OK] Refreshed aggregate materialized views")
    return 0


if __name__ == "__main__":
    sys.exit(main())
