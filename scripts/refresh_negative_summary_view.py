#!/usr/bin/env python3
import os
import sys

import psycopg2

REFRESH_LOCK_KEY = int(os.getenv("REFRESH_LOCK_KEY", "918273645"))


def refresh_view() -> None:
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
                return
            try:
                cur.execute("refresh materialized view concurrently negative_articles_summary_mv")
            except Exception:
                cur.execute("refresh materialized view negative_articles_summary_mv")
            try:
                cur.execute("refresh materialized view concurrently serp_feature_daily_mv")
            except Exception:
                cur.execute("refresh materialized view serp_feature_daily_mv")
            try:
                cur.execute("refresh materialized view concurrently article_daily_counts_mv")
            except Exception:
                cur.execute("refresh materialized view article_daily_counts_mv")
            try:
                cur.execute("refresh materialized view concurrently serp_daily_counts_mv")
            except Exception:
                cur.execute("refresh materialized view serp_daily_counts_mv")
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute("select pg_advisory_unlock(%s)", (REFRESH_LOCK_KEY,))
        except Exception:
            pass
        conn.close()


def main() -> int:
    try:
        refresh_view()
    except Exception as exc:
        print(f"[WARN] Failed to refresh negative summary view: {exc}")
        return 1
    print("[OK] Refreshed aggregate materialized views")
    return 0


if __name__ == "__main__":
    sys.exit(main())
