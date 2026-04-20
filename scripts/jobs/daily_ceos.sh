#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_job_summary.sh"
job_summary_init "daily_ceos"

GCS_BUCKET="${GCS_BUCKET:-risk-dashboard}"
RUN_DATE="${RUN_DATE:-$(date -u +%F)}"
export ARTICLES_MAX_PER_ALIAS="${ARTICLES_MAX_PER_ALIAS:-25}"
export ARTICLES_SLEEP_SEC="${ARTICLES_SLEEP_SEC:-0.35}"

run_step "ingest_roster_only" python scripts/ingest_roster_only.py \
  --roster-path "gs://${GCS_BUCKET}/rosters/main-roster.csv" \
  --boards-path ""

run_step "news_articles_ceos" python scripts/news_articles_ceos.py \
  --date "${RUN_DATE}" \
  --bucket "${GCS_BUCKET}" \
  --batch-size "${CEOS_BATCH_SIZE:-1500}"

run_step "news_sentiment_ceos" python scripts/news_sentiment_ceos.py \
  --date "${RUN_DATE}" \
  --bucket "${GCS_BUCKET}"

run_step "backfill_article_mentions_ceo" python scripts/backfill_article_mentions_daily.py \
  --date "${RUN_DATE}" \
  --entity-type ceo

run_step "process_serps_ceos" python scripts/process_serps_ceos.py \
  --date "${RUN_DATE}" \
  --bucket "${GCS_BUCKET}"

run_step "refresh_negative_summary_view" python scripts/refresh_negative_summary_view.py --article-counts --serp-counts
