#!/usr/bin/env bash
set -euo pipefail

GCS_BUCKET="${GCS_BUCKET:-risk-dashboard}"
RUN_DATE="${RUN_DATE:-$(date -u +%F)}"
export ARTICLES_MAX_PER_ALIAS="${ARTICLES_MAX_PER_ALIAS:-25}"
export ARTICLES_SLEEP_SEC="${ARTICLES_SLEEP_SEC:-0.35}"

python scripts/ingest_roster_only.py \
  --roster-path "gs://${GCS_BUCKET}/rosters/main-roster.csv" \
  --boards-path ""

python scripts/news_articles_ceos.py \
  --date "${RUN_DATE}" \
  --bucket "${GCS_BUCKET}" \
  --batch-size "${CEOS_BATCH_SIZE:-1500}"

python scripts/news_sentiment_ceos.py \
  --date "${RUN_DATE}" \
  --bucket "${GCS_BUCKET}"

python scripts/backfill_article_mentions_daily.py \
  --date "${RUN_DATE}" \
  --entity-type ceo

python scripts/process_serps_ceos.py \
  --date "${RUN_DATE}" \
  --bucket "${GCS_BUCKET}"

python scripts/refresh_negative_summary_view.py --article-counts --serp-counts
