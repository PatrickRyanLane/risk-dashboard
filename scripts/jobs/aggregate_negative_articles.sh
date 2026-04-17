#!/usr/bin/env bash
set -euo pipefail

GCS_BUCKET="${GCS_BUCKET:-risk-dashboard}"
python scripts/aggregate_negative_articles.py \
  --days-back "${NEGATIVE_SUMMARY_DAYS_BACK:-90}" \
  --bucket "${GCS_BUCKET}"
