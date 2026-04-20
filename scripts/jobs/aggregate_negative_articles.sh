#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_job_summary.sh"
job_summary_init "aggregate_negative_articles"

GCS_BUCKET="${GCS_BUCKET:-risk-dashboard}"
run_step "aggregate_negative_articles" python scripts/aggregate_negative_articles.py \
  --days-back "${NEGATIVE_SUMMARY_DAYS_BACK:-90}" \
  --bucket "${GCS_BUCKET}"
