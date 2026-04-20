#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_job_summary.sh"
job_summary_init "fetch_trends_batch2"

GCS_BUCKET="${GCS_BUCKET:-risk-dashboard}"
run_step "fetch_trends_batch2" python scripts/fetch_trends_data.py \
  --batch 2 \
  --batch-size "${TRENDS_BATCH_SIZE:-300}" \
  --bucket "${GCS_BUCKET}"
