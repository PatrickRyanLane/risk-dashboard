#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_job_summary.sh"
job_summary_init "fetch_trends_batch3_merge"

GCS_BUCKET="${GCS_BUCKET:-risk-dashboard}"
run_step "fetch_trends_batch3" python scripts/fetch_trends_data.py \
  --batch 3 \
  --batch-size "${TRENDS_BATCH_SIZE:-300}" \
  --bucket "${GCS_BUCKET}"

run_step "sleep_before_merge" sleep 5
run_step "fetch_trends_merge" python scripts/fetch_trends_data.py --merge --bucket "${GCS_BUCKET}"
