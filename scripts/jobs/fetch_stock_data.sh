#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_job_summary.sh"
job_summary_init "fetch_stock_data"

GCS_BUCKET="${GCS_BUCKET:-risk-dashboard}"
run_step "fetch_stock_data" python scripts/fetch_stock_data.py \
  --bucket "${GCS_BUCKET}" \
  --batch-size "${STOCK_BATCH_SIZE:-100}"
