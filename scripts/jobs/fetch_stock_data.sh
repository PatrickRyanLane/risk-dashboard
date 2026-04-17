#!/usr/bin/env bash
set -euo pipefail

GCS_BUCKET="${GCS_BUCKET:-risk-dashboard}"
python scripts/fetch_stock_data.py \
  --bucket "${GCS_BUCKET}" \
  --batch-size "${STOCK_BATCH_SIZE:-100}"
