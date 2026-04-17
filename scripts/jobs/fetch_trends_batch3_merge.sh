#!/usr/bin/env bash
set -euo pipefail

GCS_BUCKET="${GCS_BUCKET:-risk-dashboard}"
python scripts/fetch_trends_data.py \
  --batch 3 \
  --batch-size "${TRENDS_BATCH_SIZE:-300}" \
  --bucket "${GCS_BUCKET}"

sleep 5
python scripts/fetch_trends_data.py --merge --bucket "${GCS_BUCKET}"
