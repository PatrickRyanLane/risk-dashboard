#!/usr/bin/env bash
set -euo pipefail

python scripts/recompute_serp_feature_daily.py --days "${RECOMPUTE_DAYS:-7}"
