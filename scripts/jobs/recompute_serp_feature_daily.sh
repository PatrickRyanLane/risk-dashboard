#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_job_summary.sh"
job_summary_init "recompute_serp_feature_daily"

run_step "recompute_serp_feature_daily" python scripts/recompute_serp_feature_daily.py --days "${RECOMPUTE_DAYS:-7}"
