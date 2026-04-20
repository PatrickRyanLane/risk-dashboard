#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_job_summary.sh"
job_summary_init "refresh_mvs"

run_step "refresh_negative_summary_view" python scripts/refresh_negative_summary_view.py --article-counts --serp-counts
