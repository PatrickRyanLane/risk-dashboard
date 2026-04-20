#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_job_summary.sh"
job_summary_init "llm_enrich"

MAX_CALLS="${LLM_MAX_OVERRIDE:-${LLM_MAX_CALLS:-200}}"
run_step "llm_enrich" python scripts/llm_enrich.py \
  --max-calls "${MAX_CALLS}" \
  --batch-size "${LLM_BATCH_SIZE:-200}"
