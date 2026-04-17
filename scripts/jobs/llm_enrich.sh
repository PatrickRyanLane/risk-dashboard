#!/usr/bin/env bash
set -euo pipefail

MAX_CALLS="${LLM_MAX_OVERRIDE:-${LLM_MAX_CALLS:-200}}"
python scripts/llm_enrich.py \
  --max-calls "${MAX_CALLS}" \
  --batch-size "${LLM_BATCH_SIZE:-200}"
