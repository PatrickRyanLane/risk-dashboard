#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_job_summary.sh"
job_summary_init "send_crisis_alerts"

# Keep defaults aligned with current GitHub Actions workflow vars.
export ALERT_BRANDS="${ALERT_BRANDS:-1}"
export ALERT_CEOS="${ALERT_CEOS:-1}"
export SERP_GATE_ENABLED="${SERP_GATE_ENABLED:-0}"
export SERP_TOP_STORIES_REQUIRED="${SERP_TOP_STORIES_REQUIRED:-1}"
export SERP_TOP_STORIES_NEG_MIN="${SERP_TOP_STORIES_NEG_MIN:-4}"
export SERP_GATE_MIN="${SERP_GATE_MIN:-1}"
export SERP_GATE_DAYS="${SERP_GATE_DAYS:-2}"
export SERP_GATE_DEBUG="${SERP_GATE_DEBUG:-1}"
export ALERT_LOOKBACK_DAYS="${ALERT_LOOKBACK_DAYS:-1}"
export TOP_STORIES_TODAY_ONLY="${TOP_STORIES_TODAY_ONLY:-1}"
export ALERT_TIMEZONE="${ALERT_TIMEZONE:-America/New_York}"

run_step "send_crisis_alerts" python scripts/send_crisis_alerts.py \
  --force-brands "${FORCE_BRANDS:-}" \
  --force-send "${FORCE_SEND:-0}"
