#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_job_summary.sh"
job_summary_init "send_targeted_alerts"

# Keep defaults aligned with current GitHub Actions workflow vars.
export TARGET_DEFAULT_CHANNEL="${TARGET_DEFAULT_CHANNEL:-#crisis-alerts-test}"
export BRAND_CHANNEL_MAP="${BRAND_CHANNEL_MAP:-{\"Fanatics\":\"#fanatics-alerts-test\"}}"
export TARGET_SKIP_COOLDOWN="${TARGET_SKIP_COOLDOWN:-1}"
export TARGET_SERP_GATE_ENABLED="${TARGET_SERP_GATE_ENABLED:-0}"
export TARGET_TOP_STORIES_GATE_ENABLED="${TARGET_TOP_STORIES_GATE_ENABLED:-1}"
export TARGET_TOP_STORIES_NEG_GATE_ENABLED="${TARGET_TOP_STORIES_NEG_GATE_ENABLED:-0}"
export TARGET_TOP_STORIES_TODAY_ONLY="${TARGET_TOP_STORIES_TODAY_ONLY:-1}"
export SERP_TOP_STORIES_REQUIRED="${SERP_TOP_STORIES_REQUIRED:-1}"
export SERP_TOP_STORIES_NEG_MIN="${SERP_TOP_STORIES_NEG_MIN:-2}"
export SERP_GATE_MIN="${SERP_GATE_MIN:-1}"
export SERP_GATE_DAYS="${SERP_GATE_DAYS:-2}"
export SERP_GATE_DEBUG="${SERP_GATE_DEBUG:-1}"
export ALERT_TIMEZONE="${ALERT_TIMEZONE:-America/New_York}"

run_step "send_targeted_alerts" python scripts/send_targeted_alerts.py
