#!/usr/bin/env bash
# shellcheck shell=bash

JOB_SUMMARY_NAME="${JOB_SUMMARY_NAME:-$(basename "$0" .sh)}"
JOB_SUMMARY_START_EPOCH="$(date +%s)"
JOB_SUMMARY_START_ISO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
JOB_SUMMARY_STEPS_TOTAL=0
JOB_SUMMARY_STEPS_OK=0
JOB_SUMMARY_STEPS_FAILED=0
JOB_SUMMARY_STATUS="success"
JOB_SUMMARY_FAILED_STEP=""
JOB_SUMMARY_FAILED_EXIT_CODE=0

job_summary_init() {
  local name="${1:-}"
  if [[ -n "$name" ]]; then
    JOB_SUMMARY_NAME="$name"
  fi
  JOB_SUMMARY_START_EPOCH="$(date +%s)"
  JOB_SUMMARY_START_ISO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "[JOB_START] name=${JOB_SUMMARY_NAME} at=${JOB_SUMMARY_START_ISO}"
  trap '_job_summary_on_exit $?' EXIT
}

run_step() {
  local step_name="$1"
  shift

  local step_start_epoch step_start_iso step_end_iso step_duration
  step_start_epoch="$(date +%s)"
  step_start_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  JOB_SUMMARY_STEPS_TOTAL=$((JOB_SUMMARY_STEPS_TOTAL + 1))
  echo "[STEP_START] job=${JOB_SUMMARY_NAME} step=${step_name} at=${step_start_iso}"

  if "$@"; then
    step_duration=$(( $(date +%s) - step_start_epoch ))
    step_end_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    JOB_SUMMARY_STEPS_OK=$((JOB_SUMMARY_STEPS_OK + 1))
    echo "[STEP_DONE] job=${JOB_SUMMARY_NAME} step=${step_name} duration_sec=${step_duration} at=${step_end_iso}"
    return 0
  fi

  local exit_code=$?
  step_duration=$(( $(date +%s) - step_start_epoch ))
  step_end_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  JOB_SUMMARY_STEPS_FAILED=$((JOB_SUMMARY_STEPS_FAILED + 1))
  JOB_SUMMARY_STATUS="failed"
  JOB_SUMMARY_FAILED_STEP="$step_name"
  JOB_SUMMARY_FAILED_EXIT_CODE="$exit_code"
  echo "[STEP_FAIL] job=${JOB_SUMMARY_NAME} step=${step_name} exit_code=${exit_code} duration_sec=${step_duration} at=${step_end_iso}" >&2
  return "$exit_code"
}

_job_summary_on_exit() {
  local exit_code="${1:-0}"
  local end_epoch end_iso duration
  end_epoch="$(date +%s)"
  end_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  duration=$(( end_epoch - JOB_SUMMARY_START_EPOCH ))

  if [[ "$exit_code" -ne 0 && "$JOB_SUMMARY_STATUS" == "success" ]]; then
    JOB_SUMMARY_STATUS="failed"
    JOB_SUMMARY_FAILED_EXIT_CODE="$exit_code"
  fi

  JOB_SUMMARY_END_ISO="$end_iso"
  JOB_SUMMARY_DURATION_SEC="$duration"
  JOB_SUMMARY_EXIT_CODE="$exit_code"

  export JOB_SUMMARY_NAME JOB_SUMMARY_STATUS JOB_SUMMARY_START_ISO JOB_SUMMARY_END_ISO
  export JOB_SUMMARY_DURATION_SEC JOB_SUMMARY_STEPS_TOTAL JOB_SUMMARY_STEPS_OK JOB_SUMMARY_STEPS_FAILED
  export JOB_SUMMARY_FAILED_STEP JOB_SUMMARY_EXIT_CODE

  echo "[JOB_SUMMARY] name=${JOB_SUMMARY_NAME} status=${JOB_SUMMARY_STATUS} duration_sec=${duration} steps_total=${JOB_SUMMARY_STEPS_TOTAL} steps_ok=${JOB_SUMMARY_STEPS_OK} steps_failed=${JOB_SUMMARY_STEPS_FAILED} failed_step=${JOB_SUMMARY_FAILED_STEP:-none} exit_code=${exit_code}"

  local json_payload
  json_payload="$(
python - <<'PY'
import json
import os

payload = {
    "duration_sec": int(os.getenv("JOB_SUMMARY_DURATION_SEC", "0")),
    "ended_at": os.getenv("JOB_SUMMARY_END_ISO", ""),
    "exit_code": int(os.getenv("JOB_SUMMARY_EXIT_CODE", "0")),
    "failed_step": os.getenv("JOB_SUMMARY_FAILED_STEP", "") or None,
    "job": os.getenv("JOB_SUMMARY_NAME", ""),
    "started_at": os.getenv("JOB_SUMMARY_START_ISO", ""),
    "status": os.getenv("JOB_SUMMARY_STATUS", ""),
    "steps_failed": int(os.getenv("JOB_SUMMARY_STEPS_FAILED", "0")),
    "steps_ok": int(os.getenv("JOB_SUMMARY_STEPS_OK", "0")),
    "steps_total": int(os.getenv("JOB_SUMMARY_STEPS_TOTAL", "0")),
}
print(json.dumps(payload, sort_keys=True))
PY
)"
  echo "[JOB_SUMMARY_JSON] ${json_payload}"
}
