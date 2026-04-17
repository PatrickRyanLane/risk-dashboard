#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-}"
REGION="${REGION:-us-east1}"
TIME_ZONE="${TIME_ZONE:-America/New_York}"
SCHEDULER_SERVICE_ACCOUNT="${SCHEDULER_SERVICE_ACCOUNT:-}"

DEPLOY_ALERT_JOBS="${DEPLOY_ALERT_JOBS:-true}"
DEPLOY_REFRESH_MVS_SCHEDULE="${DEPLOY_REFRESH_MVS_SCHEDULE:-true}"

require_var() {
  local name="$1"
  local value="$2"
  if [[ -z "$value" ]]; then
    echo "[ERROR] Missing required env var: ${name}" >&2
    exit 1
  fi
}

is_true() {
  local raw="${1:-}"
  local val
  val="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  case "$val" in
    1|true|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

require_var "PROJECT_ID" "$PROJECT_ID"
require_var "SCHEDULER_SERVICE_ACCOUNT" "$SCHEDULER_SERVICE_ACCOUNT"

gcloud services enable cloudscheduler.googleapis.com run.googleapis.com --project "$PROJECT_ID"

upsert_scheduler_job() {
  local scheduler_name="$1"
  local schedule="$2"
  local run_job="$3"

  local uri="https://run.googleapis.com/v2/projects/${PROJECT_ID}/locations/${REGION}/jobs/${run_job}:run"

  local action="create"
  if gcloud scheduler jobs describe "$scheduler_name" --location "$REGION" --project "$PROJECT_ID" >/dev/null 2>&1; then
    action="update"
  fi
  local action_label="Create"
  if [[ "$action" == "update" ]]; then
    action_label="Update"
  fi

  echo "[INFO] ${action_label} scheduler job: ${scheduler_name} -> ${run_job}"
  gcloud scheduler jobs "$action" http "$scheduler_name" \
    --project "$PROJECT_ID" \
    --location "$REGION" \
    --schedule "$schedule" \
    --time-zone "$TIME_ZONE" \
    --uri "$uri" \
    --http-method POST \
    --message-body '{}' \
    --oauth-service-account-email "$SCHEDULER_SERVICE_ACCOUNT" \
    --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform"
}

# Core daily jobs
upsert_scheduler_job "sch-rd-daily-brands" "10 5 * * *" "rd-daily-brands"
upsert_scheduler_job "sch-rd-daily-ceos" "10 6 * * *" "rd-daily-ceos"
upsert_scheduler_job "sch-rd-serp-features" "0 6 * * *" "rd-serp-features"
upsert_scheduler_job "sch-rd-llm-enrich" "30 10 * * *" "rd-llm-enrich"
upsert_scheduler_job "sch-rd-aggregate-negative" "30 8 * * *" "rd-aggregate-negative"
upsert_scheduler_job "sch-rd-recompute-serp-feature" "0 0 * * *" "rd-recompute-serp-feature"

if is_true "$DEPLOY_REFRESH_MVS_SCHEDULE"; then
  upsert_scheduler_job "sch-rd-refresh-mvs" "0 8 * * *" "rd-refresh-mvs"
else
  echo "[INFO] Skipping refresh-mvs schedule (DEPLOY_REFRESH_MVS_SCHEDULE=${DEPLOY_REFRESH_MVS_SCHEDULE})"
fi

# Market data jobs
upsert_scheduler_job "sch-rd-fetch-stock" "30 9 * * 1-5" "rd-fetch-stock"
upsert_scheduler_job "sch-rd-fetch-trends-b1" "30 10 * * 1-5" "rd-fetch-trends-b1"
upsert_scheduler_job "sch-rd-fetch-trends-b2" "0 14 * * 1-5" "rd-fetch-trends-b2"
upsert_scheduler_job "sch-rd-fetch-trends-b3" "0 18 * * 1-5" "rd-fetch-trends-b3"

if is_true "$DEPLOY_ALERT_JOBS"; then
  upsert_scheduler_job "sch-rd-send-crisis-alerts" "0 9 * * *" "rd-send-crisis-alerts"
  upsert_scheduler_job "sch-rd-send-targeted-alerts" "30 9 * * *" "rd-send-targeted-alerts"
else
  echo "[INFO] Skipping alert schedules (DEPLOY_ALERT_JOBS=${DEPLOY_ALERT_JOBS})"
fi

echo "[OK] Cloud Scheduler deployment complete."
