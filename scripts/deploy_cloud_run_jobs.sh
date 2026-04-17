#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-}"
REGION="${REGION:-us-east1}"
AR_LOCATION="${AR_LOCATION:-$REGION}"
AR_REPO="${AR_REPO:-risk-dashboard-jobs}"
IMAGE_NAME="${IMAGE_NAME:-risk-dashboard-jobs}"
IMAGE_TAG="${IMAGE_TAG:-$(date -u +%Y%m%d-%H%M%S)}"
JOB_SERVICE_ACCOUNT="${JOB_SERVICE_ACCOUNT:-}"
GCS_BUCKET="${GCS_BUCKET:-risk-dashboard}"

DEPLOY_ALERT_JOBS="${DEPLOY_ALERT_JOBS:-true}"
DEPLOY_REFRESH_MVS_JOB="${DEPLOY_REFRESH_MVS_JOB:-true}"
SKIP_BUILD="${SKIP_BUILD:-false}"

LLM_PROVIDER="${LLM_PROVIDER:-openai}"
LLM_MODEL="${LLM_MODEL:-gpt-4o-mini}"
LLM_MAX_CALLS="${LLM_MAX_CALLS:-200}"
LLM_SUMMARY_MAX_CALLS="${LLM_SUMMARY_MAX_CALLS:-20}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_URI="${AR_LOCATION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/${IMAGE_NAME}:${IMAGE_TAG}"

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
require_var "JOB_SERVICE_ACCOUNT" "$JOB_SERVICE_ACCOUNT"

echo "[INFO] Project: ${PROJECT_ID}"
echo "[INFO] Region: ${REGION}"
echo "[INFO] Artifact Registry location: ${AR_LOCATION}"
echo "[INFO] Image: ${IMAGE_URI}"
echo "[INFO] Job service account: ${JOB_SERVICE_ACCOUNT}"

gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  secretmanager.googleapis.com \
  --project "$PROJECT_ID"

if ! gcloud artifacts repositories describe "$AR_REPO" \
  --location "$AR_LOCATION" \
  --project "$PROJECT_ID" >/dev/null 2>&1; then
  echo "[INFO] Creating Artifact Registry repo: ${AR_REPO}"
  gcloud artifacts repositories create "$AR_REPO" \
    --repository-format docker \
    --location "$AR_LOCATION" \
    --description "Risk dashboard Cloud Run jobs images" \
    --project "$PROJECT_ID"
fi

echo "[INFO] Building jobs image..."
if is_true "$SKIP_BUILD"; then
  echo "[INFO] Skipping image build (SKIP_BUILD=${SKIP_BUILD})"
else
  gcloud builds submit "$ROOT_DIR" \
    --config "$ROOT_DIR/cloudbuild.jobs.yaml" \
    --substitutions "_IMAGE_URI=${IMAGE_URI}" \
    --project "$PROJECT_ID"
fi

upsert_job() {
  local name="$1"
  local cpu="$2"
  local memory="$3"
  local timeout="$4"
  local retries="$5"
  local command="$6"
  local env_vars="$7"
  local secrets="$8"

  local action="create"
  if gcloud run jobs describe "$name" --region "$REGION" --project "$PROJECT_ID" >/dev/null 2>&1; then
    action="update"
  fi
  local action_label="Create"
  if [[ "$action" == "update" ]]; then
    action_label="Update"
  fi

  local cmd=(
    gcloud run jobs "$action" "$name"
    --project "$PROJECT_ID"
    --region "$REGION"
    --image "$IMAGE_URI"
    --service-account "$JOB_SERVICE_ACCOUNT"
    --tasks 1
    --parallelism 1
    --cpu "$cpu"
    --memory "$memory"
    --task-timeout "$timeout"
    --max-retries "$retries"
    --command "$command"
  )

  if [[ -n "$env_vars" ]]; then
    cmd+=(--set-env-vars "$env_vars")
  fi
  if [[ -n "$secrets" ]]; then
    cmd+=(--set-secrets "$secrets")
  fi

  echo "[INFO] ${action_label} job: ${name}"
  "${cmd[@]}"
}

COMMON_ENV="GCS_BUCKET=${GCS_BUCKET}"
DB_SECRET="DATABASE_URL=DATABASE_URL:latest"

upsert_job "rd-daily-brands" \
  "1" "2Gi" "7200s" "1" "/app/scripts/jobs/daily_brands.sh" \
  "$COMMON_ENV" "$DB_SECRET"

upsert_job "rd-daily-ceos" \
  "1" "2Gi" "7200s" "1" "/app/scripts/jobs/daily_ceos.sh" \
  "$COMMON_ENV" "$DB_SECRET"

upsert_job "rd-serp-features" \
  "2" "4Gi" "5400s" "1" "/app/scripts/jobs/serp_features_daily.sh" \
  "$COMMON_ENV" "$DB_SECRET"

upsert_job "rd-fetch-trends-b1" \
  "1" "1Gi" "3600s" "1" "/app/scripts/jobs/fetch_trends_batch1.sh" \
  "$COMMON_ENV" "$DB_SECRET"

upsert_job "rd-fetch-trends-b2" \
  "1" "1Gi" "3600s" "1" "/app/scripts/jobs/fetch_trends_batch2.sh" \
  "$COMMON_ENV" "$DB_SECRET"

upsert_job "rd-fetch-trends-b3" \
  "1" "1Gi" "3600s" "1" "/app/scripts/jobs/fetch_trends_batch3_merge.sh" \
  "$COMMON_ENV" "$DB_SECRET"

upsert_job "rd-fetch-stock" \
  "1" "1Gi" "1200s" "1" "/app/scripts/jobs/fetch_stock_data.sh" \
  "$COMMON_ENV" "$DB_SECRET"

upsert_job "rd-llm-enrich" \
  "1" "1Gi" "3600s" "1" "/app/scripts/jobs/llm_enrich.sh" \
  "${COMMON_ENV},LLM_PROVIDER=${LLM_PROVIDER},LLM_MODEL=${LLM_MODEL},LLM_MAX_CALLS=${LLM_MAX_CALLS}" \
  "${DB_SECRET},LLM_API_KEY=LLM_API_KEY:latest"

upsert_job "rd-aggregate-negative" \
  "1" "1Gi" "1500s" "1" "/app/scripts/jobs/aggregate_negative_articles.sh" \
  "$COMMON_ENV" "$DB_SECRET"

upsert_job "rd-recompute-serp-feature" \
  "1" "1Gi" "1800s" "1" "/app/scripts/jobs/recompute_serp_feature_daily.sh" \
  "$COMMON_ENV" "$DB_SECRET"

if is_true "$DEPLOY_REFRESH_MVS_JOB"; then
  upsert_job "rd-refresh-mvs" \
    "1" "1Gi" "1800s" "0" "/app/scripts/jobs/refresh_mvs.sh" \
    "$COMMON_ENV" "$DB_SECRET"
else
  echo "[INFO] Skipping rd-refresh-mvs deployment (DEPLOY_REFRESH_MVS_JOB=${DEPLOY_REFRESH_MVS_JOB})"
fi

if is_true "$DEPLOY_ALERT_JOBS"; then
  upsert_job "rd-send-crisis-alerts" \
    "1" "1Gi" "1800s" "0" "/app/scripts/jobs/send_crisis_alerts.sh" \
    "${COMMON_ENV},LLM_PROVIDER=${LLM_PROVIDER},LLM_MODEL=${LLM_MODEL},LLM_MAX_CALLS=${LLM_MAX_CALLS},LLM_SUMMARY_MAX_CALLS=${LLM_SUMMARY_MAX_CALLS}" \
    "${DB_SECRET},SF_USERNAME=SF_USERNAME:latest,SF_PASSWORD=SF_PASSWORD:latest,SF_SECURITY_TOKEN=SF_SECURITY_TOKEN:latest,SLACK_BOT_TOKEN=SLACK_BOT_TOKEN:latest,SLACK_ACTION_VALUE_SIGNING_SECRET=SLACK_ACTION_VALUE_SIGNING_SECRET:latest,LLM_API_KEY=LLM_API_KEY:latest"

  upsert_job "rd-send-targeted-alerts" \
    "1" "1Gi" "1200s" "0" "/app/scripts/jobs/send_targeted_alerts.sh" \
    "${COMMON_ENV},LLM_PROVIDER=${LLM_PROVIDER},LLM_MODEL=${LLM_MODEL},LLM_SUMMARY_MAX_CALLS=${LLM_SUMMARY_MAX_CALLS}" \
    "${DB_SECRET},SF_USERNAME=SF_USERNAME:latest,SF_PASSWORD=SF_PASSWORD:latest,SF_SECURITY_TOKEN=SF_SECURITY_TOKEN:latest,SLACK_BOT_TOKEN=SLACK_BOT_TOKEN:latest,SLACK_ACTION_VALUE_SIGNING_SECRET=SLACK_ACTION_VALUE_SIGNING_SECRET:latest,LLM_API_KEY=LLM_API_KEY:latest"
else
  echo "[INFO] Skipping alert jobs deployment (DEPLOY_ALERT_JOBS=${DEPLOY_ALERT_JOBS})"
fi

echo "[OK] Cloud Run jobs deployment complete."
