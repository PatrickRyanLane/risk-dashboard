#!/usr/bin/env bash
set -euo pipefail

# Deploys the Slack -> Salesforce interactivity webhook to Cloud Run.
# Defaults target Patrick's project, but can be overridden via env vars.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

PROJECT_ID="${PROJECT_ID:-gen-lang-client-0154760958}"
REGION="${REGION:-us-west1}"
SERVICE="${SERVICE:-risk-slack-actions}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
IMAGE="gcr.io/${PROJECT_ID}/${SERVICE}:${IMAGE_TAG}"
DOCKERFILE="${DOCKERFILE:-Dockerfile.slack-actions}"

# Secret names in Secret Manager (not secret values).
SECRET_SLACK_SIGNING="${SECRET_SLACK_SIGNING:-SLACK_SIGNING_SECRET}"
SECRET_SLACK_ACTION_VALUE_SIGNING="${SECRET_SLACK_ACTION_VALUE_SIGNING:-SLACK_ACTION_VALUE_SIGNING_SECRET}"
SECRET_DATABASE_URL="${SECRET_DATABASE_URL:-DATABASE_URL}"
SECRET_SF_USERNAME="${SECRET_SF_USERNAME:-SF_USERNAME}"
SECRET_SF_PASSWORD="${SECRET_SF_PASSWORD:-SF_PASSWORD}"
SECRET_SF_SECURITY_TOKEN="${SECRET_SF_SECURITY_TOKEN:-SF_SECURITY_TOKEN}"

# Non-sensitive runtime config.
EXPECTED_ACTION_BLOCK_ID="${EXPECTED_ACTION_BLOCK_ID:-crisis_alert_actions_v1}"
MAX_SLACK_BODY_BYTES="${MAX_SLACK_BODY_BYTES:-200000}"
OUTREACH_TASK_SUBJECT="${OUTREACH_TASK_SUBJECT:-Crisis Outreach Draft Review}"
OUTREACH_TASK_STATUS="${OUTREACH_TASK_STATUS:-Open}"
OUTREACH_TASK_PRIORITY="${OUTREACH_TASK_PRIORITY:-High}"
OUTREACH_TASK_DUE_DAYS="${OUTREACH_TASK_DUE_DAYS:-1}"
OUTREACH_LEAD_STATUS="${OUTREACH_LEAD_STATUS:-Cold Outreach}"
OUTREACH_LEAD_LAST_NAME_FALLBACK="${OUTREACH_LEAD_LAST_NAME_FALLBACK:-Communications}"

# Optional allowlists (recommended).
SLACK_ALLOWED_TEAM_IDS="${SLACK_ALLOWED_TEAM_IDS:-}"
SLACK_ALLOWED_APP_IDS="${SLACK_ALLOWED_APP_IDS:-}"
SLACK_ALLOWED_CHANNEL_IDS="${SLACK_ALLOWED_CHANNEL_IDS:-}"
SLACK_ALLOWED_USER_IDS="${SLACK_ALLOWED_USER_IDS:-}"
SLACK_EXPECTED_BOT_ID="${SLACK_EXPECTED_BOT_ID:-}"
OUTREACH_TASK_OWNER_ID="${OUTREACH_TASK_OWNER_ID:-}"
OUTREACH_LEAD_OWNER_ID="${OUTREACH_LEAD_OWNER_ID:-}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1"
    exit 1
  fi
}

require_secret() {
  local secret_name="$1"
  if ! gcloud secrets describe "$secret_name" --project "$PROJECT_ID" >/dev/null 2>&1; then
    echo "Missing secret in project ${PROJECT_ID}: ${secret_name}"
    exit 1
  fi
}

join_csv() {
  local IFS=","
  echo "$*"
}

require_cmd gcloud

if [[ ! -f "$DOCKERFILE" ]]; then
  echo "Dockerfile not found: ${DOCKERFILE}"
  exit 1
fi

ACTIVE_ACCOUNT="$(gcloud auth list --filter=status:ACTIVE --format='value(account)' | head -n1 || true)"
if [[ -z "${ACTIVE_ACCOUNT}" ]]; then
  echo "No active gcloud account. Run: gcloud auth login"
  exit 1
fi

echo "Using gcloud account: ${ACTIVE_ACCOUNT}"
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Service: ${SERVICE}"
echo "Image: ${IMAGE}"

require_secret "$SECRET_SLACK_SIGNING"
require_secret "$SECRET_SLACK_ACTION_VALUE_SIGNING"
require_secret "$SECRET_DATABASE_URL"
require_secret "$SECRET_SF_USERNAME"
require_secret "$SECRET_SF_PASSWORD"
require_secret "$SECRET_SF_SECURITY_TOKEN"

gcloud config set project "$PROJECT_ID" >/dev/null

echo "Building container image..."
TMP_CLOUDBUILD="$(mktemp)"
trap 'rm -f "$TMP_CLOUDBUILD"' EXIT
cat > "$TMP_CLOUDBUILD" <<EOF
steps:
  - name: gcr.io/cloud-builders/docker
    args: ["build", "-f", "${DOCKERFILE}", "-t", "${IMAGE}", "."]
images:
  - "${IMAGE}"
EOF
gcloud builds submit \
  --project "$PROJECT_ID" \
  --config "$TMP_CLOUDBUILD" \
  .

SECRETS_ARG="$(join_csv \
  "SLACK_SIGNING_SECRET=${SECRET_SLACK_SIGNING}:latest" \
  "SLACK_ACTION_VALUE_SIGNING_SECRET=${SECRET_SLACK_ACTION_VALUE_SIGNING}:latest" \
  "DATABASE_URL=${SECRET_DATABASE_URL}:latest" \
  "SF_USERNAME=${SECRET_SF_USERNAME}:latest" \
  "SF_PASSWORD=${SECRET_SF_PASSWORD}:latest" \
  "SF_SECURITY_TOKEN=${SECRET_SF_SECURITY_TOKEN}:latest")"

ENV_KVS=(
  "EXPECTED_ACTION_BLOCK_ID=${EXPECTED_ACTION_BLOCK_ID}"
  "MAX_SLACK_BODY_BYTES=${MAX_SLACK_BODY_BYTES}"
  "OUTREACH_TASK_SUBJECT=${OUTREACH_TASK_SUBJECT}"
  "OUTREACH_TASK_STATUS=${OUTREACH_TASK_STATUS}"
  "OUTREACH_TASK_PRIORITY=${OUTREACH_TASK_PRIORITY}"
  "OUTREACH_TASK_DUE_DAYS=${OUTREACH_TASK_DUE_DAYS}"
  "OUTREACH_LEAD_STATUS=${OUTREACH_LEAD_STATUS}"
  "OUTREACH_LEAD_LAST_NAME_FALLBACK=${OUTREACH_LEAD_LAST_NAME_FALLBACK}"
)

if [[ -n "$SLACK_ALLOWED_TEAM_IDS" ]]; then
  ENV_KVS+=("SLACK_ALLOWED_TEAM_IDS=${SLACK_ALLOWED_TEAM_IDS}")
fi
if [[ -n "$SLACK_ALLOWED_APP_IDS" ]]; then
  ENV_KVS+=("SLACK_ALLOWED_APP_IDS=${SLACK_ALLOWED_APP_IDS}")
fi
if [[ -n "$SLACK_ALLOWED_CHANNEL_IDS" ]]; then
  ENV_KVS+=("SLACK_ALLOWED_CHANNEL_IDS=${SLACK_ALLOWED_CHANNEL_IDS}")
fi
if [[ -n "$SLACK_ALLOWED_USER_IDS" ]]; then
  ENV_KVS+=("SLACK_ALLOWED_USER_IDS=${SLACK_ALLOWED_USER_IDS}")
fi
if [[ -n "$SLACK_EXPECTED_BOT_ID" ]]; then
  ENV_KVS+=("SLACK_EXPECTED_BOT_ID=${SLACK_EXPECTED_BOT_ID}")
fi
if [[ -n "$OUTREACH_TASK_OWNER_ID" ]]; then
  ENV_KVS+=("OUTREACH_TASK_OWNER_ID=${OUTREACH_TASK_OWNER_ID}")
fi
if [[ -n "$OUTREACH_LEAD_OWNER_ID" ]]; then
  ENV_KVS+=("OUTREACH_LEAD_OWNER_ID=${OUTREACH_LEAD_OWNER_ID}")
fi

ENV_VARS_ARG="$(join_csv "${ENV_KVS[@]}")"

echo "Deploying Cloud Run service..."
gcloud run deploy "$SERVICE" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --image "$IMAGE" \
  --allow-unauthenticated \
  --update-secrets "$SECRETS_ARG" \
  --update-env-vars "$ENV_VARS_ARG"

SERVICE_URL="$(gcloud run services describe "$SERVICE" --project "$PROJECT_ID" --region "$REGION" --format='value(status.url)')"
echo ""
echo "Deployed: ${SERVICE_URL}"
echo "Slack interactivity URL:"
echo "${SERVICE_URL}/slack/interactions"
