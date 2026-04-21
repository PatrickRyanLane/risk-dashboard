#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Run Cloud Run jobs with workflow-like flags.

Usage:
  scripts/run_cloud_job.sh <job> [options]

Job aliases:
  daily-brands | rd-daily-brands
  daily-ceos | rd-daily-ceos
  serp-features | rd-serp-features
  fetch-stock | rd-fetch-stock
  fetch-trends-b1 | rd-fetch-trends-b1
  fetch-trends-b2 | rd-fetch-trends-b2
  fetch-trends-b3 | rd-fetch-trends-b3
  fetch-trends | rd-fetch-trends   (requires --batch 1|2|3)
  llm-enrich | rd-llm-enrich
  aggregate-negative | rd-aggregate-negative
  recompute-serp-feature | rd-recompute-serp-feature
  refresh-mvs | rd-refresh-mvs
  send-crisis-alerts | rd-send-crisis-alerts
  send-targeted-alerts | rd-send-targeted-alerts

Global options:
  --project <id>              GCP project (default: gcloud core/project)
  --region <region>           Region (default: us-east1)
  --wait                      Wait for completion (default)
  --async                     Return after execution starts
  --dry-run                   Print command only
  --env KEY=VALUE             Add/override any env var (repeatable)

Common convenience flags:
  --date YYYY-MM-DD           RUN_DATE
  --run-days N                RUN_DAYS
  --batch N                   For fetch-trends alias only (1,2,3)
  --gcs-bucket NAME           GCS_BUCKET
  --brands-batch-size N       BRANDS_BATCH_SIZE
  --ceos-batch-size N         CEOS_BATCH_SIZE
  --articles-max-per-alias N  ARTICLES_MAX_PER_ALIAS
  --articles-sleep-sec N      ARTICLES_SLEEP_SEC
  --trends-batch-size N       TRENDS_BATCH_SIZE
  --stock-batch-size N        STOCK_BATCH_SIZE
  --llm-max-calls N           LLM_MAX_OVERRIDE
  --llm-batch-size N          LLM_BATCH_SIZE
  --llm-provider NAME         LLM_PROVIDER
  --llm-model NAME            LLM_MODEL
  --negative-days-back N      NEGATIVE_SUMMARY_DAYS_BACK
  --recompute-days N          RECOMPUTE_DAYS
  --force-brands CSV          FORCE_BRANDS
  --force-send 0|1            FORCE_SEND
  --target-brands CSV         TARGET_BRANDS
  --target-dry-run 0|1        DRY_RUN
  --dashboard-base-url URL    DASHBOARD_BASE_URL

Examples:
  scripts/run_cloud_job.sh daily-brands --date 2026-04-17 --wait
  scripts/run_cloud_job.sh llm-enrich --llm-max-calls 400 --wait
  scripts/run_cloud_job.sh send-crisis-alerts --force-brands 'Fanatics,Nike' --force-send 1 --wait
  scripts/run_cloud_job.sh send-targeted-alerts --target-brands 'Fanatics,Nike' --target-dry-run 1 --wait
  scripts/run_cloud_job.sh fetch-trends --batch 3 --wait
EOF
}

die() {
  echo "[ERROR] $*" >&2
  exit 1
}

PROJECT_ID=""
REGION="us-east1"
WAIT_FLAG="--wait"
DRY_RUN="false"
JOB_INPUT=""

BATCH_OVERRIDE=""
DATE_OVERRIDE=""
RUN_DAYS_OVERRIDE=""
GCS_BUCKET_OVERRIDE=""
BRANDS_BATCH_SIZE_OVERRIDE=""
CEOS_BATCH_SIZE_OVERRIDE=""
ARTICLES_MAX_PER_ALIAS_OVERRIDE=""
ARTICLES_SLEEP_SEC_OVERRIDE=""
TRENDS_BATCH_SIZE_OVERRIDE=""
STOCK_BATCH_SIZE_OVERRIDE=""
LLM_MAX_CALLS_OVERRIDE=""
LLM_BATCH_SIZE_OVERRIDE=""
LLM_PROVIDER_OVERRIDE=""
LLM_MODEL_OVERRIDE=""
NEGATIVE_DAYS_BACK_OVERRIDE=""
RECOMPUTE_DAYS_OVERRIDE=""
FORCE_BRANDS_OVERRIDE=""
FORCE_SEND_OVERRIDE=""
TARGET_BRANDS_OVERRIDE=""
TARGET_DRY_RUN_OVERRIDE=""
DASHBOARD_BASE_URL_OVERRIDE=""

ENV_OVERRIDES=()

add_env_override() {
  local kv="$1"
  local key="${kv%%=*}"
  local value
  value="${kv#*=}"

  if [[ -z "$key" || "$key" == "$kv" ]]; then
    die "Invalid --env value '$kv' (expected KEY=VALUE)"
  fi

  local updated="false"
  local new_overrides=()
  local existing existing_key
  if [[ "${#ENV_OVERRIDES[@]}" -gt 0 ]]; then
    for existing in "${ENV_OVERRIDES[@]}"; do
      existing_key="${existing%%=*}"
      if [[ "$existing_key" == "$key" ]]; then
        if [[ "$updated" == "false" ]]; then
          new_overrides+=("${key}=${value}")
          updated="true"
        fi
      else
        new_overrides+=("$existing")
      fi
    done
  fi
  if [[ "$updated" == "false" ]]; then
    new_overrides+=("${key}=${value}")
  fi
  ENV_OVERRIDES=("${new_overrides[@]}")
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --project)
      PROJECT_ID="$2"
      shift 2
      ;;
    --region)
      REGION="$2"
      shift 2
      ;;
    --wait)
      WAIT_FLAG="--wait"
      shift
      ;;
    --async)
      WAIT_FLAG="--async"
      shift
      ;;
    --dry-run)
      DRY_RUN="true"
      shift
      ;;
    --env)
      add_env_override "$2"
      shift 2
      ;;
    --env=*)
      add_env_override "${1#*=}"
      shift
      ;;
    --date)
      DATE_OVERRIDE="$2"
      shift 2
      ;;
    --run-days)
      RUN_DAYS_OVERRIDE="$2"
      shift 2
      ;;
    --batch)
      BATCH_OVERRIDE="$2"
      shift 2
      ;;
    --gcs-bucket)
      GCS_BUCKET_OVERRIDE="$2"
      shift 2
      ;;
    --brands-batch-size)
      BRANDS_BATCH_SIZE_OVERRIDE="$2"
      shift 2
      ;;
    --ceos-batch-size)
      CEOS_BATCH_SIZE_OVERRIDE="$2"
      shift 2
      ;;
    --articles-max-per-alias)
      ARTICLES_MAX_PER_ALIAS_OVERRIDE="$2"
      shift 2
      ;;
    --articles-sleep-sec)
      ARTICLES_SLEEP_SEC_OVERRIDE="$2"
      shift 2
      ;;
    --trends-batch-size)
      TRENDS_BATCH_SIZE_OVERRIDE="$2"
      shift 2
      ;;
    --stock-batch-size)
      STOCK_BATCH_SIZE_OVERRIDE="$2"
      shift 2
      ;;
    --llm-max-calls)
      LLM_MAX_CALLS_OVERRIDE="$2"
      shift 2
      ;;
    --llm-batch-size)
      LLM_BATCH_SIZE_OVERRIDE="$2"
      shift 2
      ;;
    --llm-provider)
      LLM_PROVIDER_OVERRIDE="$2"
      shift 2
      ;;
    --llm-model)
      LLM_MODEL_OVERRIDE="$2"
      shift 2
      ;;
    --negative-days-back)
      NEGATIVE_DAYS_BACK_OVERRIDE="$2"
      shift 2
      ;;
    --recompute-days)
      RECOMPUTE_DAYS_OVERRIDE="$2"
      shift 2
      ;;
    --force-brands)
      FORCE_BRANDS_OVERRIDE="$2"
      shift 2
      ;;
    --force-send)
      FORCE_SEND_OVERRIDE="$2"
      shift 2
      ;;
    --target-brands)
      TARGET_BRANDS_OVERRIDE="$2"
      shift 2
      ;;
    --target-dry-run)
      TARGET_DRY_RUN_OVERRIDE="$2"
      shift 2
      ;;
    --dashboard-base-url)
      DASHBOARD_BASE_URL_OVERRIDE="$2"
      shift 2
      ;;
    -*)
      die "Unknown option: $1"
      ;;
    *)
      if [[ -z "$JOB_INPUT" ]]; then
        JOB_INPUT="$1"
      else
        die "Unexpected extra positional argument: $1"
      fi
      shift
      ;;
  esac
done

[[ -n "$JOB_INPUT" ]] || die "Missing job name. Run with --help."

if [[ -z "$PROJECT_ID" ]]; then
  PROJECT_ID="$(gcloud config get-value project 2>/dev/null || true)"
fi
[[ -n "$PROJECT_ID" ]] || die "Project not set. Use --project or set gcloud core/project."

normalize_job() {
  local input="$1"
  case "$input" in
    daily-brands|rd-daily-brands) echo "rd-daily-brands" ;;
    daily-ceos|rd-daily-ceos) echo "rd-daily-ceos" ;;
    serp-features|rd-serp-features) echo "rd-serp-features" ;;
    fetch-stock|rd-fetch-stock) echo "rd-fetch-stock" ;;
    fetch-trends-b1|rd-fetch-trends-b1) echo "rd-fetch-trends-b1" ;;
    fetch-trends-b2|rd-fetch-trends-b2) echo "rd-fetch-trends-b2" ;;
    fetch-trends-b3|rd-fetch-trends-b3) echo "rd-fetch-trends-b3" ;;
    fetch-trends|rd-fetch-trends)
      case "$BATCH_OVERRIDE" in
        1) echo "rd-fetch-trends-b1" ;;
        2) echo "rd-fetch-trends-b2" ;;
        3) echo "rd-fetch-trends-b3" ;;
        *) die "--batch must be 1, 2, or 3 when using fetch-trends alias." ;;
      esac
      ;;
    llm-enrich|rd-llm-enrich) echo "rd-llm-enrich" ;;
    aggregate-negative|rd-aggregate-negative) echo "rd-aggregate-negative" ;;
    recompute-serp-feature|rd-recompute-serp-feature) echo "rd-recompute-serp-feature" ;;
    refresh-mvs|rd-refresh-mvs) echo "rd-refresh-mvs" ;;
    send-crisis-alerts|rd-send-crisis-alerts) echo "rd-send-crisis-alerts" ;;
    send-targeted-alerts|rd-send-targeted-alerts) echo "rd-send-targeted-alerts" ;;
    *)
      die "Unknown job alias '$input'. Run with --help."
      ;;
  esac
}

JOB_NAME="$(normalize_job "$JOB_INPUT")"

[[ -n "$DATE_OVERRIDE" ]] && add_env_override "RUN_DATE=${DATE_OVERRIDE}"
[[ -n "$RUN_DAYS_OVERRIDE" ]] && add_env_override "RUN_DAYS=${RUN_DAYS_OVERRIDE}"
[[ -n "$GCS_BUCKET_OVERRIDE" ]] && add_env_override "GCS_BUCKET=${GCS_BUCKET_OVERRIDE}"
[[ -n "$BRANDS_BATCH_SIZE_OVERRIDE" ]] && add_env_override "BRANDS_BATCH_SIZE=${BRANDS_BATCH_SIZE_OVERRIDE}"
[[ -n "$CEOS_BATCH_SIZE_OVERRIDE" ]] && add_env_override "CEOS_BATCH_SIZE=${CEOS_BATCH_SIZE_OVERRIDE}"
[[ -n "$ARTICLES_MAX_PER_ALIAS_OVERRIDE" ]] && add_env_override "ARTICLES_MAX_PER_ALIAS=${ARTICLES_MAX_PER_ALIAS_OVERRIDE}"
[[ -n "$ARTICLES_SLEEP_SEC_OVERRIDE" ]] && add_env_override "ARTICLES_SLEEP_SEC=${ARTICLES_SLEEP_SEC_OVERRIDE}"
[[ -n "$TRENDS_BATCH_SIZE_OVERRIDE" ]] && add_env_override "TRENDS_BATCH_SIZE=${TRENDS_BATCH_SIZE_OVERRIDE}"
[[ -n "$STOCK_BATCH_SIZE_OVERRIDE" ]] && add_env_override "STOCK_BATCH_SIZE=${STOCK_BATCH_SIZE_OVERRIDE}"
[[ -n "$LLM_MAX_CALLS_OVERRIDE" ]] && add_env_override "LLM_MAX_OVERRIDE=${LLM_MAX_CALLS_OVERRIDE}"
[[ -n "$LLM_BATCH_SIZE_OVERRIDE" ]] && add_env_override "LLM_BATCH_SIZE=${LLM_BATCH_SIZE_OVERRIDE}"
[[ -n "$LLM_PROVIDER_OVERRIDE" ]] && add_env_override "LLM_PROVIDER=${LLM_PROVIDER_OVERRIDE}"
[[ -n "$LLM_MODEL_OVERRIDE" ]] && add_env_override "LLM_MODEL=${LLM_MODEL_OVERRIDE}"
[[ -n "$NEGATIVE_DAYS_BACK_OVERRIDE" ]] && add_env_override "NEGATIVE_SUMMARY_DAYS_BACK=${NEGATIVE_DAYS_BACK_OVERRIDE}"
[[ -n "$RECOMPUTE_DAYS_OVERRIDE" ]] && add_env_override "RECOMPUTE_DAYS=${RECOMPUTE_DAYS_OVERRIDE}"
[[ -n "$FORCE_BRANDS_OVERRIDE" ]] && add_env_override "FORCE_BRANDS=${FORCE_BRANDS_OVERRIDE}"
[[ -n "$FORCE_SEND_OVERRIDE" ]] && add_env_override "FORCE_SEND=${FORCE_SEND_OVERRIDE}"
[[ -n "$TARGET_BRANDS_OVERRIDE" ]] && add_env_override "TARGET_BRANDS=${TARGET_BRANDS_OVERRIDE}"
[[ -n "$TARGET_DRY_RUN_OVERRIDE" ]] && add_env_override "DRY_RUN=${TARGET_DRY_RUN_OVERRIDE}"
[[ -n "$DASHBOARD_BASE_URL_OVERRIDE" ]] && add_env_override "DASHBOARD_BASE_URL=${DASHBOARD_BASE_URL_OVERRIDE}"

build_env_update_arg() {
  local delim="__ENV__"
  local kv=""
  local conflict="true"
  while [[ "$conflict" == "true" ]]; do
    conflict="false"
    if [[ "${#ENV_OVERRIDES[@]}" -gt 0 ]]; then
      for kv in "${ENV_OVERRIDES[@]}"; do
        if [[ "$kv" == *"$delim"* ]]; then
          conflict="true"
          break
        fi
      done
    fi
    if [[ "$conflict" == "true" ]]; then
      delim="${delim}_X"
    fi
  done

  local joined=""
  local i=0
  if [[ "${#ENV_OVERRIDES[@]}" -gt 0 ]]; then
    for kv in "${ENV_OVERRIDES[@]}"; do
      if [[ "$i" -eq 0 ]]; then
        joined="$kv"
      else
        joined="${joined}${delim}${kv}"
      fi
      i=$((i + 1))
    done
  fi

  printf '^%s^%s' "$delim" "$joined"
}

CMD=(
  gcloud run jobs execute "$JOB_NAME"
  --project "$PROJECT_ID"
  --region "$REGION"
  "$WAIT_FLAG"
)

if [[ "${#ENV_OVERRIDES[@]}" -gt 0 ]]; then
  CMD+=(--update-env-vars "$(build_env_update_arg)")
fi

echo "[INFO] Project: $PROJECT_ID"
echo "[INFO] Region: $REGION"
echo "[INFO] Job: $JOB_NAME"
if [[ "${#ENV_OVERRIDES[@]}" -gt 0 ]]; then
  echo "[INFO] Env overrides:"
  for kv in "${ENV_OVERRIDES[@]}"; do
    echo "  - $kv"
  done
fi

if [[ "$DRY_RUN" == "true" ]]; then
  echo "[DRY RUN] Command:"
  printf ' %q' "${CMD[@]}"
  echo
  exit 0
fi

"${CMD[@]}"
