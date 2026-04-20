#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-}"
RUNS_METRIC_NAME="${RUNS_METRIC_NAME:-rd_job_runs_total}"
DURATION_METRIC_NAME="${DURATION_METRIC_NAME:-rd_job_duration_sec}"
DASHBOARD_DISPLAY_NAME="${DASHBOARD_DISPLAY_NAME:-Risk Dashboard Jobs Observability}"
RUNS_LOOKBACK="${RUNS_LOOKBACK:-24h}"

if [[ -z "$PROJECT_ID" ]]; then
  echo "[ERROR] PROJECT_ID is required." >&2
  exit 1
fi

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

RUNS_CFG="$TMP_DIR/${RUNS_METRIC_NAME}.json"
DURATION_CFG="$TMP_DIR/${DURATION_METRIC_NAME}.json"
DASHBOARD_CFG="$TMP_DIR/dashboard.json"

python3 - "$RUNS_CFG" "$RUNS_METRIC_NAME" <<'PY'
import json
import sys

path, metric_name = sys.argv[1], sys.argv[2]
config = {
    "name": metric_name,
    "description": "Count of Cloud Run job wrapper summaries by job and status.",
    "filter": 'resource.type="cloud_run_job" AND textPayload=~"\\[JOB_SUMMARY_JSON\\]"',
    "metricDescriptor": {
        "metricKind": "DELTA",
        "valueType": "INT64",
        "unit": "1",
        "labels": [
            {"key": "job", "valueType": "STRING", "description": "Cloud Run job wrapper name"},
            {"key": "status", "valueType": "STRING", "description": "Job status from summary payload"},
        ],
    },
    "labelExtractors": {
        "job": 'REGEXP_EXTRACT(textPayload, "\\"job\\": \\"([^\\"]+)\\"")',
        "status": 'REGEXP_EXTRACT(textPayload, "\\"status\\": \\"([^\\"]+)\\"")',
    },
}
with open(path, "w", encoding="utf-8") as fh:
    json.dump(config, fh, indent=2)
PY

python3 - "$DURATION_CFG" "$DURATION_METRIC_NAME" <<'PY'
import json
import sys

path, metric_name = sys.argv[1], sys.argv[2]
config = {
    "name": metric_name,
    "description": "Cloud Run job wrapper duration in seconds, extracted from JOB_SUMMARY_JSON.",
    "filter": 'resource.type="cloud_run_job" AND textPayload=~"\\[JOB_SUMMARY_JSON\\]"',
    "valueExtractor": 'REGEXP_EXTRACT(textPayload, "\\"duration_sec\\": ([0-9]+)")',
    "metricDescriptor": {
        "metricKind": "DELTA",
        "valueType": "DISTRIBUTION",
        "unit": "s",
        "labels": [
            {"key": "job", "valueType": "STRING", "description": "Cloud Run job wrapper name"},
            {"key": "status", "valueType": "STRING", "description": "Job status from summary payload"},
        ],
    },
    "labelExtractors": {
        "job": 'REGEXP_EXTRACT(textPayload, "\\"job\\": \\"([^\\"]+)\\"")',
        "status": 'REGEXP_EXTRACT(textPayload, "\\"status\\": \\"([^\\"]+)\\"")',
    },
    "bucketOptions": {
        "exponentialBuckets": {
            "numFiniteBuckets": 20,
            "growthFactor": 1.6,
            "scale": 1.0,
        }
    },
}
with open(path, "w", encoding="utf-8") as fh:
    json.dump(config, fh, indent=2)
PY

upsert_metric() {
  local name="$1"
  local cfg="$2"

  if gcloud logging metrics describe "$name" --project "$PROJECT_ID" >/dev/null 2>&1; then
    echo "[INFO] Updating logs-based metric: ${name}"
    gcloud logging metrics update "$name" \
      --project "$PROJECT_ID" \
      --config-from-file "$cfg"
  else
    echo "[INFO] Creating logs-based metric: ${name}"
    gcloud logging metrics create "$name" \
      --project "$PROJECT_ID" \
      --config-from-file "$cfg"
  fi
}

upsert_metric "$RUNS_METRIC_NAME" "$RUNS_CFG"
upsert_metric "$DURATION_METRIC_NAME" "$DURATION_CFG"

RUNS_METRIC_TYPE="logging.googleapis.com/user/${RUNS_METRIC_NAME}"
DURATION_METRIC_TYPE="logging.googleapis.com/user/${DURATION_METRIC_NAME}"

DASHBOARD_NAME="$(
  gcloud monitoring dashboards list \
    --project "$PROJECT_ID" \
    --format json \
    | python3 -c '
import json
import sys

target = sys.argv[1]
raw = sys.stdin.read().strip()
dashboards = json.loads(raw) if raw else []
for item in dashboards:
    if item.get("displayName") == target:
        print(item.get("name", ""))
        break
' "$DASHBOARD_DISPLAY_NAME"
)"

DASHBOARD_ETAG=""
if [[ -n "$DASHBOARD_NAME" ]]; then
  DASHBOARD_ETAG="$(
    gcloud monitoring dashboards describe "$DASHBOARD_NAME" \
      --project "$PROJECT_ID" \
      --format "value(etag)"
  )"
fi

python3 - "$DASHBOARD_CFG" "$DASHBOARD_DISPLAY_NAME" "$RUNS_METRIC_TYPE" "$DURATION_METRIC_TYPE" "$DASHBOARD_ETAG" "$RUNS_LOOKBACK" <<'PY'
import json
import sys

path, display_name, runs_metric, duration_metric, etag, runs_lookback = sys.argv[1:]

dashboard = {
    "displayName": display_name,
    "gridLayout": {
        "columns": "2",
        "widgets": [
            {
                "title": f"Job Runs By Status ({runs_lookback})",
                "xyChart": {
                    "chartOptions": {"mode": "COLOR"},
                    "dataSets": [
                        {
                            "legendTemplate": "${metric.labels.job} ${metric.labels.status}",
                            "minAlignmentPeriod": "300s",
                            "plotType": "STACKED_BAR",
                            "targetAxis": "Y1",
                            "timeSeriesQuery": {
                                "timeSeriesFilter": {
                                    "filter": f'metric.type="{runs_metric}" AND resource.type="cloud_run_job"',
                                    "aggregation": {
                                        "alignmentPeriod": "300s",
                                        "perSeriesAligner": "ALIGN_SUM",
                                        "crossSeriesReducer": "REDUCE_SUM",
                                        "groupByFields": ["metric.label.job", "metric.label.status"],
                                    },
                                }
                            },
                        }
                    ],
                    "timeshiftDuration": "0s",
                    "yAxis": {"label": "runs", "scale": "LINEAR"},
                },
            },
            {
                "title": f"Failed Runs By Job ({runs_lookback})",
                "xyChart": {
                    "chartOptions": {"mode": "COLOR"},
                    "dataSets": [
                        {
                            "legendTemplate": "${metric.labels.job}",
                            "minAlignmentPeriod": "300s",
                            "plotType": "LINE",
                            "targetAxis": "Y1",
                            "timeSeriesQuery": {
                                "timeSeriesFilter": {
                                    "filter": (
                                        f'metric.type="{runs_metric}" '
                                        'AND resource.type="cloud_run_job" '
                                        'AND metric.labels.status="failed"'
                                    ),
                                    "aggregation": {
                                        "alignmentPeriod": "300s",
                                        "perSeriesAligner": "ALIGN_SUM",
                                        "crossSeriesReducer": "REDUCE_SUM",
                                        "groupByFields": ["metric.label.job"],
                                    },
                                }
                            },
                        }
                    ],
                    "timeshiftDuration": "0s",
                    "yAxis": {"label": "failed runs", "scale": "LINEAR"},
                },
            },
            {
                "title": "P95 Duration By Job (Success Only)",
                "xyChart": {
                    "chartOptions": {"mode": "COLOR"},
                    "dataSets": [
                        {
                            "legendTemplate": "${metric.labels.job}",
                            "minAlignmentPeriod": "300s",
                            "plotType": "LINE",
                            "targetAxis": "Y1",
                            "timeSeriesQuery": {
                                "timeSeriesFilter": {
                                    "filter": (
                                        f'metric.type="{duration_metric}" '
                                        'AND resource.type="cloud_run_job" '
                                        'AND metric.labels.status="success"'
                                    ),
                                    "aggregation": {
                                        "alignmentPeriod": "300s",
                                        "perSeriesAligner": "ALIGN_PERCENTILE_95",
                                        "crossSeriesReducer": "REDUCE_MEAN",
                                        "groupByFields": ["metric.label.job"],
                                    },
                                }
                            },
                        }
                    ],
                    "timeshiftDuration": "0s",
                    "yAxis": {"label": "seconds", "scale": "LINEAR"},
                },
            },
        ],
    },
}

if etag:
    dashboard["etag"] = etag

with open(path, "w", encoding="utf-8") as fh:
    json.dump(dashboard, fh, indent=2)
PY

if [[ -n "$DASHBOARD_NAME" ]]; then
  echo "[INFO] Updating dashboard: ${DASHBOARD_NAME}"
  gcloud monitoring dashboards update "$DASHBOARD_NAME" \
    --project "$PROJECT_ID" \
    --config-from-file "$DASHBOARD_CFG"
else
  echo "[INFO] Creating dashboard: ${DASHBOARD_DISPLAY_NAME}"
  gcloud monitoring dashboards create \
    --project "$PROJECT_ID" \
    --config-from-file "$DASHBOARD_CFG"
fi

echo "[OK] Job observability setup complete."
echo "[INFO] Dashboard display name: ${DASHBOARD_DISPLAY_NAME}"
echo "[INFO] Metrics: ${RUNS_METRIC_NAME}, ${DURATION_METRIC_NAME}"
