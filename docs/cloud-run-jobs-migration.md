# Cloud Run Jobs + Scheduler Deployment

This repo includes scripts to deploy the pipeline from GitHub Actions to Cloud Run Jobs + Cloud Scheduler.

## What this deploys

Scheduled jobs (right-sized defaults):

- `rd-daily-brands` (1 vCPU / 2 GiB / 120m)
- `rd-daily-ceos` (1 vCPU / 2 GiB / 120m)
- `rd-serp-features` (2 vCPU / 4 GiB / 90m)
- `rd-fetch-stock` (1 vCPU / 1 GiB / 20m)
- `rd-fetch-trends-b1` (1 vCPU / 1 GiB / 60m)
- `rd-fetch-trends-b2` (1 vCPU / 1 GiB / 60m)
- `rd-fetch-trends-b3` (1 vCPU / 1 GiB / 60m; includes merge)
- `rd-llm-enrich` (1 vCPU / 1 GiB / 60m)
- `rd-aggregate-negative` (1 vCPU / 1 GiB / 25m)
- `rd-recompute-serp-feature` (1 vCPU / 1 GiB / 30m)
- `rd-refresh-mvs` (1 vCPU / 1 GiB / 30m; optional schedule)
- `rd-send-crisis-alerts` (1 vCPU / 1 GiB / 30m; no retries)
- `rd-send-targeted-alerts` (1 vCPU / 1 GiB / 20m; no retries)

## Files

- `Dockerfile.jobs`: shared image for all batch jobs
- `cloudbuild.jobs.yaml`: Cloud Build config that builds `Dockerfile.jobs`
- `scripts/jobs/*.sh`: per-job entry scripts
- `scripts/deploy_cloud_run_jobs.sh`: build image + create/update Cloud Run Jobs
- `scripts/deploy_cloud_scheduler_jobs.sh`: create/update scheduler triggers
- `scripts/run_cloud_job.sh`: helper to execute jobs with workflow-like runtime flags
- `docs/cloud-run-job-flags.md`: full runtime flags reference

## Required Secret Manager secrets

At minimum:

- `DATABASE_URL`
- `LLM_API_KEY` (for LLM jobs)

For alert jobs:

- `SF_USERNAME`
- `SF_PASSWORD`
- `SF_SECURITY_TOKEN`
- `SLACK_BOT_TOKEN`
- `SLACK_ACTION_VALUE_SIGNING_SECRET`

Optional (recommended for Okta/SSO orgs, JWT auth):

- `SF_CLIENT_ID` (Salesforce Connected App consumer key)
- `SF_JWT_PRIVATE_KEY` (PEM private key) or `SF_JWT_PRIVATE_KEY_B64` (base64 PEM)

## Required IAM

Job runtime service account (`JOB_SERVICE_ACCOUNT`) should be able to:

- run jobs with that service account
- access Secret Manager secrets used by jobs
- access GCS bucket data
  - needs `storage.buckets.get` on `gs://risk-dashboard` (for bucket existence checks)
  - needs object read/write on job paths (for CSV writes/reads)
- access Cloud SQL / Postgres endpoint as needed

Scheduler service account (`SCHEDULER_SERVICE_ACCOUNT`) should be able to execute Cloud Run jobs:

- grant a role containing `run.jobs.run` on the target jobs/project

## Deploy

```bash
cd /Users/plane/Documents/GitHub/risk-dashboard

export PROJECT_ID="your-gcp-project"
export REGION="us-east1"
export AR_LOCATION="us-east1"
export AR_REPO="risk-dashboard-jobs"
export JOB_SERVICE_ACCOUNT="risk-dashboard-jobs@${PROJECT_ID}.iam.gserviceaccount.com"
export SCHEDULER_SERVICE_ACCOUNT="risk-dashboard-scheduler@${PROJECT_ID}.iam.gserviceaccount.com"

# Optional toggles
export DEPLOY_ALERT_JOBS="true"
export DEPLOY_REFRESH_MVS_JOB="true"
export DEPLOY_REFRESH_MVS_SCHEDULE="true"
export SKIP_BUILD="false"  # set true to reuse an existing IMAGE_TAG

./scripts/deploy_cloud_run_jobs.sh
./scripts/deploy_cloud_scheduler_jobs.sh
```

## Execute a job manually

```bash
gcloud run jobs execute rd-daily-brands \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --wait
```

## Estimated monthly cost (us-east1)

Using your reported workload of about `220` runtime minutes/day from GitHub Actions:

- Cloud Run Jobs (lean): about `$2.81/mo` (1.0 vCPU average, 1.0 GiB average)
- Cloud Run Jobs (base): about `$3.61/mo` (1.1 vCPU average, 1.25 GiB average)
- Cloud Run Jobs (high): about `$5.95/mo` (1.4 vCPU average, 1.5 GiB average)

Plus:

- Cloud Scheduler: `$0.10/job/month` after first 3 jobs per billing account
  - With 13 schedules enabled: about `$1.00/mo`
  - With 11 schedules enabled (no refresh-mvs, no targeted): about `$0.80/mo`
- Artifact Registry image storage: first `0.5 GB` free, then `$0.10/GB-month`

These estimates exclude outbound internet egress and any downstream API service charges.

## Right-size recommendation

Start with the current defaults in `scripts/deploy_cloud_run_jobs.sh`, then tune down over 1 week:

- if p95 runtime is below 50% of timeout and memory peak is below 60%, reduce timeout first
- keep `rd-serp-features` at `2 vCPU / 4 GiB` initially; it is the most bursty job
- keep alert jobs with `--max-retries=0` to avoid duplicate Slack/Salesforce side effects
- if targeted alerts are not needed daily, disable schedule with `DEPLOY_ALERT_JOBS=false`

## Notes

- Schedules are created in `America/New_York` timezone.
- Alert wrappers include defaults to match current GitHub Actions behavior.
- Salesforce auth supports both:
  - password mode (`SF_USERNAME` + `SF_PASSWORD` + `SF_SECURITY_TOKEN`)
  - JWT mode (`SF_AUTH_MODE=jwt` with `SF_CLIENT_ID` + JWT key secret)
  - `SF_AUTH_MODE=auto` tries JWT first, then password fallback
- `db-ingest` is intentionally not scheduled (manual/backfill path).
- `send-alerts` is intentionally not deployed.

## Log summaries in Cloud Console

All Cloud Run job wrappers now emit standardized summary lines:

- `[JOB_START]` when a job begins
- `[STEP_START]`, `[STEP_DONE]`, `[STEP_FAIL]` around each pipeline step
- `[JOB_SUMMARY]` key-value final summary
- `[JOB_SUMMARY_JSON]` structured final summary

Alert jobs additionally emit:

- `[ALERT_SUMMARY_JSON]` from `send_crisis_alerts.py`
- `[TARGETED_ALERT_SUMMARY_JSON]` from `send_targeted_alerts.py`

Suggested Logs Explorer filter:

```text
resource.type="cloud_run_job"
("JOB_SUMMARY_JSON" OR "ALERT_SUMMARY_JSON" OR "TARGETED_ALERT_SUMMARY_JSON")
```

Set up logs-based metrics + dashboard:

```bash
cd /Users/plane/Documents/GitHub/risk-dashboard
PROJECT_ID="your-gcp-project" ./scripts/setup_job_observability.sh
```

This creates/updates:

- logs-based metric `rd_job_runs_total` (labels: `job`, `status`)
- logs-based metric `rd_job_duration_sec` (distribution, labels: `job`, `status`)
- dashboard `Risk Dashboard Jobs Observability`

Notes:

- Logs Explorer saved queries are UI-managed; there is no direct `gcloud` command in this environment to create them.
- After opening Logs Explorer with the filter above, click `Save query` in the UI for a persistent shortcut.

## Salesforce JWT Cutover (Okta/SSO-Friendly)

Use this to avoid password-expiration failures in scheduled jobs.

1. Create Salesforce Connected App for JWT bearer auth and capture the consumer key.
2. Generate an RSA private key (`.pem`) and upload cert/public key in the Connected App.
3. Create secrets:

```bash
echo -n 'YOUR_CONNECTED_APP_CONSUMER_KEY' | gcloud secrets versions add SF_CLIENT_ID --project "$PROJECT_ID" --data-file=-
gcloud secrets create SF_JWT_PRIVATE_KEY --project "$PROJECT_ID" || true
gcloud secrets versions add SF_JWT_PRIVATE_KEY --project "$PROJECT_ID" --data-file=/path/to/private_key.pem
```

4. Redeploy with JWT mode:

```bash
SF_AUTH_MODE=jwt ./scripts/deploy_cloud_run_jobs.sh
```

Notes:

- `SF_AUTH_MODE=auto` tries JWT first, then password fallback.
- `SF_AUTH_MODE=jwt` enforces JWT-only auth (recommended after cutover).
