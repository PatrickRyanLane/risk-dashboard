# Cloud Run Job Flags Reference

This doc lists all practical manual "run with options" flags for the migrated Cloud Run Jobs setup.

Use either:

- direct `gcloud run jobs execute ... --update-env-vars ...`, or
- the helper: `scripts/run_cloud_job.sh`

## Quick Start

```bash
cd /Users/plane/Documents/GitHub/risk-dashboard

# Example: run daily brands for a specific date
scripts/run_cloud_job.sh daily-brands --date 2026-04-17 --wait

# Example: run LLM enrichment with a temporary max-calls override
scripts/run_cloud_job.sh llm-enrich --llm-max-calls 400 --wait

# Example: force-send crisis alerts for selected brands
scripts/run_cloud_job.sh send-crisis-alerts \
  --force-brands "Fanatics,Nike" \
  --force-send 1 \
  --wait
```

## Helper Script

Path: `scripts/run_cloud_job.sh`

Supported job aliases:

- `daily-brands` (`rd-daily-brands`)
- `daily-ceos` (`rd-daily-ceos`)
- `serp-features` (`rd-serp-features`)
- `fetch-stock` (`rd-fetch-stock`)
- `fetch-trends-b1` (`rd-fetch-trends-b1`)
- `fetch-trends-b2` (`rd-fetch-trends-b2`)
- `fetch-trends-b3` (`rd-fetch-trends-b3`)
- `fetch-trends` (`rd-fetch-trends`) + `--batch 1|2|3`
- `llm-enrich` (`rd-llm-enrich`)
- `aggregate-negative` (`rd-aggregate-negative`)
- `recompute-serp-feature` (`rd-recompute-serp-feature`)
- `refresh-mvs` (`rd-refresh-mvs`)
- `send-crisis-alerts` (`rd-send-crisis-alerts`)
- `send-targeted-alerts` (`rd-send-targeted-alerts`)

Global options:

- `--project <id>`
- `--region <region>` (default `us-east1`)
- `--wait` or `--async`
- `--dry-run`
- `--env KEY=VALUE` (repeatable, any env var)

Convenience options:

- `--date` -> `RUN_DATE`
- `--run-days` -> `RUN_DAYS`
- `--batch` (for `fetch-trends` alias)
- `--gcs-bucket` -> `GCS_BUCKET`
- `--brands-batch-size` -> `BRANDS_BATCH_SIZE`
- `--ceos-batch-size` -> `CEOS_BATCH_SIZE`
- `--articles-max-per-alias` -> `ARTICLES_MAX_PER_ALIAS`
- `--articles-sleep-sec` -> `ARTICLES_SLEEP_SEC`
- `--trends-batch-size` -> `TRENDS_BATCH_SIZE`
- `--stock-batch-size` -> `STOCK_BATCH_SIZE`
- `--llm-max-calls` -> `LLM_MAX_OVERRIDE`
- `--llm-batch-size` -> `LLM_BATCH_SIZE`
- `--llm-provider` -> `LLM_PROVIDER`
- `--llm-model` -> `LLM_MODEL`
- `--negative-days-back` -> `NEGATIVE_SUMMARY_DAYS_BACK`
- `--recompute-days` -> `RECOMPUTE_DAYS`
- `--force-brands` -> `FORCE_BRANDS`
- `--force-send` -> `FORCE_SEND`
- `--target-brands` -> `TARGET_BRANDS`
- `--target-dry-run` -> `DRY_RUN`

## Job-by-Job Flags

### `rd-daily-brands`

- `RUN_DATE` (default: today UTC)
- `GCS_BUCKET` (default: `risk-dashboard`)
- `BRANDS_BATCH_SIZE` (default: `1500`)

Example:

```bash
scripts/run_cloud_job.sh daily-brands --date 2026-04-17 --brands-batch-size 1200 --wait
```

### `rd-daily-ceos`

- `RUN_DATE`
- `GCS_BUCKET`
- `CEOS_BATCH_SIZE` (default: `1500`)
- `ARTICLES_MAX_PER_ALIAS` (default: `25`)
- `ARTICLES_SLEEP_SEC` (default: `0.35`)

Example:

```bash
scripts/run_cloud_job.sh daily-ceos \
  --date 2026-04-17 \
  --ceos-batch-size 1200 \
  --articles-max-per-alias 35 \
  --wait
```

### `rd-serp-features`

- `RUN_DATE`
- `RUN_DAYS` (default: `1`)

Example:

```bash
scripts/run_cloud_job.sh serp-features --date 2026-04-17 --run-days 3 --wait
```

### `rd-fetch-stock`

- `GCS_BUCKET`
- `STOCK_BATCH_SIZE` (default: `100`)

Example:

```bash
scripts/run_cloud_job.sh fetch-stock --stock-batch-size 200 --wait
```

### `rd-fetch-trends-b1` / `rd-fetch-trends-b2` / `rd-fetch-trends-b3`

- `GCS_BUCKET`
- `TRENDS_BATCH_SIZE` (default: `300`)

Examples:

```bash
scripts/run_cloud_job.sh fetch-trends-b1 --trends-batch-size 450 --wait
scripts/run_cloud_job.sh fetch-trends --batch 3 --trends-batch-size 450 --wait
```

### `rd-llm-enrich`

- `LLM_MAX_OVERRIDE` (preferred per-run override)
- `LLM_MAX_CALLS` (job default/fallback)
- `LLM_BATCH_SIZE` (default: `200`)
- `LLM_PROVIDER`
- `LLM_MODEL`

Example:

```bash
scripts/run_cloud_job.sh llm-enrich \
  --llm-max-calls 500 \
  --llm-batch-size 250 \
  --wait
```

### `rd-aggregate-negative`

- `GCS_BUCKET`
- `NEGATIVE_SUMMARY_DAYS_BACK` (default: `90`)

Example:

```bash
scripts/run_cloud_job.sh aggregate-negative --negative-days-back 120 --wait
```

### `rd-recompute-serp-feature`

- `RECOMPUTE_DAYS` (default: `7`)

Example:

```bash
scripts/run_cloud_job.sh recompute-serp-feature --recompute-days 14 --wait
```

### `rd-refresh-mvs`

No commonly-used runtime flags.

Example:

```bash
scripts/run_cloud_job.sh refresh-mvs --wait
```

### `rd-send-crisis-alerts`

Common:

- `FORCE_BRANDS` (comma-separated)
- `FORCE_SEND` (`0`/`1`)

Advanced:

- `ALERT_BRANDS`, `ALERT_CEOS`
- `SERP_GATE_ENABLED`, `SERP_GATE_MIN`, `SERP_GATE_DAYS`, `SERP_GATE_DEBUG`
- `SERP_TOP_STORIES_REQUIRED`, `SERP_TOP_STORIES_NEG_MIN`
- `ALERT_LOOKBACK_DAYS`, `TOP_STORIES_TODAY_ONLY`
- `ALERT_TIMEZONE`
- `LLM_PROVIDER`, `LLM_MODEL`, `LLM_SUMMARY_MAX_CALLS`
- `SF_AUTH_MODE` (`auto` | `jwt` | `password`)
- `SF_LOGIN_URL` (default: `https://login.salesforce.com`)
- `SF_API_VERSION` (default: `59.0`)

Example:

```bash
scripts/run_cloud_job.sh send-crisis-alerts \
  --force-brands "Fanatics,Nike" \
  --force-send 1 \
  --env SERP_GATE_ENABLED=1 \
  --env SERP_GATE_MIN=2 \
  --wait
```

### `rd-send-targeted-alerts`

Common:

- `TARGET_BRANDS` (comma-separated)
- `DRY_RUN` (`0`/`1`)

Advanced:

- `TARGET_DEFAULT_CHANNEL`
- `BRAND_CHANNEL_MAP` (JSON)
- `TARGET_SKIP_COOLDOWN`
- `TARGET_SERP_GATE_ENABLED`
- `TARGET_TOP_STORIES_GATE_ENABLED`
- `TARGET_TOP_STORIES_NEG_GATE_ENABLED`
- `TARGET_TOP_STORIES_TODAY_ONLY`
- `SERP_TOP_STORIES_REQUIRED`, `SERP_TOP_STORIES_NEG_MIN`
- `SERP_GATE_MIN`, `SERP_GATE_DAYS`, `SERP_GATE_DEBUG`
- `ALERT_TIMEZONE`
- `LLM_PROVIDER`, `LLM_MODEL`, `LLM_SUMMARY_MAX_CALLS`
- `SF_AUTH_MODE` (`auto` | `jwt` | `password`)
- `SF_LOGIN_URL` (default: `https://login.salesforce.com`)
- `SF_API_VERSION` (default: `59.0`)

Example:

```bash
scripts/run_cloud_job.sh send-targeted-alerts \
  --target-brands "Fanatics,Nike" \
  --target-dry-run 1 \
  --env TARGET_SERP_GATE_ENABLED=1 \
  --wait
```

## Direct `gcloud` Equivalent

If you prefer direct `gcloud` commands:

```bash
gcloud run jobs execute rd-daily-brands \
  --project "gen-lang-client-0154760958" \
  --region "us-east1" \
  --update-env-vars RUN_DATE=2026-04-17 \
  --wait
```

For values that contain commas, use delimiter escaping:

```bash
gcloud run jobs execute rd-send-crisis-alerts \
  --project "gen-lang-client-0154760958" \
  --region "us-east1" \
  --update-env-vars '^:^FORCE_BRANDS=Fanatics,Nike:FORCE_SEND=1' \
  --wait
```

The helper script automatically handles delimiter escaping for `--update-env-vars`.
