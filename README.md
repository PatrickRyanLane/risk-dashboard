# News Sentiment Dashboard

Automated news sentiment analysis and monitoring system for brands and CEOs. Tracks media coverage, analyzes sentiment, and processes search engine results (SERPs) to provide comprehensive reputation monitoring.

## Overview

This system automatically:
- Fetches daily news articles about brands and CEOs from Google News RSS
- Analyzes sentiment using VADER (positive, neutral, negative)
- Processes SERP data to track search engine visibility
- Saves generated .csv files to GCS risk-dashboard bucket
// Currently not sending // - Sends email alerts for significant negative coverage

## Workflows

See [WORKFLOWS.md](WORKFLOWS.md) for detailed workflow documentation.

### LLM Enrichment (DB-only)

The LLM enrichment step runs separately from the daily CSV pipelines. It finds rows in Postgres
where `uncertain = true` and `llm_label` is still null, calls the LLM, and updates the DB in place.

- Script: `scripts/llm_enrich.py`
- Workflow: `.github/workflows/llm-enrich.yml` (10:30am ET)
- Required secrets: `DATABASE_URL`, `LLM_API_KEY`
- Required vars: `LLM_PROVIDER`, `LLM_MODEL`, `LLM_MAX_CALLS`

### Daily Automated Runs

**Brand Pipeline** (`daily_brands.yml`) - 09:10 UTC daily
- Fetches news articles about brands
- Analyzes sentiment
- Processes SERP data
- Updates rolling aggregates

**CEO Pipeline** (`daily_ceos.yml`) - 12:00 UTC daily
- Fetches news articles about CEOs
- Analyzes sentiment
- Processes SERP data
- Updates rolling aggregates

**Alert System** (`send_alerts.yml`) - Triggered after pipelines
- Monitors for negative sentiment spikes
- Sends email alerts via Mailgun
- Respects cooldown periods

## Key Files

- `scripts/` - Python automation scripts
- `.github/workflows/` - GitHub Actions automation

## Manual Operations

**Run for specific date:**
1. Go to Actions → Daily Brands Pipeline
2. Click "Run workflow"
3. Enter date (YYYY-MM-DD format)

**Backfill historical data:**
1. Go to Actions → Backfill Brand & CEO SERPs
2. Enter start and end dates

## Troubleshooting

**Workflow failed:** Check GitHub Actions logs for errors

**Missing data:** Manually trigger workflow for that date

**Alerts not sending:** Verify Mailgun secrets are configured

## Support

For detailed documentation, see [WORKFLOWS.md](WORKFLOWS.md)
