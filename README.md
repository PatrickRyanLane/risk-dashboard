# Risk Dashboard Pipeline

`risk-dashboard` is the automation repo for the risk dashboard product. It owns daily collection, scoring rules, active DB ingest and backfills, enrichment jobs, and alerting. The sibling repo `risk-dashboard-database` owns the Postgres schema, materialized views, Flask dashboard and API, and Cloud Run deployment.

## Current responsibilities

- Google News article collection for brands and CEOs
- SERP result processing from raw parquet inputs
- SERP feature ingest from raw parquet into Postgres
- Roster and boards sync into Postgres
- CSV backfills from GCS or local data into Postgres
- LLM enrichment for DB rows that still need model labels
- Slack and Salesforce-driven alerting
- GCS compatibility outputs under `gs://risk-dashboard`

The current pipeline is hybrid:

- Daily article and SERP jobs still write CSV outputs to GCS.
- Those same daily jobs also upsert core article and SERP rows directly into Postgres when `DATABASE_URL` is set.
- CSV ingest jobs remain in place for backfills, repairs, and metric loading.

## Current pipeline

1. `scripts/ingest_roster_only.py`
   Syncs `gs://risk-dashboard/rosters/main-roster.csv` and `boards-roster.csv` into `companies`, `ceos`, and `boards`.

2. `scripts/news_articles_brands.py` and `scripts/news_articles_ceos.py`
   Pull Google News RSS, apply `risk_rules.py` plus VADER scoring, mark uncertain rows, write `data/processed_articles/YYYY-MM-DD-*-articles-modal.csv`, and upsert `articles` plus `company_article_mentions` or `ceo_article_mentions`.

3. `scripts/news_sentiment_brands.py` and `scripts/news_sentiment_ceos.py`
   Build daily article table CSVs and rolling chart CSVs from processed article outputs.

4. `scripts/backfill_article_mentions_daily.py`
   Copies article mention rows into the partitioned `company_article_mentions_daily` and `ceo_article_mentions_daily` tables used by dashboard aggregates.

5. `scripts/process_serps_brands.py` and `scripts/process_serps_ceos.py`
   Read raw SERP parquet files from S3, classify sentiment and control, mark uncertain rows, write modal and table CSVs plus rolling SERP chart CSVs, emit mismatch files, and upsert `serp_runs` plus `serp_results`.

6. `scripts/ingest_serp_features_parquet.py`
   Reads the raw SERP parquet inputs directly into `serp_feature_daily` and `serp_feature_items`, including Top Stories narrative tagging for negative, non-financial items.

7. `scripts/fetch_stock_data.py` and `scripts/fetch_trends_data.py`
   Produce GCS CSV outputs for stock and trends data. Those files can then be loaded into Postgres by `scripts/bulk_ingest.py`.

8. `scripts/refresh_negative_summary_view.py`
   Refreshes the materialized views used by the dashboard for article counts, SERP counts, and SERP features.

9. `scripts/llm_enrich.py`
   Updates Postgres rows that still need LLM labels. It covers article mentions, SERP results, and SERP feature items.

10. `scripts/send_crisis_alerts.py` and `scripts/send_targeted_alerts.py`
    Read DB-backed summary and Top Stories data, then send Slack alerts and Salesforce review actions. Optional LLM summaries are supported when `LLM_API_KEY` is configured.

## Current outputs and storage

GCS outputs remain the compatibility layer and backfill source:

- `data/processed_articles/YYYY-MM-DD-brand-articles-modal.csv`
- `data/processed_articles/YYYY-MM-DD-ceo-articles-modal.csv`
- `data/processed_articles/YYYY-MM-DD-brand-articles-table.csv`
- `data/processed_articles/YYYY-MM-DD-ceo-articles-table.csv`
- `data/processed_serps/YYYY-MM-DD-brand-serps-modal.csv`
- `data/processed_serps/YYYY-MM-DD-ceo-serps-modal.csv`
- `data/processed_serps/YYYY-MM-DD-brand-serps-table.csv`
- `data/processed_serps/YYYY-MM-DD-ceo-serps-table.csv`
- `data/daily_counts/brand-articles-daily-counts-chart.csv`
- `data/daily_counts/ceo-articles-daily-counts-chart.csv`
- `data/daily_counts/brand-serps-daily-counts-chart.csv`
- `data/daily_counts/ceo-serps-daily-counts-chart.csv`
- `data/daily_counts/negative-articles-summary.csv`
- `data/stock_prices/YYYY-MM-DD-stock-data.csv`
- `data/trends_data/YYYY-MM-DD-trends-data.csv`

Current direct Postgres writes from this repo include:

- `companies`, `ceos`, `boards`
- `articles`
- `company_article_mentions`, `ceo_article_mentions`
- `company_article_mentions_daily`, `ceo_article_mentions_daily`
- `serp_runs`, `serp_results`
- `serp_feature_daily`, `serp_feature_items`
- LLM label columns and narrative tag columns on those tables

## Key workflows

The workflow YAML files under `.github/workflows/` are the source of truth for schedules. The main jobs today are:

- `daily_brands.yml` and `daily_ceos.yml`
- `db-ingest.yml`
- `roster-ingest.yml`
- `fetch-stock-data.yml`
- `fetch-trends-data.yml`
- `serp-features-parquet.yml`
- `recompute-serp-feature-daily.yml`
- `backfill_articles.yml`
- `backfill_serps.yml`
- `backfill_narrative_tags.yml`
- `llm-enrich.yml`
- `refresh-mvs.yml`
- `aggregate_negative_articles.yml`
- `send_crisis_alerts.yml`
- `send_targeted_alerts.yml`

`db-ingest.yml` and `scripts/bulk_ingest.py` are now best understood as CSV backfill and repair paths, not the only way article and SERP rows reach Postgres.

## Common commands

```bash
export DATABASE_URL="postgresql://..."

python scripts/ingest_roster_only.py \
  --roster-path gs://risk-dashboard/rosters/main-roster.csv \
  --boards-path gs://risk-dashboard/rosters/boards-roster.csv

python scripts/bulk_ingest.py \
  --data-dir gs://risk-dashboard/data \
  --date YYYY-MM-DD

python scripts/ingest_serp_features_parquet.py \
  --date YYYY-MM-DD \
  --entity-type brand

python scripts/llm_enrich.py --max-calls 200
```

## Notes

- `scripts/risk_rules.py` is the central rules module for finance-routine filtering, forced-negative logic, control classification, and narrative tagging thresholds.
- `data/daily_counts/negative-articles-summary.csv` is still generated for compatibility and reporting, but the current alerting path reads from Postgres rather than depending on that CSV.
- The dashboard and public/internal API behavior are documented in `risk-dashboard-database/docs/system-overview.md`.
