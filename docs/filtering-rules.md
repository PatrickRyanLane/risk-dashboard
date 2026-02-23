# Filtering Rules

This document is the source of truth for shared sentiment/control helpers in
`scripts/risk_rules.py`, and where each helper is used in the pipeline.

## Rule Families In `risk_rules.py`

- `classify_control(...)`
- `parse_company_domains(...)`
- `is_financial_routine(...)`
- `should_neutralize_finance_routine(...)`
- `title_mentions_legal_trouble(...)`
- `strip_neutral_terms_brand(...)`
- `should_neutralize_brand_title(...)`
- `strip_neutral_terms_ceo(...)`
- `should_neutralize_ceo_title(...)`
- `should_force_negative_ceo(...)`

## Rule Usage Matrix

| Script | Pipeline Stage | Entity | Shared Rules Used |
|---|---|---|---|
| `scripts/news_articles_brands.py` | Google News article classification | Brand | `parse_company_domains`, `classify_control`, `is_financial_routine`, `should_neutralize_finance_routine`, `title_mentions_legal_trouble`, `strip_neutral_terms_brand` |
| `scripts/news_articles_ceos.py` | Google News article classification | CEO | `parse_company_domains`, `classify_control`, `is_financial_routine`, `should_neutralize_finance_routine`, `should_force_negative_ceo`, `strip_neutral_terms_ceo` |
| `scripts/process_serps_brands.py` | Raw SERP row processing | Brand | `parse_company_domains`, `classify_control`, `is_financial_routine`, `should_neutralize_finance_routine`, `title_mentions_legal_trouble`, `should_neutralize_brand_title` |
| `scripts/process_serps_ceos.py` | Raw SERP row processing | CEO | `parse_company_domains`, `classify_control`, `is_financial_routine`, `should_neutralize_finance_routine`, `should_force_negative_ceo`, `should_neutralize_ceo_title` |
| `scripts/ingest_v2.py` | CSV/backfill ingest into DB | Brand + CEO | `parse_company_domains`, `classify_control`, `is_financial_routine`, `should_neutralize_finance_routine` |
| `scripts/ingest_serp_features_parquet.py` | SERP features ingest into DB | Brand + CEO | `parse_company_domains`, `classify_control`, `is_financial_routine`, `title_mentions_legal_trouble` |

## Control Classification (`classify_control`)

Control status is computed with ordered checks:

1. CEO-only exclusions and profile handle checks.
2. Platform path rules (`facebook.com`, `instagram.com`, `threads.net`, `x.com`/`twitter.com`).
   - Instagram reel URLs (`/reel/` or `/reels/`) are controlled when either:
     - snippet byline attribution matches the brand (for example: `by Brand Name on Instagram`), or
     - the Instagram handle in the URL path matches the brand/CEO handle tokens.
   - Social URLs can still be controlled when snippet byline attribution matches the brand
     (for example: `by Brand Name on Instagram`; supported for Facebook/Instagram/Threads/YouTube/X).
3. YouTube, LinkedIn, and X/Twitter handle matching.
4. Always-controlled domains (`play.google.com`, `apps.apple.com`, etc.).
5. Company roster domain match.
6. Fallback token match in subdomain.
7. CEO-only path keyword fallback (`/leadership/`, `/about/`, `/team/`, etc.).

CEO hard-uncontrolled domains:

- `wikipedia.org`
- `youtube.com`
- `youtu.be`
- `tiktok.com`

## Financial Routine Neutralization

`is_financial_routine(...)` returns true if any of the following match:

- finance regex terms (earnings, EPS, revenue, guidance, buyback, shares, stock, market cap, fiscal, 10-K/10-Q, IPO, etc.)
- ticker pattern: `\b(?:NYSE|NASDAQ|AMEX):\s?[A-Z]{1,5}\b`
- finance source domains (`yahoo.com`, `marketwatch.com`, `wsj.com`, `seekingalpha.com`, etc.)

`should_neutralize_finance_routine(...)` neutralizes only when:

- current sentiment is `positive` or `negative`
- routine finance pattern matched
- no material risk term matched (lawsuit, fraud, investigation, breach, recall, layoffs, etc.)

## Brand And CEO Sentiment Lexicons

Centralized lexicons now live in `scripts/risk_rules.py`:

- brand neutralization terms: `BRAND_NEUTRALIZE_TITLE_TERMS`
- brand legal-trouble terms: `BRAND_LEGAL_TROUBLE_TERMS`
- CEO neutralization terms: `CEO_NEUTRALIZE_TITLE_TERMS`
- CEO forced-negative terms: `CEO_ALWAYS_NEGATIVE_TERMS`
- force-negative source domains: `FORCE_NEGATIVE_SOURCE_DOMAINS`

These are consumed through helper functions (not directly in pipeline scripts).

## Rule Authoring Guideline

When adding or changing sentiment/control lexicons:

1. Add the regex terms and helper function in `scripts/risk_rules.py`.
2. Import the helper in pipeline scripts (`news_*`, `process_serps_*`, `ingest_*`).
3. Update this file's Rule Usage Matrix if a new helper is added.

Avoid adding new sentiment word lists directly inside pipeline scripts.
