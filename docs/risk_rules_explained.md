# `scripts/risk_rules.py` — Detailed Walkthrough

This document explains each section of `risk_rules.py` in plain language, with emphasis on how filtering decisions are made.

---

## 1) High-level purpose

`risk_rules.py` is a rule engine for classifying news/social links and deciding whether potentially negative sentiment should be **kept**, **overridden**, or **neutralized**.

At a high level, it does four things:

1. Defines large keyword/domain lists used as heuristics.
2. Decides whether a URL/source is **controlled** by a brand/CEO.
3. Detects legal/risk/finance contexts in titles/snippets/sources.
4. Applies neutralization rules so routine financial stories do not over-trigger risk sentiment.

---

## 2) Constants and regex setup

The file starts with configuration-like constants.

### A) Controlled/uncontrolled domain sets

- `ALWAYS_CONTROLLED_DOMAINS`: domains considered controlled by default (e.g., Facebook, Instagram app stores).
- `CEO_UNCONTROLLED_DOMAINS`: domains that are generally not considered controlled for CEOs (e.g., Wikipedia, YouTube), unless there is matching byline evidence.
- `CEO_CONTROLLED_PATH_KEYWORDS`: URL path hints (`/leadership/`, `/about/`, etc.) that can make CEO-related links controlled under specific conditions.

### B) Finance detection vocabulary

- `FINANCE_TERMS` is a regex term list for routine market/earnings language.
- `FINANCE_TERMS_RE` compiles it once for speed.
- `FINANCE_SOURCES` is a set of known finance/news domains.
- `TICKER_RE` captures exchange ticker patterns like `NYSE: ABC`.

### C) Material risk vocabulary

- `MATERIAL_RISK_TERMS` includes terms for legal/regulatory/security/workforce issues.
- `MATERIAL_RISK_TERMS_RE` compiles these into one matcher.

### D) Name normalization and publisher matching helpers

- `NAME_IGNORE_TOKENS` removes common company fillers (`inc`, `group`, `the`, etc.) when tokenizing names.
- `PUBLISHER_SUFFIX_TOKENS` allows publisher variants like `brandnews` / `brandmedia`.
- `SOCIAL_BYLINE_PLATFORM_PATTERNS` maps hostnames to allowed platform names in “By X on Y” snippets.

### E) Title neutralization and force-negative vocabularies

There are several broad regex vocab sets:

- `BRAND_NEUTRALIZE_TITLE_TERMS`: terms that can falsely imply brand risk and should often be stripped before matching.
- `BRAND_LEGAL_TROUBLE_TERMS`: terms signaling legal/trouble context for brands.
- `CEO_NEUTRALIZE_TITLE_TERMS`: similar “strip noise” list for CEO titles.
- `CEO_ALWAYS_NEGATIVE_TERMS`: terms that should strongly force CEO-negative interpretation.

Each list is compiled once into a regex (`*_RE`) for repeated use.

---

## 3) Brand/CEO title cleanup and trouble detection

### `strip_neutral_terms_brand(headline)`
Replaces brand neutralization terms with spaces, then normalizes whitespace.

### `should_neutralize_brand_title(title)`
Boolean check: does title contain any brand-neutralize patterns?

### `_is_force_negative_source(url, source)`
Checks domain/source-level overrides (`FORCE_NEGATIVE_SOURCE_DOMAINS` or source string variants). If matched, downstream checks can force negative.

### `title_mentions_legal_trouble(title, snippet, url, source)`
Returns `True` if:
1. source is force-negative, or
2. title+snippet contains any `BRAND_LEGAL_TROUBLE_TERMS`.

### `strip_neutral_terms_ceo(title)` and `should_neutralize_ceo_title(title)`
CEO equivalents of brand cleanup/boolean-neutralize checks.

### `should_force_negative_ceo(title, snippet, url, source)`
CEO-negative forcing logic:
1. If `title_mentions_legal_trouble(...)` => `True`.
2. Else match `CEO_ALWAYS_NEGATIVE_RE` in title+snippet.

---

## 4) URL/hostname/token normalization primitives

### `hostname(url)`
Parses URL hostname safely, lowercases, strips `www.`. Returns empty string on parse failure.

### `_norm_token(s)`
Lowercases and keeps only alphanumeric characters. This is core to fuzzy token comparisons.

### `_name_tokens(value, min_len=4)`
Splits on non-word chars, normalizes tokens, drops ignored/common short tokens, returns cleaned token list.

These three utilities power almost all fuzzy matching in the file.

---

## 5) Publisher/byline matching logic

### `_publisher_matches_company(company, publisher)`
Determines whether publisher name appears to represent the company.

Matching strategy (in order):
1. normalized full-string equality,
2. multi-token company subset inside publisher tokens,
3. single-token company prefix + known suffix (`news`, `media`, etc.).

### Social byline extraction path

- `_host_matches_social(base_host, host)`: exact/subdomain matcher.
- `_social_platform_tokens_for_host(host)`: picks allowed platform tokens for a host.
- `_extract_social_byline_author(snippet, host)`: regex-extracts author from patterns like “By Jane Doe on Instagram”.
- `_social_byline_matches_company(company, snippet, host)`: checks whether extracted author matches company via `_publisher_matches_company`.

This is a key safeguard for social URLs where ownership is ambiguous.

---

## 6) Platform-specific handle/page matching

The next block checks whether a URL likely belongs to the company or a CEO.

### Instagram
- `_instagram_handle_from_path(path)`: extracts first path segment as handle, skipping reserved paths (`/p/`, `/reel/`, etc.).
- `_is_instagram_company_handle(company, url)`: compares handle token to company handle tokens.
- `_is_instagram_person_handle(name, url)`: compares handle token to person tokens.

### Token builders
- `_company_handle_tokens(company)`: candidate compact tokens for brand handles.
- `_person_handle_tokens(name)`: candidate tokens from first/last/full and combined forms.

### YouTube / LinkedIn / X
- `_is_brand_youtube_channel(company, url)`: checks YouTube channel/user/@slug for brand token inclusion.
- `_is_linkedin_company_page(company, url)`: checks `/company/<slug>` + token overlap.
- `_is_linkedin_company_post(company, url)`: checks `/posts/<handle...>` + overlap.
- `_linkedin_slug_matches_company(company, slug)`: fallback token overlap matcher for LinkedIn slugs.
- `_is_linkedin_person_profile(name, url)`: checks `/in/` or `/pub/` profile slug against person tokens.
- `_is_x_company_handle(company, url)`: handle token overlap on x.com/twitter.com.
- `_is_x_person_handle(name, url)`: person-token overlap on x.com/twitter.com.

---

## 7) Parsing company website domains

### `parse_company_domains(websites)`
Takes a pipe-separated website string, normalizes each entry to URL form if needed, extracts valid hosts, and returns a set.

This set feeds controlled-domain matching later.

---

## 8) Core ownership decision: `classify_control(...)`

This is the central filter for “is this source controlled by the company/CEO?”.

Inputs include company name, URL, precomputed `company_domains`, optional entity type (`company`/`ceo`), person name, publisher, snippet.

Decision flow (ordered):

1. **Publisher shortcut**: if publisher matches company, controlled.
2. Parse host/path; if no host, uncontrolled.
3. Compute `social_byline_controlled` from snippet attribution.
4. Set platform flags (Facebook/Instagram/Threads/X).
5. **CEO-specific guardrail**:
   - For `CEO_UNCONTROLLED_DOMAINS`, default uncontrolled unless social byline proves control.
   - CEO LinkedIn person profile or X handle matching person name => controlled.
6. **Facebook**:
   - post/photo/video URLs require byline control,
   - other Facebook URLs default controlled.
7. **Instagram**:
   - reels require byline OR company/person handle match,
   - `/p/` posts require byline,
   - otherwise controlled.
8. **Threads**:
   - `/posts/` requires byline,
   - otherwise controlled.
9. YouTube channel brand match => controlled.
10. Social byline on YouTube/shortlinks/X => controlled.
11. LinkedIn company post/page matches => controlled.
12. X `/status/` URLs require byline; otherwise uncontrolled.
13. X handle match => controlled.
14. Host in `ALWAYS_CONTROLLED_DOMAINS` => controlled.
15. Host matches known company domain set => controlled.
16. Host labels contain brand token (pre-TLD parts) => controlled.
17. CEO fallback: leadership/about-style path keywords can indicate control, but only if tied to matched company-domain/brand-token evidence.
18. Else uncontrolled.

This ordering matters: earlier conditions short-circuit later checks.

### Important nuance about `facebook.com` in `ALWAYS_CONTROLLED_DOMAINS`

No—Facebook post URLs are **not** automatically controlled just because `facebook.com` appears in
`ALWAYS_CONTROLLED_DOMAINS`.

Why:

- `classify_control(...)` evaluates the Facebook-specific branch **before** the global
  `ALWAYS_CONTROLLED_DOMAINS` check.
- In that earlier Facebook branch, URLs containing `/posts/`, `/photos/`, or `/videos/` return
  `social_byline_controlled`.
- So if byline attribution does **not** match the company, those post/detail URLs are treated as
  uncontrolled.
- The broad always-controlled behavior mainly applies to non-post Facebook pages/paths.

---

## 9) Financial routine and material risk gating

### `is_financial_routine(title, snippet, url, source)`
Returns `True` if **any** of:
- finance term regex match in title+snippet+source,
- ticker pattern in title,
- URL hostname belongs to known finance source domain.

### `has_material_risk_terms(title, snippet, source)`
Simple regex hit for material risk vocabulary.

### `should_neutralize_finance_routine(sentiment, title, snippet, url, source, finance_routine=None)`
Final neutralization gate:

1. Only applies if sentiment is currently `positive` or `negative`.
2. Requires routine-finance context (`finance_routine` arg or computed by `is_financial_routine`).
3. **Does not neutralize** if material risk terms are present.
4. Otherwise returns `True` (neutralize routine finance sentiment).

In practice: routine market chatter gets neutralized, but serious risk language keeps sentiment active.

---

## 10) Practical interpretation of the filtering strategy

- The file favors **high recall** on legal/risk trouble terms (broad vocab lists).
- It adds explicit **de-noising** lists for frequent false-positive title words.
- Controlled-source detection is conservative on social post/detail URLs (needs byline/handle checks), but permissive on certain platform roots.
- Finance neutralization prevents over-flagging earnings/market boilerplate unless there is explicit material risk context.
