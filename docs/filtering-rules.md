# Filtering Rules

This document summarizes the shared filtering logic implemented in
`risk-dashboard/scripts/risk_rules.py` and mirrored in
`risk-dashboard-database/src/risk_rules.py`.

## Control Classification

Control status is computed by `classify_control(...)` and is applied to
articles, SERPs, and SERP features. Rules are evaluated in this order:

1) CEO-only hard exclusions (entity_type = ceo)
   - If host is in the CEO uncontrolled list, return uncontrolled.
   - If LinkedIn profile (`/in/` or `/pub/`) matches the CEO name, return controlled.
   - If X/Twitter handle matches the CEO name, return controlled.

2) Domain-specific path rules
   - facebook.com: uncontrolled if path contains `/posts/`, `/photos/`, or `/videos/`
   - instagram.com: uncontrolled if path contains `/p/` or `/reels/`
   - threads.net: uncontrolled if path contains `/posts/`
   - x.com / twitter.com: uncontrolled if path contains `/status/`

3) Platform and profile rules
   - YouTube brand channel (slug contains normalized company token): controlled
   - LinkedIn company page (`/company/`) with slug match: controlled
     - Uses strict match first, then a relaxed token match
   - X/Twitter company handle match: controlled

4) Always-controlled domains
   - play.google.com
   - apps.apple.com

5) Company domain rules
   - If host matches a roster domain, controlled
   - If a company token appears in subdomain, controlled

6) CEO-only path keywords (entity_type = ceo)
   - If the URL path contains any of the keywords below and the host
     matches the company domain or company token, controlled.

CEO uncontrolled domains:
- wikipedia.org
- youtube.com
- youtu.be
- tiktok.com

CEO controlled path keywords:
- /leadership/
- /about/
- /governance/
- /team/
- /investors/
- /board-of-directors
- /members/
- /member/

## Financial Routine Filter

`is_financial_routine(...)` marks items as routine financial coverage based on
title/snippet/source/URL.

### Financial terms (regex list)
- \bearnings\b
- \beps\b
- \brevenue\b
- \bguidance\b
- \bforecast\b
- \bprice target\b
- \bupgrade\b
- \bdowngrade\b
- \bdividend\b
- \bbuyback\b
- \bshares?\b
- \bstock\b
- \bmarket cap\b
- \bquarterly\b
- \bfiscal\b
- \bprofit\b
- \bEBITDA\b
- \b10-q\b
- \b10-k\b
- \bsec\b
- \bipo\b

### Financial sources (host match)
- yahoo.com
- marketwatch.com
- fool.com
- benzinga.com
- seekingalpha.com
- thefly.com
- barrons.com
- wsj.com
- investorplace.com
- nasdaq.com
- foolcdn.com

### Ticker pattern
- Regex: `\b(?:NYSE|NASDAQ|AMEX):\s?[A-Z]{1,5}\b`
