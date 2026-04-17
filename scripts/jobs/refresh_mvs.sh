#!/usr/bin/env bash
set -euo pipefail

python scripts/refresh_negative_summary_view.py --article-counts --serp-counts
