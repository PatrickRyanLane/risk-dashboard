#!/usr/bin/env bash
set -euo pipefail

RUN_DATE="${RUN_DATE:-$(date -u +%F)}"
RUN_DAYS="${RUN_DAYS:-1}"

if ! [[ "${RUN_DAYS}" =~ ^[0-9]+$ ]] || [ "${RUN_DAYS}" -lt 1 ]; then
  echo "RUN_DAYS must be a positive integer; got '${RUN_DAYS}'" >&2
  exit 1
fi

for i in $(seq 0 $((RUN_DAYS - 1))); do
  DSTR="$(date -u -d "${RUN_DATE} -${i} day" +%F)"
  python -u scripts/ingest_serp_features_parquet.py --date "${DSTR}" --entity-type brand
  python -u scripts/ingest_serp_features_parquet.py --date "${DSTR}" --entity-type ceo
done

python -u scripts/refresh_negative_summary_view.py --serp-features
