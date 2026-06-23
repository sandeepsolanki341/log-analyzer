#!/usr/bin/env bash
# Registers the index template before first ingest so field types are correct
# (especially @timestamp as date and keyword fields for aggregation).
set -euo pipefail
ES="${ES_URL:-http://localhost:9200}"
AUTH=()
if [[ -n "${ES_API_KEY:-}" ]]; then AUTH=(-H "Authorization: ApiKey ${ES_API_KEY}")
elif [[ -n "${ES_USER:-}" ]]; then AUTH=(-u "${ES_USER}:${ES_PASSWORD:-}"); fi
curl -sS -X PUT "${ES}/_index_template/app-logs" \
  "${AUTH[@]}" -H 'Content-Type: application/json' \
  --data-binary @"$(dirname "$0")/index-template.json"
echo
echo "Template 'app-logs' applied to ${ES}"
