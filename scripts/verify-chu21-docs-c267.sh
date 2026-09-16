#!/usr/bin/env bash
set -euo pipefail

test -f CHU21_STREAMING_TEXT_INGESTION.md
rg -q 'CHU21_STREAMING_TEXT_INGESTION.md' README.md
rg -q 'CH-U19.*Adopted' PRODUCT_IDEA_GAPS.md
rg -q 'CH-U21.*Adopted' PRODUCT_IDEA_GAPS.md
rg -q 'Streaming.*JSONEachRow' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 'ch-u21-streaming-text-ingestion' BENCHMARK.md
test ! -e scripts/inspect-chu21-c266.sh
printf '%s\n' 'CH-U21 documentation verification passed.'
