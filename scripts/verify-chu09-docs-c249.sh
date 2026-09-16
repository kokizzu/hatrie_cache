#!/usr/bin/env bash
set -euo pipefail

test -f CHU09_PERSISTED_SQL_RESULT_CACHE.md
rg -n 'CH-U09|Persisted SQL Result Cache|Persist\(|Restore\(|88,098|115,546|1.31x|1.25x' README.md PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md CHU09_PERSISTED_SQL_RESULT_CACHE.md
