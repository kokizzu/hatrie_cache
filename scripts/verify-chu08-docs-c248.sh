#!/usr/bin/env bash
set -euo pipefail

test -s CHU08_AUTOMATIC_SQL_RESULT_CACHE.md
rg -q 'CHU08_AUTOMATIC_SQL_RESULT_CACHE.md' README.md
rg -q 'CH-U08.*Adopted' PRODUCT_IDEA_GAPS.md
rg -q 'CH-U08: Automatic SQL Result-Cache Wiring' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 'CH-U08 Automatic SQL Result-Cache Wiring' BENCHMARK.md
rg -q 'ConfigureSQLResultCache' CHU08_AUTOMATIC_SQL_RESULT_CACHE.md
rg -q '317-357x faster' BENCHMARK.md
