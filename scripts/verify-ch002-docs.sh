#!/usr/bin/env bash
set -euo pipefail

test -s SQL_ORDERED_RANGE_PRUNING.md
rg -n '^\| CH-002 \|' ENGINE_IDEAS.md
rg -n 'Sparse primary-key mark pruning|SQL_ORDERED_RANGE_PRUNING.md|#ordered-range-pruning' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -n '^## Ordered Range Pruning$|benchmark-ch002-hattrie-materialized|benchmark-ch002-hattrie-stream' BENCHMARK.md
rg -n 'SQL_ORDERED_RANGE_PRUNING.md' README.md
