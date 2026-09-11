#!/usr/bin/env bash
set -euo pipefail

for path in \
  SQL_PARALLEL_ROW_BINARY.md \
  README.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  INSPIRATION.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md; do
  test -s "$path"
done

rg -F -q 'SQL_PARALLEL_ROW_BINARY.md' README.md
rg -F -q 'CH-047: Parallel RowBinary Decode' BENCHMARK.md
rg -F -q '2.16x CPU' BENCHMARK.md
rg -F -q 'CH-047/M065u' INSPIRATION.md
rg -F -q 'Parallel RowBinary decode' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -F -q 'Large RowBinary payloads' ENGINE_IDEAS.md
rg -F -q 'M065u: Parallel RowBinary decode' CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
test ! -e scripts/inspect-next-codec-candidates.sh
