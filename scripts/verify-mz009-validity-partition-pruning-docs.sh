#!/usr/bin/env bash
set -euo pipefail

for file in \
  MZ009_VALIDITY_PARTITION_PRUNING.md \
  SQL_TEMPORAL_VALIDITY.md \
  ENGINE_IDEAS.md \
  BENCHMARK.md \
  README.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md; do
  test -s "$file"
done

grep -F 'MZ009_VALIDITY_PARTITION_PRUNING.md' README.md >/dev/null
grep -F 'mz-009-validity-partition-pruning' BENCHMARK.md >/dev/null
grep -F 'Operator: "VALID_AT"' SQL_TEMPORAL_VALIDITY.md >/dev/null
grep -F 'frontier metadata and cross-node coordination remain caller-owned' ENGINE_IDEAS.md >/dev/null
grep -F '## MZ-009 implementation update' CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md >/dev/null
