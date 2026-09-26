#!/usr/bin/env bash
set -euo pipefail

for file in BENCHMARK.md CH038_OR_NULL_COLUMNAR.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md ENGINE_IDEAS.md; do
	test -s "$file"
done

grep -F 'CH038' CH038_OR_NULL_COLUMNAR.md >/dev/null
grep -F 'COUNT_OR_NULL' BENCHMARK.md >/dev/null
