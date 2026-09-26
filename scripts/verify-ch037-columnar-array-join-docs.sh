#!/usr/bin/env bash
set -euo pipefail

for file in CH037_COLUMNAR_ARRAY_JOIN.md BENCHMARK.md ENGINE_IDEAS.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md README.md; do
	test -s "$file"
done

grep -Fq 'CH-037' CH037_COLUMNAR_ARRAY_JOIN.md
grep -Fq 'ch-037-columnar-array-join' BENCHMARK.md
grep -Fq 'CH-037 physical columnar ARRAY JOIN' ENGINE_IDEAS.md
grep -Fq 'CH-037 implementation update' CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
grep -Fq 'CH037_COLUMNAR_ARRAY_JOIN.md' README.md
