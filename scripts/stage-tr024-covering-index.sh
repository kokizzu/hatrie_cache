#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	BENCHMARK.md \
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
	INSPIRATION.md \
	INSPIRATION_BACKLOG.md \
	TR024_COVERING_INDEX.md \
	hat/hatSchema/materialized.go \
	hat/hatSchema/tr024_covering_index_baseline_benchmark_test.go \
	hat/hatSchema/tr024_covering_index_benchmark_test.go \
	hat/hatSchema/tr024_covering_index_test.go \
	scripts/benchmark-tr024-covering-index-baseline.sh \
	scripts/benchmark-tr024-covering-index.sh \
	scripts/commit-tr024-covering-index.sh \
	scripts/format-tr024-covering-index.sh \
	scripts/push-tr024-covering-index.sh \
	scripts/race-tr024-covering-index.sh \
	scripts/stage-tr024-covering-index.sh \
	scripts/test-tr024-covering-index.sh \
	scripts/test-tr024-sql-package.sh \
	scripts/verify-tr024-covering-index.sh \
	scripts/vet-tr024-covering-index.sh
