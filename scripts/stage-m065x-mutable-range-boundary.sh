#!/usr/bin/env bash
set -euo pipefail
git add -- \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
	INCREMENTAL_MUTABLE_RANGE_BOUNDARY_WINDOW.md \
	INSPIRATION.md \
	Makefile \
	hat/hatSql/m065x_mutable_range_boundary_window.go \
	hat/hatSql/m065x_mutable_range_boundary_window_test.go \
	hat/hatSql/m065x_mutable_range_boundary_window_benchmark_test.go \
	scripts/benchmark-m065x-mutable-range-boundary.sh \
	scripts/commit-m065x-mutable-range-boundary.sh \
	scripts/format-m065x-mutable-range-boundary.sh \
	scripts/push-m065x-mutable-range-boundary.sh \
	scripts/review-m065x-mutable-range-boundary.sh \
	scripts/stage-m065x-mutable-range-boundary.sh \
	scripts/test-m065x-mutable-range-boundary.sh \
	scripts/verify-m065x-mutable-range-boundary.sh
git diff --cached --check
git diff --cached --stat
