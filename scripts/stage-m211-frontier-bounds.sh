#!/usr/bin/env bash
set -euo pipefail

git add -- \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	M211_SQL_FRONTIER_BOUNDS.md \
	Makefile \
	hat/hatSql/m210_retained_sql_snapshot.go \
	hat/hatSql/m211_frontier_bounds_baseline_benchmark_test.go \
	hat/hatSql/m211_frontier_bounds_benchmark_test.go \
	hat/hatSql/m211_frontier_bounds_test.go \
	hat/hatSql/sql_as_of.go \
	hat/hatSql/sql_distributed_frontier.go \
	hat/hatSql/sql_frontier_bounds.go \
	hat/hatSql/sql_frontier_snapshot_provider.go \
	hat/hatSql/typed_table_mvcc.go \
	scripts/benchmark-m211-frontier-bounds-baseline.sh \
	scripts/benchmark-m211-frontier-bounds.sh \
	scripts/commit-m211-frontier-bounds.sh \
	scripts/format-m211-frontier-bounds.sh \
	scripts/push-m211-frontier-bounds.sh \
	scripts/race-m211-frontier-bounds.sh \
	scripts/stage-m211-frontier-bounds.sh \
	scripts/test-m211-frontier-bounds-package.sh \
	scripts/test-m211-frontier-bounds.sh \
	scripts/verify-m211-frontier-bounds.sh \
	scripts/vet-m211-frontier-bounds.sh
