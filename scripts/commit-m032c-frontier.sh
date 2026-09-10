#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
	INSPIRATION.md \
	Makefile \
	README.md \
	SQL_SOURCE_FRONTIERS.md \
	hat/hatSql/sql_frontier_barrier_benchmark_test.go \
	hat/hatSql/sql_source_frontier_barrier.go \
	hat/hatSql/sql_source_frontier_barrier_test.go \
	scripts/benchmark-m032c-frontier.sh \
	scripts/commit-m032c-frontier.sh \
	scripts/format-m032c-frontier.sh \
	scripts/push-m032c-frontier.sh \
	scripts/review-m032c-frontier.sh \
	scripts/test-m032c-frontier.sh \
	scripts/test-race-m032c-frontier.sh
git diff --cached --check
git commit -m "feat(sql): add source frontier barrier"
