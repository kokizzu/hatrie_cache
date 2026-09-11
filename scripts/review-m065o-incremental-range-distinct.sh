#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --check
git diff --stat
git diff -- Makefile INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md BENCHMARK.md INCREMENTAL_RANGE_WINDOW.md hat/hatSql/incremental_range_window.go hat/hatSql/m065o_incremental_range_distinct_benchmark_test.go hat/hatSql/m065o_incremental_range_distinct_test.go scripts/benchmark-m065o-incremental-range-distinct.sh scripts/format-m065o-incremental-range-distinct.sh scripts/test-m065o-incremental-range-distinct.sh scripts/test-race-m065o-incremental-range-distinct.sh scripts/vet-m065o-incremental-range-distinct.sh scripts/review-m065o-incremental-range-distinct.sh scripts/commit-m065o-incremental-range-distinct.sh scripts/push-m065o-incremental-range-distinct.sh scripts/inspect-sql-window-ideas-now.sh
