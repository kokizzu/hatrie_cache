#!/usr/bin/env bash
set -euo pipefail

git add Makefile INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md BENCHMARK.md INCREMENTAL_RANGE_WINDOW.md \
  hat/hatSql/incremental_range_window.go \
  hat/hatSql/m065p_incremental_range_average_benchmark_test.go \
  hat/hatSql/m065p_incremental_range_average_test.go \
  scripts/benchmark-m065p-incremental-range-average.sh \
  scripts/format-m065p-incremental-range-average.sh \
  scripts/test-m065p-incremental-range-average.sh \
  scripts/test-race-m065p-incremental-range-average.sh \
  scripts/vet-m065p-incremental-range-average.sh \
  scripts/review-m065p-incremental-range-average.sh \
  scripts/commit-m065p-incremental-range-average.sh \
  scripts/push-m065p-incremental-range-average.sh
git diff --cached --check
git commit -m 'add incremental range average'
