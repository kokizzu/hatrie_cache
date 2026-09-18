#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  BENCHMARK.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  INSPIRATION.md \
  INSPIRATION_BACKLOG.md \
  TR021_ONLINE_SECONDARY_INDEX.md \
  hat/hatSchema/materialized.go \
  hat/hatSchema/tr021_online_secondary_index_baseline_benchmark_test.go \
  hat/hatSchema/tr021_online_secondary_index_test.go \
  hat/hatSchema/tr021_online_secondary_index_benchmark_test.go \
  scripts/benchmark-tr021-secondary-index-baseline.sh \
  scripts/benchmark-tr021-online-secondary-index.sh \
  scripts/format-tr021-online-secondary-index.sh \
  scripts/race-tr021-online-secondary-index.sh \
  scripts/test-tr021-online-secondary-index.sh \
  scripts/test-tr021-sql-package.sh \
  scripts/vet-tr021-online-secondary-index.sh \
  scripts/verify-tr021-online-secondary-index.sh \
  scripts/stage-tr021-online-secondary-index.sh \
  scripts/commit-tr021-online-secondary-index.sh \
  scripts/push-tr021-online-secondary-index.sh
