#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  ENGINE_IDEAS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  TYPED_TABLE_ARRANGEMENT_TELEMETRY.md \
  hat/hatSql/typed_table.go \
  hat/hatSql/typed_table_aggregate_dictionary.go \
  hat/hatSql/typed_table_arrangement_stats.go \
  hat/hatSql/typed_table_arrangement_stats_benchmark_test.go \
  hat/hatSql/typed_table_arrangement_stats_test.go \
  scripts/format-mz027-arrangement-stats.sh \
  scripts/test-mz027-arrangement-stats.sh \
  scripts/benchmark-mz027-baseline.sh \
  scripts/benchmark-mz027-arrangement-stats.sh \
  scripts/test-race-mz027-arrangement-stats.sh \
  scripts/vet-mz027-arrangement-stats.sh \
  scripts/verify-mz027-docs.sh \
  scripts/review-mz027-arrangement-stats.sh \
  scripts/commit-mz027-arrangement-stats.sh \
  scripts/push-mz027-arrangement-stats.sh
git commit -m 'hatSql: expose arrangement memory telemetry'
