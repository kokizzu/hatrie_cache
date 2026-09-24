#!/usr/bin/env bash
set -euo pipefail

paths=(
  Makefile
  SQL_TEMPORAL_VALIDITY.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  hat/hatSql/contracts.go
  hat/hatSql/query.go
  hat/hatSql/time_zone.go
  hat/hatCache/main.go
  hat/hatCache/sql_validity_index.go
  hat/hatCache/mz009_temporal_validity_index_test.go
  hat/hatCache/mz009_temporal_validity_index_benchmark_test.go
  scripts/format-mz009-validity-index.sh
  scripts/test-mz009-validity-index.sh
  scripts/test-mz009-validity-index-package.sh
  scripts/benchmark-mz009-validity-baseline.sh
  scripts/benchmark-mz009-validity-index.sh
  scripts/race-mz009-validity-index.sh
  scripts/vet-mz009-validity-index.sh
  scripts/review-mz009-validity-index.sh
  scripts/commit-mz009-validity-index.sh
  scripts/push-mz009-validity-index.sh
)

git add -- "${paths[@]}"
git diff --cached --check
git commit -m "feat: add SQL temporal validity index"
