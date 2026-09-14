#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'Working-tree status:'
git status --short
printf '%s\n' 'Makefile diff:'
git diff -- Makefile
printf '%s\n' 'HEAD Makefile tail:'
git show HEAD:Makefile | tail -n 10
printf '%s\n' 'Feature diff stat:'
git diff --stat -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  DIFFERENTIAL_GROUP_BY.md \
  DIFFERENTIAL_OPERATORS.md \
  INSPIRATION.md \
  hat/hatSql/differential_average.go \
  hat/hatSql/differential_average_test.go \
  hat/hatSql/differential_average_benchmark_test.go \
  scripts/benchmark-differential-average.sh \
  scripts/format-differential-average.sh \
  scripts/review-differential-average.sh \
  scripts/test-differential-average.sh \
  scripts/verify-differential-average.sh
printf '%s\n' 'Whitespace check:'
git diff --check
