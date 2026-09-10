#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  INCREMENTAL_RECURSIVE_REACHABILITY.md \
  INSPIRATION.md \
  Makefile \
  README.md \
  hat/hatSql/recursive_reachability.go \
  hat/hatSql/recursive_reachability_benchmark_test.go \
  hat/hatSql/recursive_reachability_test.go \
  scripts/benchmark-m064-recursive-reachability.sh \
  scripts/commit-m064-recursive-reachability.sh \
  scripts/format-m064-recursive-reachability.sh \
  scripts/push-m064-recursive-reachability.sh \
  scripts/review-m064-recursive-reachability.sh \
  scripts/test-m064-recursive-reachability.sh \
  scripts/test-race-m064-recursive-reachability.sh
git diff --cached --check
git commit -m "feat(sql): add incremental recursive reachability"
