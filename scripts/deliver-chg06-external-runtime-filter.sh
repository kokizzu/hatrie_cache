#!/usr/bin/env bash
set -euo pipefail

files=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  CHU14_RUNTIME_JOIN_FILTER.md
  Makefile
  PRODUCT_IDEA_GAPS.md
  README.md
  hat/hatSql/chg06_external_runtime_filter_benchmark_test.go
  hat/hatSql/query.go
  hat/hatSql/runtime_join_filter_test.go
  scripts/benchmark-chg06-external-runtime-filter.sh
  scripts/check-chg06-diff.sh
  scripts/deliver-chg06-external-runtime-filter.sh
  scripts/format-chg06-external-runtime-filter.sh
  scripts/race-chg06-package.sh
  scripts/test-chg06-external-runtime-filter.sh
  scripts/test-chg06-package.sh
  scripts/test-chg06-related.sh
  scripts/vet-chg06-package.sh
)

git add "${files[@]}"
git diff --cached --check
git diff --cached --stat
git commit -m 'feat(sql): stream runtime filters for external joins [skip ci]'
git push origin HEAD
