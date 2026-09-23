#!/usr/bin/env bash
set -euo pipefail

paths=(
  Makefile
  README.md
  BENCHMARK.md
  CH045_EXPLAIN_ESTIMATE.md
  hat/hatSql/query.go
  hat/hatSql/tooling.go
  hat/hatSql/ch045_explain_estimate_test.go
  hat/hatSql/ch045_explain_estimate_benchmark_test.go
  scripts/benchmark-chg45-baseline.sh
  scripts/benchmark-chg45.sh
  scripts/commit-chg45.sh
  scripts/format-chg45.sh
  scripts/race-chg45.sh
  scripts/push-chg45.sh
  scripts/test-chg45.sh
  scripts/test-chg45-related.sh
  scripts/test-chg45-package.sh
  scripts/verify-chg45-docs.sh
  scripts/vet-chg45.sh
)

git add -- "${paths[@]}"
git diff --cached --check
git commit --only -m "feat(sql): add explain estimate alias" -- "${paths[@]}"
