#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  DIFFERENTIAL_DIFFERENCE.md \
  DIFFERENTIAL_OPERATORS.md \
  INSPIRATION.md \
  Makefile \
  hat/hatSql/differential_difference.go \
  hat/hatSql/differential_difference_benchmark_test.go \
  hat/hatSql/differential_difference_test.go \
  scripts/benchmark-differential-except.sh \
  scripts/commit-differential-except.sh \
  scripts/format-differential-except.sh \
  scripts/test-differential-except.sh \
  scripts/verify-differential-except.sh
git diff --cached --check
git diff --cached --stat
git commit -m "hatSql: add optimized differential EXCEPT"
git push origin HEAD:master
