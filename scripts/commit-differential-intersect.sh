#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  DIFFERENTIAL_INTERSECT.md \
  DIFFERENTIAL_OPERATORS.md \
  INSPIRATION.md \
  Makefile \
  hat/hatSql/differential_intersect.go \
  hat/hatSql/differential_intersect_benchmark_test.go \
  hat/hatSql/differential_intersect_test.go \
  scripts/benchmark-differential-intersect.sh \
  scripts/commit-differential-intersect.sh \
  scripts/format-differential-intersect.sh \
  scripts/test-differential-intersect.sh \
  scripts/verify-differential-intersect.sh
git diff --cached --check
git diff --cached --stat
git commit -m "hatSql: add differential INTERSECT ALL"
git push origin HEAD:master
