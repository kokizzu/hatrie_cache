#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  DIFFERENTIAL_GROUP_BY.md \
  INSPIRATION.md \
  BENCHMARK.md \
  hat/hatSql/differential_group_by.go \
  hat/hatSql/differential_min_max.go \
  hat/hatSql/differential_min_max_benchmark_test.go \
  hat/hatSql/differential_min_max_test.go \
  scripts/benchmark-differential-group-min-max.sh \
  scripts/commit-differential-group-min-max.sh \
  scripts/format-differential-group-min-max.sh \
  scripts/test-differential-group-min-max.sh \
  scripts/verify-differential-group-min-max.sh
git diff --cached --check
git diff --cached --stat
git commit -m "hatSql: add differential grouped min and max"
git push origin HEAD:master
