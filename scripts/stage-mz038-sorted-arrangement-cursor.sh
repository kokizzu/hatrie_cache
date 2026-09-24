#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  MZ038_SORTED_ARRANGEMENT_RANGE.md \
  Makefile \
  hat/hatSql/mz038_sorted_arrangement_range.go \
  hat/hatSql/mz038_sorted_arrangement_range_benchmark_test.go \
  hat/hatSql/mz038_sorted_arrangement_range_test.go \
  scripts/benchmark-mz038-sorted-arrangement-cursor.sh \
  scripts/format-mz038-sorted-arrangement-cursor.sh \
  scripts/stage-mz038-sorted-arrangement-cursor.sh \
  scripts/test-mz038-sorted-arrangement-cursor.sh \
  scripts/test-mz038-sorted-arrangement-package.sh \
  scripts/verify-mz038-sorted-arrangement-cursor.sh \
  scripts/commit-mz038-sorted-arrangement-cursor.sh \
  scripts/push-mz038-sorted-arrangement-cursor.sh
