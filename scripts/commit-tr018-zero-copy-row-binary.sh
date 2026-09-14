#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  TR018_ZERO_COPY_ROW_BINARY.md \
  hat/hatSql/tr018_zero_copy_row_binary.go \
  hat/hatSql/tr018_zero_copy_row_binary_test.go \
  hat/hatSql/tr018_zero_copy_row_binary_benchmark_test.go \
  hat/hatSql/tr018_zero_copy_row_binary_borrowed_benchmark_test.go \
  scripts/benchmark-tr018-zero-copy-row-binary.sh \
  scripts/commit-tr018-zero-copy-row-binary.sh \
  scripts/format-tr018-zero-copy-row-binary.sh \
  scripts/push-tr018-zero-copy-row-binary.sh \
  scripts/race-tr018-zero-copy-row-binary.sh \
  scripts/review-tr018-zero-copy-row-binary.sh \
  scripts/test-tr018-zero-copy-row-binary.sh \
  scripts/vet-tr018-zero-copy-row-binary.sh
git diff --cached --check
git commit -m "Add zero-copy RowBinary reader"
