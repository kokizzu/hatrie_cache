#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU16_MEMTX_TABLE.md \
  hat/hatDataStructure/memtx_table.go \
  hat/hatDataStructure/tu16_memtx_baseline_benchmark_test.go \
  hat/hatDataStructure/tu16_memtx_table_test.go \
  scripts/benchmark-tu16.sh \
  scripts/commit-tu16.sh \
  scripts/format-tu16.sh \
  scripts/push-tu16.sh \
  scripts/race-tu16.sh \
  scripts/test-tu16-memtx.sh \
  scripts/test-tu16-package.sh \
  scripts/verify-tu16.sh \
  scripts/vet-tu16.sh
git commit -m "feat: add selectable memtx row table"
