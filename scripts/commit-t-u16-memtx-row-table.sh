#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  T-U16_MEMTX_ROW_TABLE.md \
  hat/hatDataStructure/memtx_row_table.go \
  hat/hatDataStructure/t_u16_memtx_row_table_benchmark_test.go \
  hat/hatDataStructure/t_u16_memtx_row_table_test.go \
  scripts/benchmark-t-u16-memtx-row-table.sh \
  scripts/commit-t-u16-memtx-row-table.sh \
  scripts/format-t-u16-memtx-row-table.sh \
  scripts/push-t-u16-memtx-row-table.sh \
  scripts/test-t-u16-memtx-row-table.sh \
  scripts/verify-t-u16-memtx-row-table.sh

git diff --cached --check
git commit -m 'feat: add selectable memtx row table [skip ci]'
