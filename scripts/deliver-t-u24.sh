#!/usr/bin/env bash
set -euo pipefail

paths=(
  Makefile
  README.md
  PRODUCT_IDEA_GAPS.md
  BENCHMARK.md
  TU24_CONDITIONAL_INDEX_METADATA.md
  hat/hatSchema/space_catalog.go
  hat/hatSchema/t_u24_conditional_index_metadata_test.go
  hat/hatSchema/t_u24_conditional_index_metadata_benchmark_test.go
  scripts/format-t-u24.sh
  scripts/benchmark-t-u24.sh
  scripts/test-t-u24.sh
  scripts/test-t-u24-package.sh
  scripts/race-t-u24.sh
  scripts/vet-t-u24.sh
  scripts/review-t-u24.sh
  scripts/deliver-t-u24.sh
)

git diff --check -- "${paths[@]}"
git add -- "${paths[@]}"
git commit -m "feat: add conditional index metadata [skip ci]"
git push -u origin HEAD
