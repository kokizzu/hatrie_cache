#!/usr/bin/env bash
set -euo pipefail

paths=(
  Makefile
  README.md
  PRODUCT_IDEA_GAPS.md
  BENCHMARK.md
  TU23_TUPLE_MULTIKEY_INDEX.md
  hat/hatDataStructure/tuple_multikey_index.go
  hat/hatDataStructure/t_u23_tuple_multikey_index_test.go
  hat/hatDataStructure/t_u23_tuple_multikey_index_benchmark_test.go
  hat/hatDataStructure/t_u23_tuple_multikey_baseline_test.go
  scripts/benchmark-t-u23-baseline.sh
  scripts/benchmark-t-u23.sh
  scripts/format-t-u23.sh
  scripts/test-t-u23.sh
  scripts/test-t-u23-package.sh
  scripts/race-t-u23.sh
  scripts/vet-t-u23.sh
  scripts/review-t-u23.sh
  scripts/deliver-t-u23.sh
)

git diff --check -- "${paths[@]}"
git add -- "${paths[@]}"
git commit -m "feat: add automatic tuple multikey index [skip ci]"
git push -u origin HEAD
