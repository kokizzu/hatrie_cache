#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU23_MULTIKEY_INDEX.md \
  hat/hatDataStructure/multikey_index.go \
  hat/hatDataStructure/string_multikey_index.go \
  hat/hatDataStructure/t_u23_multikey_index_test.go \
  hat/hatDataStructure/t_u23_multikey_baseline_benchmark_test.go \
  hat/hatDataStructure/t_u23_multikey_benchmark_test.go \
  scripts/benchmark-tu23-before.sh \
  scripts/benchmark-tu23-bounded-build.sh \
  scripts/benchmark-tu23-comparison.sh \
  scripts/benchmark-tu23-existing-build.sh \
  scripts/benchmark-tu23-existing-lookup.sh \
  scripts/benchmark-tu23.sh \
  scripts/deliver-tu23.sh \
  scripts/format-tu23.sh \
  scripts/race-tu23-package.sh \
  scripts/race-tu23.sh \
  scripts/status-tu23.sh \
  scripts/test-tu23-package.sh \
  scripts/test-tu23.sh \
  scripts/verify-tu23.sh \
  scripts/vet-tu23.sh
git diff --cached --check
git diff --cached --stat
git commit -m "feat: add bounded multikey indexes [skip ci]"
git push -u origin codex/tu23-multikey-index
