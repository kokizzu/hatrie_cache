#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  T234_TRANSACTION_CONFLICTS.md \
  hat/hatDataStructure/space.go \
  hat/hatDataStructure/space_conflict.go \
  hat/hatDataStructure/space_transaction.go \
  hat/hatDataStructure/t234_conflict_baseline_benchmark_test.go \
  hat/hatDataStructure/t234_conflict_benchmark_test.go \
  hat/hatDataStructure/t234_space_conflict_test.go \
  scripts/benchmark-t234-before.sh \
  scripts/benchmark-t234-direct.sh \
  scripts/benchmark-t234-quick.sh \
  scripts/benchmark-t234.sh \
  scripts/commit-t234.sh \
  scripts/format-t234.sh \
  scripts/push-t234.sh \
  scripts/race-t234.sh \
  scripts/stage-t234.sh \
  scripts/test-t234-package.sh \
  scripts/test-t234.sh \
  scripts/vet-t234.sh
git diff --cached --check
