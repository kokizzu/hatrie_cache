#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU34_PER_SPACE_WAL_SYNC.md \
  hat/hatCache/journal.go \
  hat/hatCache/tu34_space_sync_policy.go \
  hat/hatCache/tu34_space_sync_policy_benchmark_test.go \
  hat/hatCache/tu34_space_sync_policy_test.go \
  scripts/benchmark-tu34.sh \
  scripts/format-tu34.sh \
  scripts/race-tu34.sh \
  scripts/ship-tu34.sh \
  scripts/status-tu34.sh \
  scripts/test-tu34-package.sh \
  scripts/test-tu34.sh \
  scripts/vet-tu34.sh
git diff --cached --check
git commit -m 'feat: add per-space WAL sync policies [skip ci]'
git push -u origin HEAD
