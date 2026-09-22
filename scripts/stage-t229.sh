#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  TT029_BEFORE_REPLACE.md \
  hat/hatDataStructure/memtx_table.go \
  hat/hatDataStructure/t229_before_replace_benchmark_test.go \
  hat/hatDataStructure/t229_before_replace_test.go \
  scripts/audit-inspiration.sh \
  scripts/benchmark-t229-before.sh \
  scripts/benchmark-t229.sh \
  scripts/commit-t229.sh \
  scripts/format-t229.sh \
  scripts/push-t229.sh \
  scripts/race-t229.sh \
  scripts/stage-t229.sh \
  scripts/test-t229-package.sh \
  scripts/test-t229.sh \
  scripts/verify-t229-docs.sh \
  scripts/vet-t229.sh

git diff --cached --check
git diff --cached --name-status
