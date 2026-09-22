#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  TT031_AFTER_REPLACE_AUDIT.md \
  hat/hatDataStructure/memtx_table.go \
  hat/hatDataStructure/t231_after_replace_test.go \
  scripts/benchmark-t231.sh \
  scripts/commit-t231.sh \
  scripts/format-t231.sh \
  scripts/push-t231.sh \
  scripts/race-t231.sh \
  scripts/stage-t231.sh \
  scripts/test-t231.sh \
  scripts/verify-t231-docs.sh \
  scripts/vet-t231.sh

git diff --cached --check
git diff --cached --name-status
