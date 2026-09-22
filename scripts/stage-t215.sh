#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  README.md \
  T215_PER_SPACE_STORAGE_POLICY.md \
  hat/hatDataStructure/storage_space.go \
  hat/hatDataStructure/t215_storage_space_test.go \
  hat/hatDataStructure/t215_storage_space_benchmark_test.go \
  scripts/format-t215.sh \
  scripts/test-t215.sh \
  scripts/benchmark-t215.sh \
  scripts/race-t215.sh \
  scripts/vet-t215.sh \
  scripts/verify-docs-t215.sh \
  scripts/stage-t215.sh \
  scripts/commit-t215.sh \
  scripts/push-t215.sh
git diff --cached --check
git diff --cached --stat
