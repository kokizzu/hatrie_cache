#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  T216_STORAGE_COMPACTION_SCHEDULER.md \
  hat/hatDataStructure/spillable_arrangement.go \
  hat/hatDataStructure/storage_space.go \
  hat/hatDataStructure/storage_space_compaction.go \
  hat/hatDataStructure/t216_storage_space_compaction_test.go \
  hat/hatDataStructure/t216_storage_space_compaction_benchmark_test.go \
  scripts/format-t216.sh \
  scripts/benchmark-t216.sh \
  scripts/race-t216.sh \
  scripts/vet-t216.sh \
  scripts/verify-docs-t216.sh \
  scripts/stage-t216.sh \
  scripts/commit-t216.sh \
  scripts/push-t216.sh \
  scripts/test-t216.sh

git diff --cached --name-status
git diff --cached --check
