#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  MZ029_SPILLABLE_ARRANGEMENT.md \
  README.md \
  Makefile \
  hat/hatDataStructure/spillable_arrangement.go \
  hat/hatDataStructure/spillable_arrangement_test.go \
  hat/hatDataStructure/spillable_arrangement_benchmark_test.go \
  hat/hatDataStructure/spillable_arrangement_compaction_benchmark_test.go \
  scripts/format-mz028-adaptive-compaction.sh \
  scripts/test-mz028-adaptive-compaction.sh \
  scripts/benchmark-mz028-adaptive-compaction.sh \
  scripts/race-mz028-adaptive-compaction.sh \
  scripts/vet-mz028-adaptive-compaction.sh \
  scripts/review-mz028-adaptive-compaction.sh \
  scripts/stage-mz028-adaptive-compaction.sh \
  scripts/commit-mz028-adaptive-compaction.sh \
  scripts/push-mz028-adaptive-compaction.sh
git diff --cached --check
git diff --cached --stat
