#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  MZ029_SPILLABLE_ARRANGEMENT.md \
  Makefile \
  hat/hatDataStructure/spillable_arrangement.go \
  hat/hatDataStructure/spillable_arrangement_test.go \
  hat/hatDataStructure/spillable_arrangement_benchmark_test.go \
  hat/hatDataStructure/spillable_arrangement_compaction_benchmark_test.go \
  scripts/benchmark-mz028-adaptive-compaction.sh \
  scripts/commit-mz028-adaptive-compaction.sh \
  scripts/push-mz028-adaptive-compaction.sh \
  scripts/commit-mz029-spillable-arrangement.sh \
  scripts/format-mz029-spillable-arrangement.sh
git diff --cached --check
git diff --cached --stat
git commit -m "feat: skip redundant spill compaction [skip ci]"
