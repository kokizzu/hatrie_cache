#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  Makefile \
  MZ011_PARTITION_OFFSET_FRONTIERS.md \
  README.md \
  hat/hatPipeline/mz011_partition_offset_frontier.go \
  hat/hatPipeline/mz011_partition_offset_frontier_benchmark_test.go \
  hat/hatPipeline/mz011_partition_offset_frontier_test.go \
  scripts/benchmark-mz011-partition-frontier.sh \
  scripts/commit-mz011-partition-frontier.sh \
  scripts/format-mz011-partition-frontier.sh \
  scripts/push-mz011-partition-frontier.sh \
  scripts/race-mz011-partition-frontier.sh \
  scripts/review-mz011-partition-frontier.sh \
  scripts/stage-mz011-partition-frontier.sh \
  scripts/test-mz011-package.sh \
  scripts/test-mz011-partition-frontier.sh \
  scripts/verify-mz011-docs.sh \
  scripts/vet-mz011-partition-frontier.sh
git diff --cached --check
git diff --cached --name-status
