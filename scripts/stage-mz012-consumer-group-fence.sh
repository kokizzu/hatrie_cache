#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  MZ012_CONSUMER_GROUP_FENCING.md \
  Makefile \
  README.md \
  hat/hatPipeline/mz012_consumer_group_fence.go \
  hat/hatPipeline/mz012_consumer_group_fence_benchmark_test.go \
  hat/hatPipeline/mz012_consumer_group_fence_test.go \
  scripts/benchmark-mz012-baseline.sh \
  scripts/benchmark-mz012-consumer-group-fence.sh \
  scripts/commit-mz012-consumer-group-fence.sh \
  scripts/format-mz012-consumer-group-fence.sh \
  scripts/push-mz012-consumer-group-fence.sh \
  scripts/race-mz012-consumer-group-fence.sh \
  scripts/review-mz012-consumer-group-fence.sh \
  scripts/stage-mz012-consumer-group-fence.sh \
  scripts/test-mz012-all.sh \
  scripts/test-mz012-consumer-group-fence.sh \
  scripts/test-mz012-package.sh \
  scripts/verify-mz012-docs.sh \
  scripts/vet-mz012-consumer-group-fence.sh
git diff --cached --check
git status --short
