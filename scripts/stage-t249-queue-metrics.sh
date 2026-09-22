#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  T249_QUEUE_METRICS.md \
  hat/hatDataStructure/deduplicating_priority_visibility_queue.go \
  hat/hatDataStructure/priority_visibility_queue.go \
  hat/hatDataStructure/retrying_deduplicating_priority_visibility_queue.go \
  hat/hatDataStructure/t249_queue_metrics_benchmark_test.go \
  hat/hatDataStructure/t249_queue_metrics_test.go \
  scripts/benchmark-t249-queue-metrics.sh \
  scripts/commit-t249-queue-metrics.sh \
  scripts/format-t249-queue-metrics.sh \
  scripts/memory-t249-queue-metrics.sh \
  scripts/race-t249-queue-metrics.sh \
  scripts/push-t249-queue-metrics.sh \
  scripts/stage-t249-queue-metrics.sh \
  scripts/test-t249-queue-metrics-package.sh \
  scripts/test-t249-queue-metrics.sh \
  scripts/vet-t249-queue-metrics.sh
git diff --cached --check
