#!/bin/sh
set -eu

git add Makefile VISIBILITY_QUEUE.md INSPIRATION.md \
  hat/hatDataStructure/visibility_queue.go \
  hat/hatDataStructure/visibility_queue_epoch_test.go \
  hat/hatDataStructure/visibility_queue_epoch_benchmark_test.go \
  scripts/remote-status.sh \
  scripts/test-visibility-queue-epoch.sh \
  scripts/benchmark-visibility-queue-epoch.sh \
  scripts/format-visibility-queue-epoch.sh \
  scripts/race-visibility-queue-epoch.sh \
  scripts/vet-visibility-queue-epoch.sh \
  scripts/review-visibility-queue-epoch.sh \
  scripts/commit-visibility-queue-epoch.sh \
  scripts/push-visibility-queue-epoch.sh
git diff --cached --check
git commit -m "feat: add epoch-fenced visibility queue leases"
