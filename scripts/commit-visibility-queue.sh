#!/bin/sh
set -eu
git add Makefile README.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md VISIBILITY_QUEUE.md hat/hatDataStructure/visibility_queue.go hat/hatDataStructure/visibility_queue_test.go hat/hatDataStructure/visibility_queue_benchmark_test.go scripts/benchmark-visibility-queue.sh scripts/test-visibility-queue.sh scripts/format-visibility-queue.sh scripts/test-visibility-queue-package.sh scripts/race-visibility-queue.sh scripts/vet-visibility-queue.sh scripts/review-visibility-queue.sh scripts/commit-visibility-queue.sh scripts/push-visibility-queue.sh
git commit -m "feat: add visibility timeout queue"
