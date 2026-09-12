#!/bin/sh
set -eu
git diff --check
git status --short
git diff --stat -- Makefile README.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md VISIBILITY_QUEUE.md hat/hatDataStructure/visibility_queue.go hat/hatDataStructure/visibility_queue_test.go hat/hatDataStructure/visibility_queue_benchmark_test.go scripts/benchmark-visibility-queue.sh scripts/test-visibility-queue.sh scripts/format-visibility-queue.sh scripts/test-visibility-queue-package.sh scripts/race-visibility-queue.sh scripts/vet-visibility-queue.sh
