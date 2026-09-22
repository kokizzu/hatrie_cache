#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	T248_RETRY_DEAD_LETTER.md \
	hat/hatDataStructure/retrying_deduplicating_priority_visibility_queue.go \
	hat/hatDataStructure/t248_retrying_deduplicating_priority_visibility_queue_benchmark_test.go \
	hat/hatDataStructure/t248_retrying_deduplicating_priority_visibility_queue_test.go \
	scripts/benchmark-t248-retrying-queue-baseline.sh \
	scripts/benchmark-t248-retrying-queue.sh \
	scripts/format-t248-retrying-queue.sh \
	scripts/memory-t248-retrying-queue.sh \
	scripts/race-t248-retrying-queue.sh \
	scripts/stage-t248-retrying-queue.sh \
	scripts/commit-t248-retrying-queue.sh \
	scripts/push-t248-retrying-queue.sh \
	scripts/test-t248-retrying-queue-package.sh \
	scripts/test-t248-retrying-queue.sh \
	scripts/vet-t248-retrying-queue.sh

git status --short
