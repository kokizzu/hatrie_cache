#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	T247_DEDUPLICATING_QUEUE.md \
	hat/hatDataStructure/deduplicating_priority_visibility_queue.go \
	hat/hatDataStructure/t247_deduplicating_priority_visibility_queue_benchmark_test.go \
	hat/hatDataStructure/t247_deduplicating_priority_visibility_queue_test.go \
	scripts/benchmark-t247-deduplicating-queue-baseline.sh \
	scripts/benchmark-t247-deduplicating-queue.sh \
	scripts/format-t247-deduplicating-queue.sh \
	scripts/memory-t247-deduplicating-queue.sh \
	scripts/race-t247-deduplicating-queue.sh \
	scripts/test-t247-deduplicating-queue-package.sh \
	scripts/test-t247-deduplicating-queue.sh \
	scripts/vet-t247-deduplicating-queue.sh \
	scripts/stage-t247-deduplicating-queue.sh \
	scripts/commit-t247-deduplicating-queue.sh \
	scripts/push-t247-deduplicating-queue.sh

git status --short
