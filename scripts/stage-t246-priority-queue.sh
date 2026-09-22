#!/usr/bin/env bash
set -euo pipefail

git add -- \
	Makefile \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	T246_PRIORITY_QUEUE_STARVATION.md \
	hat/hatDataStructure/priority_visibility_queue.go \
	hat/hatDataStructure/priority_visibility_queue_test.go \
	hat/hatDataStructure/t246_priority_visibility_queue_test.go \
	hat/hatDataStructure/t246_priority_visibility_queue_benchmark_test.go \
	scripts/benchmark-t246-priority-queue.sh \
	scripts/benchmark-t246-priority-queue-baseline.sh \
	scripts/format-t246-priority-queue.sh \
	scripts/race-t246-priority-queue.sh \
	scripts/test-t246-priority-queue.sh \
	scripts/test-t246-priority-queue-package.sh \
	scripts/vet-t246-priority-queue.sh \
	scripts/stage-t246-priority-queue.sh \
	scripts/commit-t246-priority-queue.sh \
	scripts/push-t246-priority-queue.sh
git diff --cached --check
git diff --cached --stat -- \
	Makefile \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	T246_PRIORITY_QUEUE_STARVATION.md \
	hat/hatDataStructure/priority_visibility_queue.go \
	hat/hatDataStructure/priority_visibility_queue_test.go \
	hat/hatDataStructure/t246_priority_visibility_queue_test.go \
	hat/hatDataStructure/t246_priority_visibility_queue_benchmark_test.go \
	scripts/benchmark-t246-priority-queue.sh \
	scripts/benchmark-t246-priority-queue-baseline.sh \
	scripts/format-t246-priority-queue.sh \
	scripts/race-t246-priority-queue.sh \
	scripts/test-t246-priority-queue.sh \
	scripts/test-t246-priority-queue-package.sh \
	scripts/vet-t246-priority-queue.sh \
	scripts/stage-t246-priority-queue.sh \
	scripts/commit-t246-priority-queue.sh \
	scripts/push-t246-priority-queue.sh
