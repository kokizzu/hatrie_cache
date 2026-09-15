#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --stat -- BENCHMARK.md DELAY_QUEUE_POP_READY_FASTPATH.md INSPIRATION.md hat/hatDataStructure/delay_queue.go hat/hatDataStructure/delay_queue_pop_ready_fastpath_test.go scripts/test-delay-queue-c215.sh scripts/benchmark-delay-queue-c215.sh scripts/format-delay-queue-c215.sh scripts/verify-delay-queue-c215.sh scripts/inspect-delay-queue-diff-c215.sh scripts/stage-delay-queue-c215.sh scripts/commit-delay-queue-c215.sh scripts/push-delay-queue-c215.sh
git diff --check -- BENCHMARK.md DELAY_QUEUE_POP_READY_FASTPATH.md INSPIRATION.md hat/hatDataStructure/delay_queue.go hat/hatDataStructure/delay_queue_pop_ready_fastpath_test.go scripts/test-delay-queue-c215.sh scripts/benchmark-delay-queue-c215.sh scripts/format-delay-queue-c215.sh scripts/verify-delay-queue-c215.sh scripts/inspect-delay-queue-diff-c215.sh scripts/stage-delay-queue-c215.sh scripts/commit-delay-queue-c215.sh scripts/push-delay-queue-c215.sh
git diff --cached --stat
git diff --cached -- Makefile
