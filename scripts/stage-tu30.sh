#!/usr/bin/env bash
set -euo pipefail

git add -- \
	BENCHMARK.md \
	Makefile \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	TU30_COOPERATIVE_FIBER_SCHEDULER.md \
	hat/hatFiber/scheduler.go \
	hat/hatFiber/tu30_scheduler_benchmark_test.go \
	hat/hatFiber/tu30_scheduler_test.go \
	scripts/benchmark-tu30.sh \
	scripts/commit-tu30.sh \
	scripts/format-tu30.sh \
	scripts/push-tu30.sh \
	scripts/race-tu30.sh \
	scripts/review-tu30.sh \
	scripts/stage-tu30.sh \
	scripts/test-tu30.sh \
	scripts/vet-tu30.sh
