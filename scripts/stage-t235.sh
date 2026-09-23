#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	Makefile \
	README.md \
	T235_COOPERATIVE_WORKER_POOL.md \
	hat/hatFiber/t235_worker_baseline_benchmark_test.go \
	hat/hatFiber/t235_worker_pool_benchmark_test.go \
	hat/hatFiber/t235_worker_pool_edge_test.go \
	hat/hatFiber/t235_worker_pool_test.go \
	hat/hatFiber/worker_pool.go \
	scripts/benchmark-t235-before-one.sh \
	scripts/benchmark-t235-before.sh \
	scripts/benchmark-t235.sh \
	scripts/commit-t235.sh \
	scripts/push-t235.sh \
	scripts/format-t235.sh \
	scripts/race-t235.sh \
	scripts/stage-t235.sh \
	scripts/test-t235-package.sh \
	scripts/test-t235.sh \
	scripts/vet-t235.sh
