#!/bin/sh
set -eu

git add \
	BENCHMARK.md \
	INSPIRATION_BACKLOG.md \
	Makefile \
	MZ037_WORKER_LOCAL_EXCHANGE.md \
	README.md \
	hat/hatPipeline/async_batcher.go \
	hat/hatPipeline/mz037_worker_local_exchange_benchmark_test.go \
	hat/hatPipeline/mz037_worker_local_exchange_test.go \
	hat/hatPipeline/worker_local_exchange.go \
	scripts/benchmark-mz037-after.sh \
	scripts/benchmark-mz037-before.sh \
	scripts/commit-mz037.sh \
	scripts/format-mz037.sh \
	scripts/inspect-mz037-batcher.sh \
	scripts/inspect-mz037-benchmark.sh \
	scripts/inspect-mz037-core.sh \
	scripts/inspect-mz037-docs.sh \
	scripts/inspect-mz037-exchange.sh \
	scripts/inspect-mz037-test-script.sh \
	scripts/inspect-mz037-test.sh \
	scripts/inspect-mz037-worker-benchmark.sh \
	scripts/inspect-mz037.sh \
	scripts/push-mz037.sh \
	scripts/race-mz037.sh \
	scripts/review-mz037.sh \
	scripts/test-mz037-full.sh \
	scripts/test-mz037.sh \
	scripts/vet-mz037.sh
git commit -m "chore: add mz037 workflow scripts"
