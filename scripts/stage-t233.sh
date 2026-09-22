#!/usr/bin/env bash
set -euo pipefail

git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	Makefile \
	README.md \
	TT033_MVCC_COOPERATIVE_YIELD.md \
	hat/hatCache/t233_transaction_yield.go \
	hat/hatCache/t233_transaction_yield_test.go \
	hat/hatCache/t233_transaction_yield_benchmark_test.go \
	scripts/benchmark-t233.sh \
	scripts/commit-t233.sh \
	scripts/format-t233.sh \
	scripts/push-t233.sh \
	scripts/race-t233.sh \
	scripts/stage-t233.sh \
	scripts/test-t233-package.sh \
	scripts/test-t233.sh \
	scripts/verify-t233-docs.sh \
	scripts/vet-t233.sh
git diff --cached --check
git diff --cached --stat
