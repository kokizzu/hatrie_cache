#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	Makefile \
	README.md \
	T233_MVCC_TRANSACTIONS.md \
	hat/hatDataStructure/space_mvcc.go \
	hat/hatDataStructure/space_transaction.go \
	hat/hatDataStructure/t233_space_mvcc_benchmark_test.go \
	hat/hatDataStructure/t233_space_mvcc_test.go \
	scripts/benchmark-t233-before.sh \
	scripts/benchmark-t233.sh \
	scripts/commit-t233.sh \
	scripts/format-t233.sh \
	scripts/push-t233.sh \
	scripts/race-t233.sh \
	scripts/stage-t233.sh \
	scripts/test-t233-package.sh \
	scripts/test-t233.sh \
	scripts/verify-t233.sh \
	scripts/vet-t233.sh
git diff --cached --check
