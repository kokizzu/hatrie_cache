#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	Makefile \
	README.md \
	T232_SPACE_TRANSACTIONS.md \
	hat/hatDataStructure/lsm_table.go \
	hat/hatDataStructure/space_transaction.go \
	hat/hatDataStructure/t232_space_transaction_benchmark_test.go \
	hat/hatDataStructure/t232_space_transaction_test.go \
	scripts/benchmark-t232-before.sh \
	scripts/benchmark-t232.sh \
	scripts/format-t232.sh \
	scripts/race-t232.sh \
	scripts/stage-t232.sh \
	scripts/test-t232-package.sh \
	scripts/test-t232.sh \
	scripts/verify-t232.sh \
	scripts/vet-t232.sh
git diff --cached --check
