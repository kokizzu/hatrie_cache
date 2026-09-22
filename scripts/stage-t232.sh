#!/usr/bin/env bash
set -euo pipefail

git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	Makefile \
	README.md \
	TT032_ATOMIC_TRANSACTION_SCOPES.md \
	hat/hatCache/sql_transaction.go \
	hat/hatCache/sql_transaction_scope.go \
	hat/hatCache/t232_transaction_scope_test.go \
	hat/hatCache/t232_transaction_scope_benchmark_test.go \
	scripts/benchmark-t232.sh \
	scripts/commit-t232.sh \
	scripts/format-t232.sh \
	scripts/inspect-t232.sh \
	scripts/race-t232.sh \
	scripts/stage-t232.sh \
	scripts/status-t232.sh \
	scripts/test-t232-package.sh \
	scripts/test-t232-regression.sh \
	scripts/test-t232.sh \
	scripts/verify-t232-docs.sh \
	scripts/vet-t232.sh \
	scripts/push-t232.sh
git diff --cached --check
git diff --cached --stat
