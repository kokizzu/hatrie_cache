#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	README.md \
	BENCHMARK.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_ROUND2.md \
	TT034_EARLY_TRANSACTION_CONFLICTS.md \
	hat/hatCache/sql_transaction.go \
	hat/hatCache/sql_transaction_options.go \
	hat/hatCache/t234_early_conflict_test.go \
	hat/hatCache/t234_early_conflict_benchmark_test.go \
	scripts/format-t234.sh \
	scripts/test-t234.sh \
	scripts/test-t234-package.sh \
	scripts/benchmark-t234.sh \
	scripts/race-t234.sh \
	scripts/vet-t234.sh \
	scripts/verify-t234-docs.sh \
	scripts/stage-t234.sh \
	scripts/commit-t234.sh \
	scripts/push-t234.sh

git diff --cached --check
git diff --cached --stat
