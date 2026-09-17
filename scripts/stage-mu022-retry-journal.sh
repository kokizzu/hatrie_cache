#!/usr/bin/env bash
set -euo pipefail

git add -- \
	BENCHMARK.md \
	MU022_CONNECTOR_TRANSACTION_RETRY_JOURNAL.md \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	Makefile \
	hat/hatSql/mu022_connector_transaction_retry_journal.go \
	hat/hatSql/mu022_retry_journal_baseline_benchmark_test.go \
	hat/hatSql/mu022_retry_journal_benchmark_test.go \
	hat/hatSql/mu022_retry_journal_test.go \
	scripts/benchmark-mu022-retry-journal-baseline.sh \
	scripts/benchmark-mu022-retry-journal.sh \
	scripts/format-mu022-retry-journal.sh \
	scripts/race-mu022-retry-journal.sh \
	scripts/test-mu022-package.sh \
	scripts/test-mu022-retry-journal.sh \
	scripts/verify-mu022-retry-journal.sh \
	scripts/vet-mu022-retry-journal.sh \
	scripts/review-mu022-retry-journal.sh \
	scripts/stage-mu022-retry-journal.sh \
	scripts/commit-mu022-retry-journal.sh \
	scripts/push-mu022-retry-journal.sh
git diff --cached --check
git diff --cached --stat
git status --short
