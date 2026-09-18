#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	README.md \
	BENCHMARK.md \
	INSPIRATION_BACKLOG.md \
	MZ021_SINK_IDEMPOTENCY_TOKENS.md \
	hat/hatSql/sql_sink_exactly_once.go \
	hat/hatSql/mz021_sink_idempotency_token.go \
	hat/hatSql/mz021_sink_idempotency_token_test.go \
	hat/hatSql/mz021_sink_idempotency_token_benchmark_test.go \
	scripts/test-mz021-sink-idempotency.sh \
	scripts/format-mz021-sink-idempotency.sh \
	scripts/benchmark-mz021-sink-idempotency.sh \
	scripts/verify-mz021-sink-idempotency-docs.sh \
	scripts/test-mz021-sink-idempotency-package.sh \
	scripts/race-mz021-sink-idempotency.sh \
	scripts/vet-mz021-sink-idempotency.sh \
	scripts/stage-mz021-sink-idempotency.sh \
	scripts/commit-mz021-sink-idempotency.sh \
	scripts/push-mz021-sink-idempotency.sh

git diff --cached --check
git diff --cached --stat
git status --short
