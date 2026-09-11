#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	ENGINE_IDEAS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	README.md \
	BENCHMARK.md \
	CDC_ENVELOPES.md \
	hat/hatSql/cdc_envelope.go \
	hat/hatSql/cdc_envelope_test.go \
	hat/hatSql/cdc_envelope_benchmark_test.go \
	scripts/benchmark-mz015-cdc.sh \
	scripts/test-mz015-cdc.sh \
	scripts/format-mz015-cdc.sh \
	scripts/test-race-mz015-cdc.sh \
	scripts/test-mz015-broad.sh \
	scripts/verify-mz015-docs.sh \
	scripts/review-mz015.sh \
	scripts/status-mz015.sh \
	scripts/commit-mz015.sh \
	scripts/push-mz015.sh
git diff --cached --check
git commit -m 'Add CDC envelope normalization'
