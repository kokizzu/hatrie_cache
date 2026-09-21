#!/usr/bin/env bash
set -euo pipefail

git add -- \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	Makefile \
	MZ026_ADAPTIVE_DICTIONARY.md \
	hat/hatSql/mz026_adaptive_dictionary_arrangement_benchmark_test.go \
	hat/hatSql/mz026_adaptive_dictionary_arrangement_test.go \
	hat/hatSql/typed_table_sorted_arrangement.go \
	hat/hatSql/typed_table_sorted_arrangement_dictionary.go \
	scripts/benchmark-mz026-after.sh \
	scripts/benchmark-mz026-before.sh \
	scripts/commit-mz026-adaptive.sh \
	scripts/push-mz026-adaptive.sh \
	scripts/format-mz026-adaptive.sh \
	scripts/race-mz026-adaptive.sh \
	scripts/review-mz026-adaptive.sh \
	scripts/stage-mz026-adaptive.sh \
	scripts/status-mz026-adaptive.sh \
	scripts/test-mz026-adaptive.sh \
	scripts/test-mz026-package.sh \
	scripts/vet-mz026-adaptive.sh
