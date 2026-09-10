#!/usr/bin/env bash
set -eu

git diff --check
git status --short
git diff --stat -- \
	Makefile \
	README.md \
	INSPIRATION.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	INCREMENTAL_NTH_VALUE_WINDOW.md \
	hat/hatSql/incremental_nth_value_window.go \
	hat/hatSql/m065f_incremental_nth_value_window_test.go \
	hat/hatSql/m065f_incremental_nth_value_window_benchmark_test.go \
	hat/hatSql/m065f_incremental_nth_value_window_example_test.go \
	scripts/test-m065f-incremental-nth-value-window.sh \
	scripts/format-m065f-incremental-nth-value-window.sh \
	scripts/test-race-m065f-incremental-nth-value-window.sh \
	scripts/vet-m065f-incremental-nth-value-window.sh \
	scripts/benchmark-m065f-incremental-nth-value-window.sh \
	scripts/review-m065f-incremental-nth-value-window.sh \
	scripts/commit-m065f-incremental-nth-value-window.sh \
	scripts/push-m065f-incremental-nth-value-window.sh
