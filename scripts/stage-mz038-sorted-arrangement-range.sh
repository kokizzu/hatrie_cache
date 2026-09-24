#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	ENGINE_IDEAS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	MZ038_SORTED_ARRANGEMENT_RANGE.md \
	hat/hatSql/typed_table_sorted_arrangement.go \
	hat/hatSql/mz038_sorted_arrangement_range.go \
	hat/hatSql/mz038_sorted_arrangement_range_test.go \
	hat/hatSql/mz038_sorted_arrangement_range_benchmark_test.go \
	scripts/test-mz038-sorted-arrangement-range.sh \
	scripts/test-package-mz038-sorted-arrangement-range.sh \
	scripts/benchmark-mz038-sorted-arrangement-range.sh \
	scripts/format-mz038-sorted-arrangement-range.sh \
	scripts/race-mz038-sorted-arrangement-range.sh \
	scripts/vet-mz038-sorted-arrangement-range.sh \
	scripts/stage-mz038-sorted-arrangement-range.sh \
	scripts/commit-mz038-sorted-arrangement-range.sh \
	scripts/push-mz038-sorted-arrangement-range.sh \
	scripts/status-mz038-sorted-arrangement-range.sh
