#!/usr/bin/env bash
set -euo pipefail
git add -- \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
	INCREMENTAL_MUTABLE_RANGE_NTH_VALUE_WINDOW.md \
	INSPIRATION.md \
	Makefile \
	hat/hatSql/m065y_mutable_range_nth_value_window.go \
	hat/hatSql/m065y_mutable_range_nth_value_window_test.go \
	hat/hatSql/m065y_mutable_range_nth_value_window_benchmark_test.go \
	scripts/benchmark-m065y-mutable-range-nth-value.sh \
	scripts/commit-m065y-mutable-range-nth-value.sh \
	scripts/format-m065y-mutable-range-nth-value.sh \
	scripts/push-m065y-mutable-range-nth-value.sh \
	scripts/review-m065y-mutable-range-nth-value.sh \
	scripts/stage-m065y-mutable-range-nth-value.sh \
	scripts/test-m065y-mutable-range-nth-value.sh \
	scripts/verify-m065y-mutable-range-nth-value.sh
git diff --cached --check
git diff --cached --stat
