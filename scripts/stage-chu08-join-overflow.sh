#!/bin/sh
set -eu

git add -- \
	Makefile \
	README.md \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	C229_JOIN_OVERFLOW_POLICY.md \
	hat/hatSql/join_overflow.go \
	hat/hatSql/c229_join_overflow_policy_test.go \
	hat/hatSql/c229_join_overflow_policy_benchmark_test.go \
	hat/hatSql/query.go \
	hat/hatSql/join_order_stats.go \
	hat/hatSql/sql_result_cache.go \
	scripts/test-chu08-join-overflow.sh \
	scripts/format-chu08-join-overflow.sh \
	scripts/benchmark-chu08-join-overflow.sh \
	scripts/test-chu08-join-overflow-package.sh \
	scripts/race-chu08-join-overflow.sh \
	scripts/vet-chu08-join-overflow.sh \
	scripts/verify-chu08-join-overflow.sh \
	 scripts/status-chu08-join-overflow.sh \
	scripts/review-chu08-join-overflow.sh \
	scripts/review-staged-chu08-join-overflow.sh \
	scripts/stage-chu08-join-overflow.sh \
	scripts/commit-chu08-join-overflow.sh \
	scripts/push-chu08-join-overflow.sh
