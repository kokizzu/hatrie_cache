#!/bin/sh
set -eu

git diff -- \
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
	hat/hatSql/sql_result_cache.go
