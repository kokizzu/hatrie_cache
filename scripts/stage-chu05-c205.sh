#!/bin/sh
set -eu

git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	C205_SUBQUERY_RESULT_CACHE.md \
	INSPIRATION_ROUND2.md \
	Makefile \
	README.md \
	hat/hatSql/c205_subquery_result_cache_benchmark_test.go \
	hat/hatSql/c205_subquery_result_cache_test.go \
	hat/hatSql/query.go \
	hat/hatSql/sql_result_cache.go \
	scripts/audit-chu05-c205.sh \
	scripts/benchmark-chu05-c205.sh \
	scripts/format-chu05-c205.sh \
	scripts/race-chu05-c205.sh \
	scripts/test-chu05-c205-package.sh \
	scripts/test-chu05-c205.sh \
	scripts/vet-chu05-c205.sh \
	scripts/stage-chu05-c205.sh \
	scripts/commit-chu05-c205.sh \
	scripts/push-chu05-c205.sh
git diff --cached --check
git status --short
