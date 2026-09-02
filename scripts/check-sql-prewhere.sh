#!/bin/sh
set -eu

git diff --check -- \
	Makefile \
	hat/hatSql/query.go \
	hat/hatSql/prewhere.go \
	hat/hatSql/prewhere_test.go \
	hat/hatSql/prewhere_benchmark_test.go \
	BENCHMARK.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	scripts/test-sql-prewhere.sh \
	scripts/test-race-sql-prewhere.sh \
	scripts/benchmark-sql-prewhere.sh \
	scripts/format-sql-prewhere.sh \
	scripts/check-sql-prewhere.sh \
	scripts/deliver-sql-prewhere.sh

git diff --cached --check -- \
	Makefile \
	hat/hatSql/query.go \
	hat/hatSql/prewhere.go \
	hat/hatSql/prewhere_test.go \
	hat/hatSql/prewhere_benchmark_test.go \
	BENCHMARK.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	scripts/test-sql-prewhere.sh \
	scripts/test-race-sql-prewhere.sh \
	scripts/benchmark-sql-prewhere.sh \
	scripts/format-sql-prewhere.sh \
	scripts/check-sql-prewhere.sh \
	scripts/deliver-sql-prewhere.sh

git status --short -- \
	Makefile \
	hat/hatSql/query.go \
	hat/hatSql/prewhere.go \
	hat/hatSql/prewhere_test.go \
	hat/hatSql/prewhere_benchmark_test.go \
	BENCHMARK.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	scripts/test-sql-prewhere.sh \
	scripts/test-race-sql-prewhere.sh \
	scripts/benchmark-sql-prewhere.sh \
	scripts/format-sql-prewhere.sh \
	scripts/check-sql-prewhere.sh \
	scripts/deliver-sql-prewhere.sh
