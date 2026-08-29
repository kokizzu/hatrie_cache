#!/bin/sh
set -eu

git diff --check
git add -- \
	Makefile \
	BENCHMARK.md \
	hat/hatCache/main.go \
	hat/hatCache/sql_query.go \
	hat/hatCache/sql_columnar_layout_cache.go \
	hat/hatCache/sql_columnar_layout_cache_test.go \
	hat/hatCache/sql_columnar_layout_cache_benchmark_test.go \
	scripts/test-sql-columnar-layout-cache.sh \
	scripts/benchmark-sql-columnar-layout-cache.sh \
	scripts/deliver-sql-columnar-layout-cache.sh
git diff --cached --name-only
git commit -m "add adaptive columnar layout cache"
git push
