#!/bin/sh
set -eu

git add BENCHMARK.md Makefile hat/hatCache/sql_columnar_layout_cache.go hat/hatCache/sql_query.go hat/hatCache/sql_columnar_topn_layout_preference_benchmark_test.go hat/hatCache/sql_columnar_topn_layout_preference_test.go hat/hatSql/contracts.go hat/hatSql/query.go scripts/benchmark-sql-columnar-topn-layout-preference.sh scripts/commit-sql-columnar-topn-layout-preference.sh scripts/format-sql-columnar-topn-layout-preference.sh scripts/stage-sql-columnar-topn-layout-preference.sh scripts/test-sql-columnar-topn-layout-preference.sh
git diff --cached --name-status
git diff --cached --check
