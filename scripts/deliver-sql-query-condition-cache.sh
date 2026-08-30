#!/bin/sh
set -eu

files='
BENCHMARK.md
Makefile
README.md
hat/hatSql/condition_cache.go
hat/hatSql/contracts.go
hat/hatSql/query.go
hat/hatSql/query_condition_cache_benchmark_test.go
hat/hatSql/query_condition_cache_test.go
scripts/benchmark-sql-query-condition-cache.sh
scripts/deliver-sql-query-condition-cache.sh
scripts/format-sql-query-condition-cache.sh
scripts/inspect-sql-condition-cache-integration.sh
scripts/test-sql-query-condition-cache.sh
'

git diff --check -- $files
git add -- $files
git diff --cached --check
git commit -m 'perf(sql): cache versioned columnar conditions'
git push
