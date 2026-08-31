#!/bin/sh
set -eu

files='
BENCHMARK.md
Makefile
hat/hatSql/columnar_distinct_benchmark_test.go
hat/hatSql/columnar_distinct_test.go
hat/hatSql/query.go
scripts/benchmark-sql-columnar-distinct.sh
scripts/deliver-sql-columnar-distinct.sh
scripts/format-sql-columnar-distinct.sh
scripts/test-sql-columnar-distinct.sh
'

sh ./scripts/format-sql-columnar-distinct.sh
sh ./scripts/test-sql-columnar-distinct.sh
sh ./scripts/benchmark-sql-columnar-distinct.sh
./scripts/verify-benchmark-md-update.sh
go test ./...
git diff --check -- $files
git add -- $files
git diff --cached --check
git commit -m 'perf(sql): read ordered dictionary distinct values directly'
git push
