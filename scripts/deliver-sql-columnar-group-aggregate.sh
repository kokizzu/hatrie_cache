#!/bin/sh
set -eu

files='
BENCHMARK.md
Makefile
README.md
hat/hatSql/columnar_group_aggregate_benchmark_test.go
hat/hatSql/columnar_group_aggregate_test.go
hat/hatSql/query.go
scripts/benchmark-sql-columnar-group-aggregate.sh
scripts/deliver-sql-columnar-group-aggregate.sh
scripts/format-sql-columnar-group-aggregate.sh
scripts/inspect-sql-columnar-projection-aggregate.sh
scripts/test-sql-columnar-group-aggregate.sh
'

git diff --check -- $files
git add -- $files
git diff --cached --check
git commit -m 'perf(sql): group dictionary columns directly'
git push
