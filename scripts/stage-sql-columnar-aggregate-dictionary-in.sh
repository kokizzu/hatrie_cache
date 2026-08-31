#!/bin/sh
set -eu

git add BENCHMARK.md Makefile hat/hatCache/sql_columnar_aggregate_dictionary_in_benchmark_test.go hat/hatCache/sql_columnar_aggregate_dictionary_in_test.go hat/hatSql/query.go scripts/benchmark-sql-columnar-aggregate-dictionary-in.sh scripts/commit-sql-columnar-aggregate-dictionary-in.sh scripts/format-sql-columnar-aggregate-dictionary-in.sh scripts/stage-sql-columnar-aggregate-dictionary-in.sh scripts/test-sql-columnar-aggregate-dictionary-in.sh
git diff --cached --name-status
git diff --cached --check
