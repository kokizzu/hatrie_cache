#!/bin/sh
set -eu

git add BENCHMARK.md Makefile hat/hatCache/sql_columnar_dictionary_in_numeric_benchmark_test.go hat/hatCache/sql_columnar_dictionary_in_numeric_test.go hat/hatSql/query.go scripts/benchmark-sql-columnar-dictionary-in-numeric.sh scripts/commit-sql-columnar-dictionary-in-numeric.sh scripts/format-sql-columnar-dictionary-in-numeric.sh scripts/stage-sql-columnar-dictionary-in-numeric.sh scripts/test-sql-columnar-dictionary-in-numeric.sh
git diff --cached --name-status
git diff --cached --check
