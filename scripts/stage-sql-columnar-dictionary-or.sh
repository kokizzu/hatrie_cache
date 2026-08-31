#!/bin/sh
set -eu

git add BENCHMARK.md Makefile hat/hatCache/sql_columnar_dictionary_or_benchmark_test.go hat/hatCache/sql_columnar_dictionary_or_test.go hat/hatSql/query.go scripts/benchmark-sql-columnar-dictionary-or.sh scripts/commit-sql-columnar-dictionary-or.sh scripts/format-sql-columnar-dictionary-or.sh scripts/stage-sql-columnar-dictionary-or.sh scripts/test-sql-columnar-dictionary-or.sh
git diff --cached --name-status
git diff --cached --check
