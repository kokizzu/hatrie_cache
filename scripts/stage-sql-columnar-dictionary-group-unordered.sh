#!/bin/sh
set -eu

git add BENCHMARK.md Makefile hat/hatCache/sql_columnar_dictionary_group_unordered_benchmark_test.go hat/hatCache/sql_columnar_dictionary_group_unordered_test.go hat/hatSql/query.go scripts/benchmark-sql-columnar-dictionary-group-unordered.sh scripts/commit-sql-columnar-dictionary-group-unordered.sh scripts/format-sql-columnar-dictionary-group-unordered.sh scripts/stage-sql-columnar-dictionary-group-unordered.sh scripts/test-sql-columnar-dictionary-group-unordered.sh
git diff --cached --name-status
git diff --cached --check
