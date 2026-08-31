#!/bin/sh
set -eu

git add BENCHMARK.md Makefile hat/hatCache/sql_columnar_topn_dictionary_in_benchmark_test.go hat/hatCache/sql_columnar_topn_dictionary_in_test.go hat/hatSql/query.go scripts/benchmark-sql-columnar-topn-dictionary-in.sh scripts/commit-sql-columnar-topn-dictionary-in.sh scripts/format-sql-columnar-topn-dictionary-in.sh scripts/stage-sql-columnar-topn-dictionary-in.sh scripts/test-sql-columnar-topn-dictionary-in.sh
git diff --cached --name-status
git diff --cached --check
