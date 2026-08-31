#!/bin/sh
set -eu

git add BENCHMARK.md Makefile hat/hatCache/sql_columnar_dictionary_like_benchmark_test.go hat/hatCache/sql_columnar_dictionary_like_test.go hat/hatSql/query.go scripts/benchmark-sql-columnar-dictionary-like.sh scripts/commit-sql-columnar-dictionary-like.sh scripts/format-sql-columnar-dictionary-like.sh scripts/stage-sql-columnar-dictionary-like.sh scripts/test-sql-columnar-dictionary-like.sh
git diff --cached --name-status
git diff --cached --check
