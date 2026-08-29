#!/usr/bin/env sh
set -eu

files='Makefile
hat/hatCache/sql_query.go
hat/hatCache/sql_index_key_test.go
hat/hatCache/sql_index_key_benchmark_test.go
scripts/inspect-sql-index-keys.sh
scripts/test-sql-index-keys.sh
scripts/benchmark-sql-index-keys.sh
scripts/format-sql-index-keys.sh
scripts/review-sql-index-keys.sh
scripts/commit-sql-index-keys.sh'

git add -- $files
git diff --cached --check
git commit --only -m 'perf: use compact typed SQL index keys' -- $files
git push origin master
