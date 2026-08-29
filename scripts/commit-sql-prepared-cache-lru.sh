#!/usr/bin/env sh
set -eu

files='Makefile
hat/hatSql/query.go
hat/hatSql/prepared_cache_test.go
hat/hatSql/prepared_cache_benchmark_test.go
scripts/inspect-sql-prepared-cache.sh
scripts/test-sql-prepared-cache.sh
scripts/benchmark-sql-prepared-cache.sh
scripts/format-sql-prepared-cache-lru.sh
scripts/review-sql-prepared-cache-lru.sh
scripts/commit-sql-prepared-cache-lru.sh'

git add -- $files
git diff --cached --check
git commit --only -m 'perf: make SQL prepared-cache LRU hits constant time' -- $files
git push origin master
