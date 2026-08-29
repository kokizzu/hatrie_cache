#!/usr/bin/env sh
set -eu

files='Makefile
hat/hatSql/result_cache.go
hat/hatSql/result_cache_test.go
hat/hatSql/result_cache_benchmark_test.go
scripts/audit-sql-parallel-contention.sh
scripts/inspect-sql-cache-locks.sh
scripts/inspect-sql-result-cache.sh
scripts/test-sql-result-cache.sh
scripts/benchmark-sql-result-cache.sh
scripts/format-sql-result-cache-clone.sh
scripts/review-sql-result-cache-clone.sh
scripts/commit-sql-result-cache-clone.sh'

git add -- $files
git diff --cached --check
git commit --only -m 'perf: clone SQL result-cache hits structurally' -- $files
git push origin master
