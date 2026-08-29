#!/bin/sh
set -eu

git add -- Makefile hat/hatSql/query.go hat/hatSql/execution_arena_test.go hat/hatSql/execution_arena_benchmark_test.go hat/hatCache/sql_execution_arena_test.go scripts/test-sql-execution-arena.sh scripts/format-sql-execution-arena.sh scripts/benchmark-sql-execution-arena.sh scripts/review-sql-execution-arena.sh scripts/commit-sql-execution-arena.sh
git diff --cached --check
git commit -m "perf: reuse SQL execution rows per query"
git push origin master
