#!/usr/bin/env sh
set -eu

files='Makefile
hat/hatSql/query.go
hat/hatSql/spill_parallel_merge_test.go
scripts/inspect-sql-spill-budget.sh
scripts/test-sql-spill-parallel-merge.sh
scripts/test-race-sql-spill-parallel-merge.sh
scripts/benchmark-sql-spill-parallel-merge.sh
scripts/format-sql-spill-parallel-merge.sh
scripts/review-sql-spill-parallel-merge.sh
scripts/commit-sql-spill-parallel-merge.sh'

git add -- $files
git diff --cached --check
git commit --only -m 'perf: parallelize SQL spill group merge passes' -- $files
git push origin master
