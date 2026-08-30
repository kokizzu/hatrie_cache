#!/bin/sh
set -eu

files='
Makefile
README.md
BENCHMARK.md
hat/hatCache/monitoring.go
hat/hatCache/sql_query.go
hat/hatCache/sql_composite_range_borrowed_test.go
hat/hatSql/contracts.go
hat/hatSql/query.go
scripts/deliver-sql-composite-range-borrowed.sh
scripts/format-sql-typed-composite.sh
scripts/inspect-sql-indexed-row-ownership.sh
scripts/test-sql-composite-range-borrowed.sh
'

git diff --check -- $files
git add -- $files
git diff --cached --check
git commit -m 'perf(sql): borrow composite index candidates'
git push
