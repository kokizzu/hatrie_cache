#!/bin/sh
set -eu

files='
Makefile
README.md
BENCHMARK.md
hat/hatCache/monitoring.go
hat/hatCache/sql_query.go
hat/hatSql/contracts.go
hat/hatSql/query.go
hat/hatSql/metrics_byte_accounting_benchmark_test.go
hat/hatSql/source_borrowed_test.go
scripts/deliver-sql-borrowed-source.sh
scripts/format-sql-typed-composite.sh
scripts/profile-sql-metrics-disabled-query.sh
scripts/test-sql-borrowed-source.sh
'

git diff --check -- $files
git add -- $files
git diff --cached --check
git commit -m 'perf(sql): borrow immutable source snapshots'
git push
