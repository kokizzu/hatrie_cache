#!/bin/sh
set -eu

files='
Makefile
README.md
BENCHMARK.md
hat/hatSql/query.go
hat/hatSql/metrics_byte_accounting_test.go
hat/hatSql/metrics_byte_accounting_benchmark_test.go
scripts/audit-sql-metrics-byte-accounting.sh
scripts/benchmark-sql-metrics-byte-accounting.sh
scripts/deliver-sql-metrics-byte-accounting.sh
scripts/format-sql-typed-composite.sh
scripts/inspect-sql-metrics-byte-docs.sh
scripts/test-sql-metrics-byte-accounting.sh
'

git diff --check -- $files
git add -- $files
git diff --cached --check
git commit -m 'perf(sql): skip byte accounting without metrics'
git push
