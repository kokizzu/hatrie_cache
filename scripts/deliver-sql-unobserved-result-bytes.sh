#!/bin/sh
set -eu

files='
Makefile
README.md
BENCHMARK.md
hat/hatSql/query.go
hat/hatSql/metrics_byte_accounting_test.go
scripts/deliver-sql-unobserved-result-bytes.sh
scripts/inspect-sql-observation.sh
scripts/test-sql-metrics-byte-accounting.sh
'

git diff --check -- $files
git add -- $files
git diff --cached --check
git commit -m 'perf(sql): skip result bytes without observers'
git push
