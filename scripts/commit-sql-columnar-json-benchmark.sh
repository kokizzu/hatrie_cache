#!/usr/bin/env sh
set -eu

files='Makefile
hat/hatCache/sql_columnar_scan_test.go
hat/hatCache/sql_columnar_scan_benchmark_test.go
scripts/benchmark-sql-columnar-scan.sh
scripts/audit-sql-storage-allocation.sh
scripts/format-sql-columnar-json-benchmark.sh
scripts/review-sql-columnar-json-benchmark.sh
scripts/commit-sql-columnar-json-benchmark.sh'

git add -- $files
git diff --cached --check
git commit --only -m 'test: benchmark SQL columnar JSON batch building' -- $files
git push origin master
