#!/usr/bin/env sh
set -eu

make review-sql-columnar-numeric-filter
git add -- Makefile hat/hatSql/query.go hat/hatCache/sql_columnar_scan_test.go hat/hatCache/sql_columnar_scan_benchmark_test.go scripts/audit-execution-efficiency-goal.sh scripts/inspect-execution-efficiency-goal.sh scripts/inspect-columnar-implementation.sh scripts/inspect-sql-expression-model.sh scripts/test-sql-columnar-scan.sh scripts/benchmark-sql-columnar-scan.sh scripts/format-sql-columnar-numeric-filter.sh scripts/review-sql-columnar-numeric-filter.sh scripts/commit-sql-columnar-numeric-filter.sh
git diff --cached --check
git commit --only -m 'perf: vectorize SQL columnar numeric filters' -- Makefile hat/hatSql/query.go hat/hatCache/sql_columnar_scan_test.go hat/hatCache/sql_columnar_scan_benchmark_test.go scripts/audit-execution-efficiency-goal.sh scripts/inspect-execution-efficiency-goal.sh scripts/inspect-columnar-implementation.sh scripts/inspect-sql-expression-model.sh scripts/test-sql-columnar-scan.sh scripts/benchmark-sql-columnar-scan.sh scripts/format-sql-columnar-numeric-filter.sh scripts/review-sql-columnar-numeric-filter.sh scripts/commit-sql-columnar-numeric-filter.sh
git push
