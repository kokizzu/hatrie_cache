#!/usr/bin/env sh
set -eu

make review-sql-columnar-dictionary
git add -- Makefile hat/hatSql/contracts.go hat/hatSql/query.go hat/hatCache/sql_query.go hat/hatCache/sql_columnar_scan_test.go hat/hatCache/sql_columnar_scan_benchmark_test.go scripts/inspect-columnar-implementation.sh scripts/locate-columnar-builder.sh scripts/inspect-columnar-builder.sh scripts/test-sql-columnar-scan.sh scripts/benchmark-sql-columnar-scan.sh scripts/format-sql-columnar-dictionary.sh scripts/review-sql-columnar-dictionary.sh scripts/commit-sql-columnar-dictionary.sh
git diff --cached --check
git commit --only -m 'perf: dictionary encode SQL columnar strings' -- Makefile hat/hatSql/contracts.go hat/hatSql/query.go hat/hatCache/sql_query.go hat/hatCache/sql_columnar_scan_test.go hat/hatCache/sql_columnar_scan_benchmark_test.go scripts/inspect-columnar-implementation.sh scripts/locate-columnar-builder.sh scripts/inspect-columnar-builder.sh scripts/test-sql-columnar-scan.sh scripts/benchmark-sql-columnar-scan.sh scripts/format-sql-columnar-dictionary.sh scripts/review-sql-columnar-dictionary.sh scripts/commit-sql-columnar-dictionary.sh
git push
