#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatCache/sql_query.go hat/hatCache/sql_columnar_raw_bytes_test.go hat/hatCache/sql_columnar_raw_bytes_benchmark_test.go scripts/inspect-sql-source-ownership.sh scripts/format-sql-columnar-raw-bytes.sh scripts/test-sql-columnar-raw-bytes.sh scripts/benchmark-sql-columnar-raw-bytes.sh scripts/review-sql-columnar-raw-bytes.sh scripts/commit-sql-columnar-raw-bytes.sh
git add -- Makefile hat/hatCache/sql_query.go hat/hatCache/sql_columnar_raw_bytes_test.go hat/hatCache/sql_columnar_raw_bytes_benchmark_test.go scripts/inspect-sql-source-ownership.sh scripts/format-sql-columnar-raw-bytes.sh scripts/test-sql-columnar-raw-bytes.sh scripts/benchmark-sql-columnar-raw-bytes.sh scripts/review-sql-columnar-raw-bytes.sh scripts/commit-sql-columnar-raw-bytes.sh
git commit --only -m 'perf: decode SQL raw bytes without source clone' -- Makefile hat/hatCache/sql_query.go hat/hatCache/sql_columnar_raw_bytes_test.go hat/hatCache/sql_columnar_raw_bytes_benchmark_test.go scripts/inspect-sql-source-ownership.sh scripts/format-sql-columnar-raw-bytes.sh scripts/test-sql-columnar-raw-bytes.sh scripts/benchmark-sql-columnar-raw-bytes.sh scripts/review-sql-columnar-raw-bytes.sh scripts/commit-sql-columnar-raw-bytes.sh
git push origin master
