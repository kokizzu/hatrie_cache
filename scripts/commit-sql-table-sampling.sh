#!/bin/sh
set -eu

git add BENCHMARK.md Makefile SQL.md hat/hatSql/query.go hat/hatSql/table_sample.go hat/hatSql/table_sample_test.go scripts/benchmark-sql-table-sampling.sh scripts/commit-sql-table-sampling.sh scripts/format-sql-table-sampling.sh scripts/inspect-sql-table-sampling.sh scripts/push-sql-table-sampling.sh scripts/show-sql-row-stream-fallback.sh scripts/show-sql-sampling-engine.sh scripts/test-sql-table-sampling.sh scripts/verify-sql-table-sampling.sh
git commit -m 'feat: add SQL table sampling'
