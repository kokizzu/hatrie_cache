#!/usr/bin/env sh
set -eu

sh ./scripts/format-sql-columnar-limit-pushdown.sh
sh ./scripts/test-sql-columnar-limit-pushdown.sh
sh ./scripts/test-sql-columnar-scans.sh
sh ./scripts/verify-sql-improvement-goal.sh
sh ./scripts/verify-benchmark-md-update.sh
go test ./...
git diff --check
git add BENCHMARK.md Makefile hat/hatSql/columnar_limit_pushdown_benchmark_test.go hat/hatSql/columnar_limit_pushdown_test.go hat/hatSql/query.go scripts/benchmark-sql-columnar-limit-pushdown.sh scripts/deliver-sql-columnar-limit-pushdown.sh scripts/format-sql-columnar-limit-pushdown.sh scripts/test-sql-columnar-limit-pushdown.sh
git commit --only -m 'perf(sql): stop unobserved columnar scans at limit' -- BENCHMARK.md Makefile hat/hatSql/columnar_limit_pushdown_benchmark_test.go hat/hatSql/columnar_limit_pushdown_test.go hat/hatSql/query.go scripts/benchmark-sql-columnar-limit-pushdown.sh scripts/deliver-sql-columnar-limit-pushdown.sh scripts/format-sql-columnar-limit-pushdown.sh scripts/test-sql-columnar-limit-pushdown.sh
git push
