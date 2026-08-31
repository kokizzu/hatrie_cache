#!/usr/bin/env sh
set -eu

sh ./scripts/format-sql-columnar-topn.sh
sh ./scripts/test-sql-columnar-topn.sh
sh ./scripts/test-sql-columnar-scans.sh
sh ./scripts/verify-sql-improvement-goal.sh
sh ./scripts/verify-benchmark-md-update.sh
go test ./...
git diff --check
git add BENCHMARK.md Makefile hat/hatSql/columnar_topn_benchmark_test.go hat/hatSql/columnar_topn_multi_order_benchmark_test.go hat/hatSql/columnar_topn_test.go hat/hatSql/query.go scripts/benchmark-sql-columnar-topn.sh scripts/deliver-sql-columnar-topn.sh scripts/format-sql-columnar-topn.sh scripts/test-sql-columnar-topn.sh
git commit --only -m 'perf(sql): rank bounded columnar orders directly' -- BENCHMARK.md Makefile hat/hatSql/columnar_topn_benchmark_test.go hat/hatSql/columnar_topn_multi_order_benchmark_test.go hat/hatSql/columnar_topn_test.go hat/hatSql/query.go scripts/benchmark-sql-columnar-topn.sh scripts/deliver-sql-columnar-topn.sh scripts/format-sql-columnar-topn.sh scripts/test-sql-columnar-topn.sh
git push
