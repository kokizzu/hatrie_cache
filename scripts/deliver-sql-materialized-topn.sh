#!/usr/bin/env sh
set -eu

sh ./scripts/format-sql-materialized-topn.sh
sh ./scripts/test-sql-materialized-order.sh
sh ./scripts/verify-sql-improvement-goal.sh
sh ./scripts/verify-benchmark-md-update.sh
go test ./...
git diff --check
git add BENCHMARK.md Makefile hat/hatCache/sql_materialized_topn_benchmark_test.go hat/hatCache/sql_production_test.go hat/hatSql/query.go scripts/benchmark-sql-materialized-topn.sh scripts/deliver-sql-materialized-topn.sh scripts/format-sql-materialized-topn.sh scripts/test-sql-materialized-order.sh
git commit --only -m 'perf(sql): stream materialized top-n orders' -- BENCHMARK.md Makefile hat/hatCache/sql_materialized_topn_benchmark_test.go hat/hatCache/sql_production_test.go hat/hatSql/query.go scripts/benchmark-sql-materialized-topn.sh scripts/deliver-sql-materialized-topn.sh scripts/format-sql-materialized-topn.sh scripts/test-sql-materialized-order.sh
git push
