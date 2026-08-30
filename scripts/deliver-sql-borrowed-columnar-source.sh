#!/bin/sh
set -eu

make format-sql-borrowed-columnar-source
make test-sql-borrowed-columnar-source
make test-sql-columnar-query-rows
make test-sql-columnar-group-aggregate
make verify-benchmark-md-update
make verify-sql-improvement-goal
git diff --check -- BENCHMARK.md Makefile README.md hat/hatCache/sql_columnar_borrowed_benchmark_test.go hat/hatCache/sql_columnar_borrowed_test.go hat/hatCache/sql_columnar_layout_cache.go hat/hatCache/sql_query.go hat/hatSql/columnar_source_borrowed_test.go hat/hatSql/contracts.go hat/hatSql/query.go scripts/benchmark-sql-borrowed-columnar-source.sh scripts/deliver-sql-borrowed-columnar-source.sh scripts/format-sql-borrowed-columnar-source.sh scripts/test-sql-borrowed-columnar-source.sh
git add BENCHMARK.md Makefile README.md hat/hatCache/sql_columnar_borrowed_benchmark_test.go hat/hatCache/sql_columnar_borrowed_test.go hat/hatCache/sql_columnar_layout_cache.go hat/hatCache/sql_query.go hat/hatSql/columnar_source_borrowed_test.go hat/hatSql/contracts.go hat/hatSql/query.go scripts/benchmark-sql-borrowed-columnar-source.sh scripts/deliver-sql-borrowed-columnar-source.sh scripts/format-sql-borrowed-columnar-source.sh scripts/test-sql-borrowed-columnar-source.sh
git commit -m "perf(sql): borrow immutable columnar layouts"
git push
