#!/bin/sh
set -eu

make format-sql-columnar-segment-skip
make test-sql-columnar-segment-skip
make test-sql-borrowed-columnar-source
make test-sql-columnar-query-rows
make test-sql-columnar-group-aggregate
make verify-benchmark-md-update
make verify-sql-improvement-goal
git diff --check -- BENCHMARK.md Makefile README.md hat/hatCache/sql_columnar_borrowed_benchmark_test.go hat/hatCache/sql_columnar_layout_cache.go hat/hatCache/sql_columnar_segment_skip_benchmark_test.go hat/hatCache/sql_columnar_segment_skip_test.go hat/hatCache/sql_query.go hat/hatSql/columnar_segment_skip_test.go hat/hatSql/contracts.go hat/hatSql/query.go scripts/benchmark-sql-columnar-segment-skip.sh scripts/deliver-sql-columnar-segment-skip.sh scripts/format-sql-columnar-segment-skip.sh scripts/test-sql-columnar-segment-skip.sh
git add BENCHMARK.md Makefile README.md hat/hatCache/sql_columnar_borrowed_benchmark_test.go hat/hatCache/sql_columnar_layout_cache.go hat/hatCache/sql_columnar_segment_skip_benchmark_test.go hat/hatCache/sql_columnar_segment_skip_test.go hat/hatCache/sql_query.go hat/hatSql/columnar_segment_skip_test.go hat/hatSql/contracts.go hat/hatSql/query.go scripts/benchmark-sql-columnar-segment-skip.sh scripts/deliver-sql-columnar-segment-skip.sh scripts/format-sql-columnar-segment-skip.sh scripts/test-sql-columnar-segment-skip.sh
git commit -m "perf(sql): skip numeric columnar segments"
git push
