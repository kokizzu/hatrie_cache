#!/bin/sh
set -eu

make format-sql-columnar-dictionary-segment-skip
make test-sql-columnar-dictionary-segment-skip
make test-sql-columnar-group-aggregate
make test-sql-columnar-query-rows
make benchmark-sql-columnar-dictionary-segment-skip
make verify-benchmark-md-update
git diff --check -- BENCHMARK.md Makefile hat/hatCache/sql_columnar_layout_cache.go hat/hatCache/sql_columnar_segment_skip_benchmark_test.go hat/hatCache/sql_columnar_segment_skip_test.go hat/hatSql/columnar_segment_skip_test.go hat/hatSql/contracts.go hat/hatSql/query.go scripts/benchmark-sql-columnar-segment-skip.sh scripts/deliver-sql-columnar-dictionary-segment-skip.sh scripts/test-sql-columnar-segment-skip.sh
git add BENCHMARK.md Makefile hat/hatCache/sql_columnar_layout_cache.go hat/hatCache/sql_columnar_segment_skip_benchmark_test.go hat/hatCache/sql_columnar_segment_skip_test.go hat/hatSql/columnar_segment_skip_test.go hat/hatSql/contracts.go hat/hatSql/query.go scripts/benchmark-sql-columnar-segment-skip.sh scripts/deliver-sql-columnar-dictionary-segment-skip.sh scripts/test-sql-columnar-segment-skip.sh
git commit -m "perf(sql): skip low-cardinality dictionary segments"
git push
