#!/bin/sh
set -eu

make format-sql-columnar-string-bloom-segment
make test-sql-columnar-string-bloom-segment
make test-sql-columnar-scans
make test-sql-columnar-query-rows
make benchmark-sql-columnar-string-bloom-segment
make verify-benchmark-md-update
git diff --check -- BENCHMARK.md Makefile hat/hatCache/sql_columnar_layout_cache.go hat/hatCache/sql_columnar_string_bloom_segment_benchmark_test.go hat/hatCache/sql_columnar_string_bloom_segment_test.go hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/columnar_string_bloom_segment_test.go scripts/benchmark-sql-columnar-string-bloom-segment.sh scripts/format-sql-columnar-string-bloom-segment.sh scripts/test-sql-columnar-string-bloom-segment.sh scripts/deliver-sql-columnar-string-bloom-segment.sh
git add BENCHMARK.md Makefile hat/hatCache/sql_columnar_layout_cache.go hat/hatCache/sql_columnar_string_bloom_segment_benchmark_test.go hat/hatCache/sql_columnar_string_bloom_segment_test.go hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/columnar_string_bloom_segment_test.go scripts/benchmark-sql-columnar-string-bloom-segment.sh scripts/format-sql-columnar-string-bloom-segment.sh scripts/test-sql-columnar-string-bloom-segment.sh scripts/deliver-sql-columnar-string-bloom-segment.sh
git commit -m "perf(sql): skip high-cardinality string segments"
git push
