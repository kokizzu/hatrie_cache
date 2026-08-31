#!/bin/sh
set -eu

gofmt -w hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/columnar_string_bloom_segment_test.go hat/hatCache/sql_columnar_layout_cache.go hat/hatCache/sql_columnar_string_bloom_segment_test.go hat/hatCache/sql_columnar_string_bloom_segment_benchmark_test.go
