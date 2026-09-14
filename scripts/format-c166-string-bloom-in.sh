#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/query.go hat/hatCache/sql_columnar_string_bloom_segment_test.go hat/hatCache/sql_columnar_string_bloom_segment_benchmark_test.go
