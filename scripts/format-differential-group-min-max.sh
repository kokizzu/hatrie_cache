#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/differential_min_max.go \
  hat/hatSql/differential_min_max_benchmark_test.go \
  hat/hatSql/differential_min_max_test.go \
  hat/hatSql/m037k_differential_string_min_max_benchmark_test.go \
  hat/hatSql/m037k_differential_string_min_max_test.go \
  hat/hatSql/m037l_differential_string_count_distinct_benchmark_test.go \
  hat/hatSql/m037l_differential_string_count_distinct_test.go
