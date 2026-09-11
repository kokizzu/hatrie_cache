#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m052f_native_distinct_test.go \
  hat/hatSql/m052m_native_string_distinct_test.go \
  hat/hatSql/m052m_native_string_distinct_benchmark_test.go
