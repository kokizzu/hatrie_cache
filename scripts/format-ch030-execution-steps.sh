#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatSql/query.go \
  hat/hatSql/sql_result_cache.go \
  hat/hatSql/ch030_execution_steps_test.go \
  hat/hatSql/ch030_execution_steps_baseline_benchmark_test.go \
  hat/hatSql/ch030_execution_steps_benchmark_test.go
