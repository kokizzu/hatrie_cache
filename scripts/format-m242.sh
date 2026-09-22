#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/contracts.go \
  hat/hatSql/query.go \
  hat/hatSql/slow_query.go \
  hat/hatSql/m242_operator_metrics_test.go \
  hat/hatSql/m242_operator_metrics_benchmark_test.go \
  hat/hatSql/m242_operator_metrics_current_benchmark_test.go
