#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatSql/contracts.go \
  hat/hatSql/query.go \
  hat/hatSql/query_profiler.go \
  hat/hatSql/c234_query_profiler_test.go \
  hat/hatSql/c234_query_profiler_baseline_benchmark_test.go
