#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/query.go \
  hat/hatSql/runtime_join_filter_test.go \
  hat/hatSql/runtime_join_filter_benchmark_test.go
