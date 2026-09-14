#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m029_incremental_interval_join.go \
  hat/hatSql/m029_incremental_interval_join_test.go \
  hat/hatSql/m029_incremental_interval_join_benchmark_test.go
