#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/typed_table_ttl_rollup.go \
  hat/hatSql/typed_table_ttl_scheduler.go \
  hat/hatSql/ch009_ttl_rollup_test.go \
  hat/hatSql/ch009_ttl_rollup_benchmark_test.go
