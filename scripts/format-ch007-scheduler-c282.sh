#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatSql/ch007_ttl_scheduler_baseline_benchmark_test.go \
  hat/hatSql/ch007_ttl_scheduler_benchmark_test.go \
  hat/hatSql/ch007_ttl_scheduler_test.go \
  hat/hatSql/typed_table_ttl_scheduler.go \
  hat/hatSql/typed_table_ttl_snapshot.go
