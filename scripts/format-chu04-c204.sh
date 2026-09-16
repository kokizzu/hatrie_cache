#!/bin/sh
set -eu

gofmt -w \
  hat/hatCache/c204_projection_idempotency_test.go \
  hat/hatCache/sql_incremental_projection.go \
  hat/hatSql/c204_projection_idempotency_benchmark_test.go \
  hat/hatSql/incremental_projection.go \
  hat/hatSql/materialized.go
