#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/session.go \
  hat/hatSql/materialized.go \
  hat/hatSql/ch011_projection_ddl_test.go \
  hat/hatSql/ch011_projection_ddl_benchmark_test.go
