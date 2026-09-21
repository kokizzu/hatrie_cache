#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/sql_query.go \
  hat/hatSql/ch237_projection_explain.go \
  hat/hatSql/ch237_projection_explain_benchmark_test.go \
  hat/hatSql/ch237_projection_explain_test.go \
  hat/hatSql/model.go \
  hat/hatSql/query.go
