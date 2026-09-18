#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/typed_table.go \
  hat/hatSql/ch010_materialized_default_test.go \
  hat/hatSql/ch010_materialized_default_benchmark_test.go
