#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/materialized.go \
  hat/hatSql/m217_materialized_view_point_lookup_benchmark_test.go \
  hat/hatSql/m217_materialized_view_point_lookup_test.go
