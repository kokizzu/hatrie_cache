#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/materialized.go \
  hat/hatSql/materialized_point_lookup.go \
  hat/hatSql/materialized_point_lookup_test.go \
  hat/hatSql/materialized_point_lookup_benchmark_test.go
