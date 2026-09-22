#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/m220_point_lookup_retirement_test.go
gofmt -w hat/hatSql/m220_point_lookup_retirement_benchmark_test.go
gofmt -w hat/hatSql/materialized_point_lookup_retirement.go hat/hatSql/materialized_point_lookup.go hat/hatSql/materialized_point_lookup_build.go hat/hatSql/materialized.go
