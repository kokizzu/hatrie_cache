#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/m219_background_point_lookup_test.go
gofmt -w hat/hatSql/m219_background_point_lookup_benchmark_test.go
gofmt -w hat/hatSql/materialized_point_lookup_build.go hat/hatSql/materialized_point_lookup.go hat/hatSql/materialized.go
