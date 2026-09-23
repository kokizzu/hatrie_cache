#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/materialized.go hat/hatSql/m220_materialized_view_index_lifecycle_test.go hat/hatSql/m220_materialized_view_index_lifecycle_internal_test.go hat/hatSql/m220_materialized_view_index_lifecycle_benchmark_test.go
