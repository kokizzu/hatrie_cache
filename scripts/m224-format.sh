#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/materialized.go \
  hat/hatSql/m224_materialized_view_hydration_benchmark_test.go \
  hat/hatSql/m224_materialized_view_hydration_progress_test.go
