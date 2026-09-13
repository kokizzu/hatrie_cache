#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/model.go \
  hat/hatSql/mz050_plan_snapshot.go \
  hat/hatSql/mz050_plan_snapshot_test.go \
  hat/hatSql/mz050_plan_snapshot_benchmark_test.go \
  hat/hatSql/materialized.go \
  hat/hatSql/query.go \
  hat/hatSql/result_cache.go \
  hat/hatSql/keyset.go \
  hat/hatSql/sql_result_cache.go
