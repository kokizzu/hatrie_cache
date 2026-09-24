#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/contracts.go \
  hat/hatSql/query.go \
  hat/hatSql/time_zone.go \
  hat/hatCache/main.go \
  hat/hatCache/sql_validity_index.go \
  hat/hatCache/mz009_temporal_validity_index_test.go \
  hat/hatCache/mz009_temporal_validity_index_benchmark_test.go
