#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatSql/ch_u41_prepared_cache_persistence.go \
  hat/hatSql/ch_u41_prepared_cache_persistence_test.go \
  hat/hatSql/ch_u41_prepared_cache_persistence_benchmark_test.go \
  hat/hatCache/sql_prepared_cache_persistence.go
