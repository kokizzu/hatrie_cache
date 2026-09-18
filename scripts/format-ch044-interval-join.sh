#!/usr/bin/env bash
set -eu

gofmt -w \
  hat/hatSql/m029_incremental_interval_join.go \
  hat/hatSql/ch044_interval_join_benchmark_test.go
