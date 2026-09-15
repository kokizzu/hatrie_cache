#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
    hat/hatSql/aggregate_state.go \
    hat/hatSql/ch036_aggregate_state_test.go \
    hat/hatSql/ch036_aggregate_state_benchmark_test.go \
    hat/hatSql/query.go
