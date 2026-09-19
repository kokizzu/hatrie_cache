#!/bin/sh
set -eu
gofmt -w \
  hat/hatSql/approx_aggregate.go \
  hat/hatSql/ch042_approx_distinct_state.go \
  hat/hatSql/ch042_approx_distinct_state_test.go \
  hat/hatSql/ch042_approx_distinct_state_benchmark_test.go \
  hat/hatSql/query.go
