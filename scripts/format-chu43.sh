#!/bin/sh
set -eu
gofmt -w hat/hatSql/approx_aggregate.go hat/hatSql/query.go hat/hatSql/ch043_tdigest_state.go hat/hatSql/chu43_tdigest_state_test.go hat/hatSql/chu43_tdigest_state_benchmark_test.go
