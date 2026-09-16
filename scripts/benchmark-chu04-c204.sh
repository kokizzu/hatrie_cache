#!/bin/sh
set -eu
go test ./hat/hatSql -run '^$' -bench '^BenchmarkC204ProjectionApply(Baseline|WithIdempotencyKeys)$' -benchtime=100ms -count=3 -benchmem
