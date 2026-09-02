#!/usr/bin/env bash
set -euo pipefail

count="${BENCH_COUNT:-5}"
iterations="${BENCH_ITERS:-20}"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableArrangementHydration$' -benchmem -benchtime="${iterations}x" -count="${count}"
