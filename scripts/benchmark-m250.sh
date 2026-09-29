#!/usr/bin/env bash
set -euo pipefail

count="${BENCH_COUNT:-5}"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM250(Direct|Aligned)TemporalJoin$' -benchmem -cpu=1 -count="$count"
