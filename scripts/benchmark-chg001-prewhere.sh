#!/usr/bin/env bash
set -euo pipefail

output="${CHG001_BENCHMARK_OUTPUT:-BENCHMARK_CHG001_RAW.txt}"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHG001' -benchmem -benchtime=200ms -count=5 | tee "$output"
