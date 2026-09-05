#!/usr/bin/env bash
set -euo pipefail

count="${BENCH_COUNT:-5}"
go test ./hat/hatSql -run '^$' -bench 'BenchmarkSQL(RowBinary|JSON)' -benchmem -count="$count"
