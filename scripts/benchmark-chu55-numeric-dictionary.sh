#!/usr/bin/env bash
set -euo pipefail

count=${CHU55_BENCH_COUNT:-5}
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHU55' -benchmem -count="$count"
