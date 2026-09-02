#!/bin/sh
set -eu

count="${BENCH_COUNT:-5}"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLPrewhere' -benchmem -count="$count"
