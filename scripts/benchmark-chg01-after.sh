#!/bin/sh
set -eu

output=/tmp/hatrie-cache-chg01-after.txt
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHG01' -benchmem -benchtime=100ms -count=5 > "$output"
cat "$output"
