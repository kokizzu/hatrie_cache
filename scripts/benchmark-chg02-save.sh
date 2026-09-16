#!/bin/sh
set -eu
output=/tmp/hatrie-cache-chg02-after.txt
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHG02' -benchmem -benchtime=200ms -count=5 > "$output"
awk '{ print }' "$output"
