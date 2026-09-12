#!/usr/bin/env sh
set -eu

printf '%s\n' 'running multikey index build benchmark'
go test -run '^$' -bench '^BenchmarkStringMultikeyIndex(Build|Lookup)$' -benchmem -count=3 ./hat/hatDataStructure
