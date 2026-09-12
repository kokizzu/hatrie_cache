#!/usr/bin/env sh
set -eu

printf '%s\n' 'running multikey index benchmark'
go test -run '^$' -bench '^BenchmarkStringMultikeyIndexLookup$' -benchmem -count=5 ./hat/hatDataStructure
