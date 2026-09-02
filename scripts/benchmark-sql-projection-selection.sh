#!/bin/sh
set -eu

count="${BENCHCOUNT:-5}"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkProjectionCatalog$' -benchmem -count="$count"
