#!/bin/sh
set -eu

GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkC229JoinOverflowPolicy/' -benchmem -cpu=1 -count=5
