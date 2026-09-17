#!/bin/sh
set -eu
GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHU22' -benchmem -cpu=1 -count=5
