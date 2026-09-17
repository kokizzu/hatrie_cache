#!/bin/sh
set -eu
GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ030LookupJoinBaseline$' -benchmem -benchtime=100ms -count=5 -cpu=1
