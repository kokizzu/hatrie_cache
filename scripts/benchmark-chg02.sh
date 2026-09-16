#!/bin/sh
set -eu
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHG02' -benchmem -benchtime=200ms -count=5
