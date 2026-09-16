#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHG01' -benchmem -benchtime=100ms -count=5
