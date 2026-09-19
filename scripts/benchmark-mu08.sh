#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkDifferentialTemporalJoin(Load|Compact)$' -benchmem -count=5 -benchtime=100ms
