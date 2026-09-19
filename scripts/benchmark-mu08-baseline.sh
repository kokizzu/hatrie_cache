#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkDifferentialTemporalJoinLoad$' -benchmem -count=5 -benchtime=100ms
