#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH036Aggregate(Baseline|OrNull)$' -benchmem -benchtime=250ms -count=5
