#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH036AggregateBaseline$' -benchmem -benchtime=250ms -count=5
