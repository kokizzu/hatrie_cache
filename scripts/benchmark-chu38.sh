#!/bin/sh
set -eu

GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^Benchmark(CH004BaselineSQLQueryLogAppend|CHU38DevNullAppend|SQLQueryLogSampling)$' -benchmem -benchtime=200ms -count=5
