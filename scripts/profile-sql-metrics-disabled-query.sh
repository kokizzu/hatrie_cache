#!/bin/sh
set -eu

profile=/tmp/hatrie-cache-sql-metrics-disabled-alloc.pprof
go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLMetricsDisabledFilteredQuery$' -benchtime=20x -memprofile "$profile"
go tool pprof -top -alloc_space "$profile"
