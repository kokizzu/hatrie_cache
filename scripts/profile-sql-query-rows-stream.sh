#!/bin/sh
set -eu

profile=/tmp/hatrie-cache-sql-query-rows-alloc.pprof
go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLQueryRowsSimpleFilter$' -benchtime=20x -memprofile "$profile"
go tool pprof -top -alloc_space "$profile"
