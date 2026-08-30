#!/bin/sh
set -eu

profile=/tmp/hatrie-cache-sql-query-rows-columnar-alloc.pprof
go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLQueryRowsColumnarSimpleFilter$' -benchtime=20x -memprofile "$profile"
go tool pprof -top -alloc_space "$profile"
