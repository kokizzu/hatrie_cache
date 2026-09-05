#!/bin/sh
set -eu

gofmt -w hat/hatSql/query_trace.go hat/hatSql/query_trace_test.go hat/hatCache/sql_query.go
