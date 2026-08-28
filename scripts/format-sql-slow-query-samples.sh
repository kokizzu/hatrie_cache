#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatSql/query.go ./hat/hatSql/slow_query.go ./hat/hatSql/slow_query_test.go
