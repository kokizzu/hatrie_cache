#!/bin/sh
set -eu

gofmt -w ./hat/hatCache/main.go ./hat/hatCache/sql_query.go ./hat/hatCache/sql_covering_index_test.go ./hat/hatSql/contracts.go ./hat/hatSql/query.go
