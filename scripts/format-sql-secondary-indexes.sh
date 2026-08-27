#!/bin/sh
set -eu

gofmt -w \
  ./hat/hatSql/contracts.go \
  ./hat/hatSql/query.go \
  ./hat/hatCache/sql_query.go \
  ./hat/hatCache/sql_secondary_index_test.go
