#!/bin/sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatSql/sql_columnar_shared_row_test.go
