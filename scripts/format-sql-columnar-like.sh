#!/bin/sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatCache/sql_columnar_like_test.go
