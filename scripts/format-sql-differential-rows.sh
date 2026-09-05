#!/bin/sh
set -eu

gofmt -w hat/hatSql/differential_rows.go hat/hatSql/differential_rows_test.go
