#!/bin/sh
set -eu

gofmt -w hat/hatCache/sql.go hat/hatCache/sql_test.go
