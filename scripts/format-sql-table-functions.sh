#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
gofmt -w hat/hatSql/query.go hat/hatSql/table_function.go hat/hatSql/table_function_test.go
