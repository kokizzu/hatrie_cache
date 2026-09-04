#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
gofmt -w hat/hatSql/limit_by.go hat/hatSql/limit_by_test.go hat/hatSql/query.go hat/hatSql/collation.go hat/hatSql/subquery.go
