#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
gofmt -w hat/hatSql/json_path.go hat/hatSql/json_path_test.go hat/hatSql/query.go hat/hatCache/sql_query.go hat/hatCache/sql_json_path_test.go
