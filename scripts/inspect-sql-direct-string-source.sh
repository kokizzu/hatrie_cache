#!/usr/bin/env sh
set -eu

sed -n '1740,2025p' hat/hatCache/sql_query.go
rg -n -A22 -B4 'ResolveSQLColumnarSource|StreamSQLSource' hat/hatCache/*_test.go
