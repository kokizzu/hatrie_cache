#!/usr/bin/env sh
set -eu

grep -n -A 180 -B 20 '^func executeSQLColumnarScan' hat/hatSql/query.go
grep -n -A 100 -B 30 'case "SUM"' hat/hatSql/query.go
grep -n -A 100 -B 20 '^func sqlCanColumnarScan' hat/hatSql/query.go
