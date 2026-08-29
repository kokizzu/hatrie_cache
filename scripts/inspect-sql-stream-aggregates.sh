#!/usr/bin/env sh
set -eu

grep -n -A 260 -B 20 '^func sqlGlobalStreamAggregates' hat/hatSql/query.go
grep -n -A 100 -B 20 '^func executeSQLQueryWithMetricsOuter' hat/hatSql/query.go
