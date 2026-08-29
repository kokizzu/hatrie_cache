#!/usr/bin/env sh
set -eu

rg -n -A 160 -B 20 '^type sqlExecutionControl|^func newSQLExecutionControl' hat/hatSql/query.go
rg -n -A 180 -B 20 '^func executeSQLQueryWithMetrics|^func executeSQLQueryWithMetricsOuter' hat/hatSql/query.go
rg -n -A 160 -B 20 '^func wrapSQLSourceWorkers|^func mergeSQLRows|^func sqlColumnarStreamMaterialize' hat/hatSql/query.go
rg -n -m 260 'make\(\[\]sqlExecRow|make\(map\[string\]SQLRow|make\(\[\]SQLRow|mergeSQLRows|sqlExecRow\{' hat/hatSql/query.go
