#!/bin/sh
set -eu

rg -n -A 180 -B 20 '^type SQLQueryOptions|^type SQLPreparedQueryCache|^func NewSQLPreparedQueryCache|^func \(.*SQLPreparedQueryCache.*\)' hat/hatSql
rg -n -A 140 -B 20 '^type SQLColumnarSourceResolver|^type ColumnarBatch|^func executeSQLColumnar' hat/hatSql/contracts.go hat/hatSql/query.go
