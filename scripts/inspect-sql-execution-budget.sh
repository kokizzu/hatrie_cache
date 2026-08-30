#!/usr/bin/env sh
set -eu

rg -n -A55 -B5 'type SQLQueryOptions|type SQLCollation|type sqlExecutionControl|func newSQLExecutionControl' hat/hatSql/contracts.go hat/hatSql/query.go
