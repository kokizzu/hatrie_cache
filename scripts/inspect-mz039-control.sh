#!/bin/sh
set -eu

rg -n 'type sqlExecutionControl|func newSQLExecutionControl|func \(control \*sqlExecutionControl\) check' hat/hatSql
rg -n -C 5 'type SQLSourceResolver|type SourceResolverFunc' hat/hatSql
sed -n '1,45p' hat/hatSql/query.go
sed -n '7750,7885p' hat/hatSql/query.go
