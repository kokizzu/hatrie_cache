#!/usr/bin/env sh
set -eu

sed -n '1,80p' hat/hatSql/query.go
sed -n '236,285p' hat/hatSql/query.go
sed -n '4120,4195p' hat/hatSql/query.go
grep -n 'cache\.entries\|cache\.order\|cache\.touch' hat/hatSql/query.go
sed -n '1,130p' hat/hatSql/query_execution_test.go
