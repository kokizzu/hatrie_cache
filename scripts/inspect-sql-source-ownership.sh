#!/usr/bin/env sh
set -eu

rg -n -m 8 '^func .*GetBytesChecked' hat/hatCache
rg -n -m 8 '^func .*LockSQLSnapshot' hat/hatCache
rg -n -m 8 '^func .*ResolveSQLColumnarSource' hat/hatCache
rg -n -m 80 'SQLSnapshotLocker|LockSQLSnapshot|GetBytesChecked|ResolveSQLColumnarSource' hat/hatSql hat/hatCache/sql_query.go
