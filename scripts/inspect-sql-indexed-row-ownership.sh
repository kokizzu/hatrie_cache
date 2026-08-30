#!/bin/sh
set -eu

rg -n -C 3 'ResolveSQLCompositeIndexedRangeSource|CloneRows\(|cloneSQLRows\(' hat/hatCache/sql_query.go hat/hatSql/query.go hat/hatSql/contracts.go
sed -n '9200,9305p' hat/hatSql/query.go
sed -n '9700,9775p' hat/hatSql/query.go
