#!/bin/sh
set -eu

rg -n 'type sqlExecRow|func wrapSQLSource|func mergeSQLRows|\.sources\[|\.ordinals\[' hat/hatSql/query.go
rg -n 'singleAlias|singleRow|columnarRow' hat/hatSql/query.go
rg -n 'func sqlExecRowsBytes|func sqlAttachSQLExecutionEnvironment|func sqlField' hat/hatSql/query.go
sed -n '9840,9910p' hat/hatSql/query.go
sed -n '12480,12550p' hat/hatSql/query.go
sed -n '6390,6460p' hat/hatSql/query.go
sed -n '9750,9850p' hat/hatSql/query.go
sed -n '7500,7645p' hat/hatSql/query.go
