#!/usr/bin/env sh
set -eu

rg -n -A10 -B4 'GetBytesChecked|sqlJSONRowsString|json\.Unmarshal' hat/hatCache/sql_query.go
rg -n -A10 -B4 'ResolveSQLIndexedSource|ResolveSQLCoveringSource|ResolveSQLOrderedSource' hat/hatCache/sql_query.go
rg -n -A10 -B4 'sort\.Slice|materialized|SQLRow' hat/hatSql/query.go
rg -n -A10 -B4 'CreateSQLJSON.*Index|sqlJSON.*Index' hat/hatCache/sql_query.go
