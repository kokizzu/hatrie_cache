#!/bin/sh
set -eu

sed -n '846,1040p' hat/hatCache/sql_query.go
sed -n '2160,2320p' hat/hatCache/sql_query.go
printf '\nTyped index maintenance references:\n'
rg -n 'sqlJSONTypedInt64CompositeIndexes|sqlJSONIndexCurrentLocked|refreshSQLJSONIndexesLocked' hat/hatCache/main.go hat/hatCache/sql_query.go
printf '\nTyped index statistics references:\n'
rg -n 'SQLJSONIndexStats|sqlJSONIndexStats' hat/hatCache/sql_query.go
printf '\nTyped composite tests:\n'
sed -n '1,260p' hat/hatCache/sql_typed_composite_test.go
printf '\nMonitoring resolver references:\n'
sed -n '1,40p' hat/hatCache/monitoring.go
rg -n 'monitoringSQLResolver|ResolveSQLCompositeIndexedSource' hat/hatCache
printf '\nTest fixture references:\n'
rg -n 'func newTestTrie' hat/hatCache
