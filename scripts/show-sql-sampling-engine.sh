#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
printf '%s\n' '== query and source declarations =='
rg -n -A 42 -B 8 'type sqlQuery struct|type sqlSource struct|func \(.*\) parse.*Source|func \(.*\) parse.*Query' "$root/hat/hatSql/query.go"
printf '%s\n' '== top-level clause parser =='
sed -n '4520,4690p' "$root/hat/hatSql/query.go"
printf '%s\n' '== clause keyword and streaming gates =='
rg -n -A 36 -B 6 '^func sqlClauseKeyword|^func executeSQLQueryRowsParsed|^func sqlQueryRowsBaseStreamable|^func executeSQLReorderedInnerHashJoins' "$root/hat/hatSql/query.go"
printf '%s\n' '== row-stream execution fallback =='
sed -n '645,735p' "$root/hat/hatSql/query.go"
printf '%s\n' '== source materialization =='
rg -n -A 70 -B 8 '^func resolveSQLSource|^func resolveSQL.*Source|resolveSQLSource\(' "$root/hat/hatSql/query.go"
